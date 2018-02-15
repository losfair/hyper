//! HTTP Client

use std::cell::Cell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use futures::{Async, Future, Poll, Stream};
use futures::future::{self, Either, Executor};
use http::{Request, Response, Uri, Version};
use http::header::{Entry, HeaderValue, HOST};
use tokio::reactor::Handle;
pub use tokio_service::Service;

use proto::{self, Body};
use self::pool::Pool;

pub use self::connect::{HttpConnector, Connect};

use self::background::{bg, Background};

mod cancel;
mod connect;
//TODO(easy): move cancel and dispatch into common instead
pub(crate) mod dispatch;
mod dns;
mod pool;

/// A Client to make outgoing HTTP requests.
pub struct Client<C, B = proto::Body> {
    connector: Rc<C>,
    executor: Exec,
    h1_writev: bool,
    pool: Pool<HyperClient<B>>,
    retry_canceled_requests: bool,
}

impl Client<HttpConnector, proto::Body> {
    /// Create a new Client with the default config.
    #[inline]
    pub fn new(handle: &Handle) -> Client<HttpConnector, proto::Body> {
        Config::default().build(handle)
    }
}

impl Client<HttpConnector, proto::Body> {
    /// Configure a Client.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # extern crate hyper;
    /// # extern crate tokio_core;
    ///
    /// # fn main() {
    /// # let core = tokio_core::reactor::Core::new().unwrap();
    /// # let handle = core.handle();
    /// let client = hyper::Client::configure()
    ///     .keep_alive(true)
    ///     .build(&handle);
    /// # drop(client);
    /// # }
    /// ```
    #[inline]
    pub fn configure() -> Config<UseDefaultConnector, proto::Body> {
        Config::default()
    }
}

impl<C, B> Client<C, B> {
    // Eventually, a Client won't really care about a tokio Handle, and only
    // the executor used to spawn background tasks. Removing this method is
    // a breaking change, so for now, it's just deprecated.
    #[doc(hidden)]
    #[deprecated]
    pub fn handle(&self) -> &Handle {
        match self.executor {
            Exec::Handle(ref h) => h,
            Exec::Executor(..) => panic!("Client not built with a Handle"),
        }
    }

    /// Create a new client with a specific connector.
    #[inline]
    fn configured(config: Config<C, B>, exec: Exec) -> Client<C, B> {
        Client {
            connector: Rc::new(config.connector),
            executor: exec,
            h1_writev: config.h1_writev,
            pool: Pool::new(config.keep_alive, config.keep_alive_timeout),
            retry_canceled_requests: config.retry_canceled_requests,
        }
    }
}

impl<C, B> Client<C, B>
where C: Connect,
      B: Stream<Error=::Error> + 'static,
      B::Item: AsRef<[u8]>,
{
    /// Send a constructed Request using this Client.
    #[inline]
    pub fn request(&self, mut req: Request<B>) -> FutureResponse {
        match req.version() {
            Version::HTTP_10 |
            Version::HTTP_11 => (),
            other => {
                error!("Request has unsupported version \"{:?}\"", other);
                return FutureResponse(Box::new(future::err(::Error::Version)));
            }
        }

        let uri = req.uri().clone();
        let domain = match (uri.scheme_part(), uri.authority_part()) {
            (Some(scheme), Some(auth)) => {
                format!("{}://{}", scheme, auth)
            }
            _ => {
                return FutureResponse(Box::new(future::err(::Error::Io(
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid URI for Client Request"
                    )
                ))));
            }
        };

        if let Entry::Vacant(entry) = req.headers_mut().entry(HOST).expect("HOST is always valid header name") {
            let hostname = uri.host().expect("authority implies host");
            let host = if let Some(port) = uri.port() {
                let s = format!("{}:{}", hostname, port);
                HeaderValue::from_str(&s)
            } else {
                HeaderValue::from_str(hostname)
            }.expect("uri host is valid header value");
            entry.insert(host);
        }


        let client = self.clone();
        //TODO: let is_proxy = req.is_proxy();
        //let uri = req.uri().clone();
        let fut = RetryableSendRequest {
            client: client,
            future: self.send_request(req, &domain),
            domain: domain,
            //is_proxy: is_proxy,
            //uri: uri,
        };
        FutureResponse(Box::new(fut))
    }

    //TODO: replace with `impl Future` when stable
    fn send_request(&self, mut req: Request<B>, domain: &str) -> Box<Future<Item=Response<Body>, Error=ClientError<B>>> {
        let url = req.uri().clone();

        let path = match url.path_and_query() {
            Some(path) => {
                let mut parts = ::http::uri::Parts::default();
                parts.path_and_query = Some(path.clone());
                Uri::from_parts(parts).expect("path is valid uri")
            },
            None => {
                "/".parse().expect("/ is valid path")
            }
        };
        *req.uri_mut() = path;

        let checkout = self.pool.checkout(domain);
        let connect = {
            let executor = self.executor.clone();
            let pool = self.pool.clone();
            let pool_key = Rc::new(domain.to_string());
            let h1_writev = self.h1_writev;
            self.connector.connect(url)
                .and_then(move |io| {
                    let (tx, rx) = dispatch::channel();
                    let tx = HyperClient {
                        tx: tx,
                        should_close: Cell::new(true),
                    };
                    let pooled = pool.pooled(pool_key, tx);
                    let mut conn = proto::Conn::<_, _, proto::ClientTransaction, _>::new(io, pooled.clone());
                    if !h1_writev {
                        conn.set_write_strategy_flatten();
                    }
                    let dispatch = proto::dispatch::Dispatcher::new(proto::dispatch::Client::new(rx), conn);
                    executor.execute(dispatch.map_err(|e| debug!("client connection error: {}", e)))?;
                    Ok(pooled)
                })
        };

        let race = checkout.select(connect)
            .map(|(client, _work)| client)
            .map_err(|(e, _checkout)| {
                // the Pool Checkout cannot error, so the only error
                // is from the Connector
                // XXX: should wait on the Checkout? Problem is
                // that if the connector is failing, it may be that we
                // never had a pooled stream at all
                ClientError::Normal(e.into())
            });

        let resp = race.and_then(move |client| {
            let conn_reused = client.is_reused();
            match client.tx.send(req) {
                Ok(rx) => {
                    client.should_close.set(false);
                    Either::A(rx.then(move |res| {
                        match res {
                            Ok(Ok(res)) => Ok(res),
                            Ok(Err((err, orig_req))) => Err(match orig_req {
                                Some(req) => ClientError::Canceled {
                                    connection_reused: conn_reused,
                                    reason: err,
                                    req: req,
                                },
                                None => ClientError::Normal(err),
                            }),
                            // this is definite bug if it happens, but it shouldn't happen!
                            Err(_) => panic!("dispatch dropped without returning error"),
                        }
                    }))
                },
                Err(req) => {
                    debug!("pooled connection was not ready");
                    let err = ClientError::Canceled {
                        connection_reused: conn_reused,
                        reason: ::Error::new_canceled(None),
                        req: req,
                    };
                    Either::B(future::err(err))
                }
            }
        });

        Box::new(resp)
    }
}

impl<C, B> Service for Client<C, B>
where C: Connect,
      B: Stream<Error=::Error> + 'static,
      B::Item: AsRef<[u8]>,
{
    type Request = Request<B>;
    type Response = Response<Body>;
    type Error = ::Error;
    type Future = FutureResponse;

    fn call(&self, req: Self::Request) -> Self::Future {
        self.request(req)
    }
}

impl<C, B> Clone for Client<C, B> {
    fn clone(&self) -> Client<C, B> {
        Client {
            connector: self.connector.clone(),
            executor: self.executor.clone(),
            h1_writev: self.h1_writev,
            pool: self.pool.clone(),
            retry_canceled_requests: self.retry_canceled_requests,
        }
    }
}

impl<C, B> fmt::Debug for Client<C, B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Client")
            .finish()
    }
}

/// A `Future` that will resolve to an HTTP Response.
#[must_use = "futures do nothing unless polled"]
pub struct FutureResponse(Box<Future<Item=Response<Body>, Error=::Error> + 'static>);

impl fmt::Debug for FutureResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("Future<Response>")
    }
}

impl Future for FutureResponse {
    type Item = Response<Body>;
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

struct RetryableSendRequest<C, B> {
    client: Client<C, B>,
    domain: String,
    future: Box<Future<Item=Response<Body>, Error=ClientError<B>>>,
    //is_proxy: bool,
    //uri: Uri,
}

impl<C, B> Future for RetryableSendRequest<C, B>
where
    C: Connect,
    B: Stream<Error=::Error> + 'static,
    B::Item: AsRef<[u8]>,
{
    type Item = Response<Body>;
    type Error = ::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.future.poll() {
                Ok(Async::Ready(resp)) => return Ok(Async::Ready(resp)),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(ClientError::Normal(err)) => return Err(err),
                Err(ClientError::Canceled {
                    connection_reused,
                    req,
                    reason,
                }) => {
                    if !self.client.retry_canceled_requests || !connection_reused {
                        // if client disabled, don't retry
                        // a fresh connection means we definitely can't retry
                        return Err(reason);
                    }

                    trace!("unstarted request canceled, trying again (reason={:?})", reason);
                    self.future = self.client.send_request(req, &self.domain);
                }
            }
        }
    }
}

struct HyperClient<B> {
    should_close: Cell<bool>,
    tx: dispatch::Sender<proto::dispatch::ClientMsg<B>, Response<Body>>,
}

impl<B> Clone for HyperClient<B> {
    fn clone(&self) -> HyperClient<B> {
        HyperClient {
            tx: self.tx.clone(),
            should_close: self.should_close.clone(),
        }
    }
}

impl<B> self::pool::Ready for HyperClient<B> {
    fn poll_ready(&mut self) -> Poll<(), ()> {
        if self.tx.is_closed() {
            Err(())
        } else {
            Ok(Async::Ready(()))
        }
    }
}

impl<B> Drop for HyperClient<B> {
    fn drop(&mut self) {
        if self.should_close.get() {
            self.should_close.set(false);
            self.tx.cancel();
        }
    }
}

pub(crate) enum ClientError<B> {
    Normal(::Error),
    Canceled {
        connection_reused: bool,
        req: Request<B>,
        reason: ::Error,
    }
}

/// Configuration for a Client
pub struct Config<C, B> {
    _body_type: PhantomData<B>,
    //connect_timeout: Duration,
    connector: C,
    keep_alive: bool,
    keep_alive_timeout: Option<Duration>,
    h1_writev: bool,
    //TODO: make use of max_idle config
    max_idle: usize,
    retry_canceled_requests: bool,
}

/// Phantom type used to signal that `Config` should create a `HttpConnector`.
#[derive(Debug, Clone, Copy)]
pub struct UseDefaultConnector(());

impl Default for Config<UseDefaultConnector, proto::Body> {
    fn default() -> Config<UseDefaultConnector, proto::Body> {
        Config {
            _body_type: PhantomData::<proto::Body>,
            connector: UseDefaultConnector(()),
            keep_alive: true,
            keep_alive_timeout: Some(Duration::from_secs(90)),
            h1_writev: true,
            max_idle: 5,
            retry_canceled_requests: true,
        }
    }
}

impl<C, B> Config<C, B> {
    /// Set the body stream to be used by the `Client`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use hyper::client::Config;
    /// let cfg = Config::default()
    ///     .body::<hyper::Body>();
    /// # drop(cfg);
    #[inline]
    pub fn body<BB>(self) -> Config<C, BB> {
        Config {
            _body_type: PhantomData::<BB>,
            connector: self.connector,
            keep_alive: self.keep_alive,
            keep_alive_timeout: self.keep_alive_timeout,
            h1_writev: self.h1_writev,
            max_idle: self.max_idle,
            retry_canceled_requests: self.retry_canceled_requests,
        }
    }

    /// Set the `Connect` type to be used.
    #[inline]
    pub fn connector<CC>(self, val: CC) -> Config<CC, B> {
        Config {
            _body_type: self._body_type,
            connector: val,
            keep_alive: self.keep_alive,
            keep_alive_timeout: self.keep_alive_timeout,
            h1_writev: self.h1_writev,
            max_idle: self.max_idle,
            retry_canceled_requests: self.retry_canceled_requests,
        }
    }

    /// Enable or disable keep-alive mechanics.
    ///
    /// Default is enabled.
    #[inline]
    pub fn keep_alive(mut self, val: bool) -> Config<C, B> {
        self.keep_alive = val;
        self
    }

    /// Set an optional timeout for idle sockets being kept-alive.
    ///
    /// Pass `None` to disable timeout.
    ///
    /// Default is 90 seconds.
    #[inline]
    pub fn keep_alive_timeout(mut self, val: Option<Duration>) -> Config<C, B> {
        self.keep_alive_timeout = val;
        self
    }

    /// Set whether HTTP/1 connections should try to use vectored writes,
    /// or always flatten into a single buffer.
    ///
    /// Note that setting this to false may mean more copies of body data,
    /// but may also improve performance when an IO transport doesn't
    /// support vectored writes well, such as most TLS implementations.
    ///
    /// Default is `true`.
    #[inline]
    pub fn http1_writev(mut self, val: bool) -> Config<C, B> {
        self.h1_writev = val;
        self
    }

    /// Set whether to retry requests that get disrupted before ever starting
    /// to write.
    ///
    /// This means a request that is queued, and gets given an idle, reused
    /// connection, and then encounters an error immediately as the idle
    /// connection was found to be unusable.
    ///
    /// When this is set to `false`, the related `FutureResponse` would instead
    /// resolve to an `Error::Cancel`.
    ///
    /// Default is `true`.
    #[inline]
    pub fn retry_canceled_requests(mut self, val: bool) -> Config<C, B> {
        self.retry_canceled_requests = val;
        self
    }
}

impl<C, B> Config<C, B>
where C: Connect,
      B: Stream<Error=::Error>,
      B::Item: AsRef<[u8]>,
{
    /// Construct the Client with this configuration.
    #[inline]
    pub fn build(self, handle: &Handle) -> Client<C, B> {
        Client::configured(self, Exec::Handle(handle.clone()))
    }

    /// Construct a Client with this configuration and an executor.
    ///
    /// The executor will be used to spawn "background" connection tasks
    /// to drive requests and responses.
    pub fn executor<E>(self, executor: E) -> Client<C, B>
    where
        E: Executor<Background> + 'static,
    {
        Client::configured(self, Exec::Executor(Rc::new(executor)))
    }
}

impl<B> Config<UseDefaultConnector, B>
where B: Stream<Error=::Error>,
      B::Item: AsRef<[u8]>,
{
    /// Construct the Client with this configuration.
    #[inline]
    pub fn build(self, handle: &Handle) -> Client<HttpConnector, B> {
        let mut connector = HttpConnector::new(4, handle);
        if self.keep_alive {
            connector.set_keepalive(self.keep_alive_timeout);
        }
        self.connector(connector).build(handle)
    }
}

impl<C, B> fmt::Debug for Config<C, B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("keep_alive", &self.keep_alive)
            .field("keep_alive_timeout", &self.keep_alive_timeout)
            .field("http1_writev", &self.h1_writev)
            .field("max_idle", &self.max_idle)
            .finish()
    }
}

impl<C: Clone, B> Clone for Config<C, B> {
    fn clone(&self) -> Config<C, B> {
        Config {
            connector: self.connector.clone(),
            .. *self
        }
    }
}


// ===== impl Exec =====

#[derive(Clone)]
enum Exec {
    Handle(Handle),
    Executor(Rc<Executor<Background>>),
}


impl Exec {
    fn execute<F>(&self, fut: F) -> io::Result<()>
    where
        F: Future<Item=(), Error=()> + 'static,
    {
        match *self {
            Exec::Handle(ref h) => h.spawn(fut),
            Exec::Executor(ref e) => {
                e.execute(bg(Box::new(fut)))
                    .map_err(|err| {
                        debug!("executor error: {:?}", err.kind());
                        io::Error::new(
                            io::ErrorKind::Other,
                            "executor error",
                        )
                    })?
            },
        }
        Ok(())
    }
}

// ===== impl Background =====

// The types inside this module are not exported out of the crate,
// so they are in essence un-nameable.
mod background {
    use futures::{Future, Poll};

    // This is basically `impl Future`, since the type is un-nameable,
    // and only implementeds `Future`.
    #[allow(missing_debug_implementations)]
    pub struct Background {
        inner: Box<Future<Item=(), Error=()>>,
    }

    pub fn bg(fut: Box<Future<Item=(), Error=()>>) -> Background {
        Background {
            inner: fut,
        }
    }

    impl Future for Background {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.inner.poll()
        }
    }
}

