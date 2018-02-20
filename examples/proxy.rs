#![deny(warnings)]
extern crate hyper;
extern crate env_logger;

use std::io::copy;
use std::time::Duration;

use hyper::server::{Server, Request, Response};
use hyper::client::Client;
use hyper::header::Connection;
use hyper::uri::RequestUri::AbsolutePath;

macro_rules! try_return(
    ($e:expr) => {{
        match $e {
            Ok(v) => v,
            Err(e) => { println!("Error: {}", e); return; }
        }
    }}
);

fn fetch(req: Request, remote_res: Response) {
    match req.uri {
        AbsolutePath(ref path) => {
            let client = Client::new();
            let mut res = client.get(
                format!("http://127.0.0.1{}", path).as_str()
            ).header(Connection::close()).send().unwrap();

            let mut remote_res = try_return!(remote_res.start());
            try_return!(copy(&mut res, &mut remote_res));
        },
        _ => {
            return;
        }
    };
}

fn main() {
    hyper::coroutines::fast_spawn(|| {
        env_logger::init().unwrap();
        let server = Server::http("127.0.0.1:1337").unwrap();

        let _guard = server.handle(fetch);
    
        println!("Listening on http://127.0.0.1:1337");
    });

    loop {
        std::thread::sleep(Duration::from_secs(3));
        println!("Event count: {}", hyper::coroutines::global_event_count());
    }
}
