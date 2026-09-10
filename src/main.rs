#![feature(likely_unlikely)]

mod cache;
mod db;
mod http;
mod server;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use db::Db;
use server::AppState;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    dotenvy::dotenv().ok();

    let addr: SocketAddr = std::env::var("TODOAPP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:13265".to_string())
        .parse()
        .expect("TODOAPP_ADDR must be a valid host:port");

    let database_url =
        std::env::var("POSTGRES_URL").expect("POSTGRES_URL must be set (in the environment or .env)");

    let db = Db::connect(&database_url).expect("failed to connect to Postgres");
    db.migrate().expect("failed to run schema migration");

    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let state = Arc::new(AppState::new(db));

    println!("todoapp listening on {addr} across {workers} worker thread(s)");

    let handles: Vec<_> = (0..workers)
        .map(|_| {
            let state = state.clone();
            thread::spawn(move || {
                let runtime =
                    compio::runtime::Runtime::new().expect("failed to start compio runtime");
                if let Err(e) = runtime.block_on(server::run(addr, state)) {
                    eprintln!("worker thread exited: {e}");
                }
            })
        })
        .collect();

    for handle in handles {
        let _ = handle.join();
    }
}
