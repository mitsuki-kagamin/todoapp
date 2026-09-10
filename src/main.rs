mod cache;
mod http;
mod server;
mod store;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use server::AppState;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let addr: SocketAddr = std::env::var("TODOAPP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:13265".to_string())
        .parse()
        .expect("TODOAPP_ADDR must be a valid host:port");

    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let state = Arc::new(AppState::new());

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
