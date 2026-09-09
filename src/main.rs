#![feature(likely_unlikely)]

mod types;

use types::*;

use actix_web::web::Json;
use actix_web::{App, HttpServer, get, web};
use arc_swap::ArcSwap;
#[cfg(debug_assertions)]
use env_logger::Env;
use prax_orm::PraxClient;

use prax_postgres::{PgEngine, PgPool, PgPoolBuilder};

use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
use std::hint::likely;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread::available_parallelism;

pub type SmallCache =
    Arc<ArcSwap<hashbrown::HashMap<uuid::Uuid, Arc<Json<Todo>>, foldhash::fast::RandomState>>>;

static BigCache: AtomicPtr<Json<Todo>> = AtomicPtr::new(std::ptr::null_mut());

#[get("/todos/{id}")]
async fn get_by_id(
    path: web::Path<uuid::Uuid>,
    db_client: web::Data<PraxClient<PgEngine>>,
    cache: web::Data<SmallCache>,
    cache_writer: web::Data<tokio::sync::mpsc::Sender<Todo>>,
) -> Result<Json<Todo>, ErrorResp> {
    let id = path.into_inner();

    #[cfg(debug_assertions)]
    dbg!(&id);

    let ptr = BigCache.load(Ordering::Relaxed);

    #[cfg(debug_assertions)]
    println!("HANDLER BIG CACHE = {:p}", ptr);

    if !ptr.is_null() {
        #[cfg(debug_assertions)]
        dbg!(Some(()));
        unsafe {
            let todo = &*ptr;
            #[cfg(debug_assertions)]
            dbg!(todo);

            if likely(todo.0.id == id) {
                #[cfg(debug_assertions)]
                dbg!(Some(true));
                return Ok(Json(todo.0.clone()));
            } else {
                #[cfg(debug_assertions)]
                dbg!(Some(false));
            }
        }
    } else {
        #[cfg(debug_assertions)]
        println!("big cache miss");
    }

    if let Some(todo) = cache.load().get(&id) {
        #[cfg(debug_assertions)]
        dbg!(Some(()));
        return Ok(Json(todo.0.clone()));
    } else {
        #[cfg(debug_assertions)]
        println!("small cache miss");
    }

    let todo = db_client
        .todo()
        .find_first()
        .r#where(todo::id::equals(id))
        .exec()
        .await
        .map_err(|e| ErrorResp::internal(e.to_string()))?
        .ok_or_else(ErrorResp::not_found)?;

    #[cfg(debug_assertions)]
    dbg!(&todo);

    #[cfg(debug_assertions)]
    println!("SENDING TO CACHE WORKER: {}", todo.id);

    if let Err(e) = cache_writer.send(todo.clone()).await {
        eprintln!("CACHE SEND ERROR: {e}");
    }

    #[cfg(debug_assertions)]
    println!("SENT TO CACHE WORKER");

    #[cfg(debug_assertions)]
    dbg!(false);

    Ok(Json(todo))
}

fn update_cache(newdata: Todo) {
    let ptr = Box::into_raw(Box::new(Json(newdata)));

    #[cfg(debug_assertions)]
    println!("PUBLISH BIG CACHE: {ptr:p}");

    BigCache.store(ptr, Ordering::Release);

    #[cfg(debug_assertions)]
    println!("BIG CACHE NOW: {:p}", BigCache.load(Ordering::Acquire));
}

async fn cache_worker(cache: SmallCache, mut rx: tokio::sync::mpsc::Receiver<Todo>) {
    println!("Cache worker succesfully started!");

    while let Some(data) = rx.recv().await {
        #[cfg(debug_assertions)]
        println!("WORKER GOT: {}", data.id);

        let mut new_cache = (**cache.load()).clone();

        update_cache(data.clone());

        #[cfg(debug_assertions)]
        println!(
            "BIG CACHE AFTER UPDATE: {:p}",
            BigCache.load(Ordering::Acquire)
        );

        new_cache.insert(data.id, Arc::new(Json(data)));

        cache.store(Arc::new(new_cache));
    }

    #[cfg(debug_assertions)]
    println!("CACHE WORKER EXITED");
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    #[cfg(debug_assertions)]
    env_logger::init_from_env(Env::default().default_filter_or("debug"));

    dotenvy::dotenv().ok();
    let workers = available_parallelism()?;
    dbg!(workers);

    let database_url = std::env::var("POSTGRES_URL").expect("POSTGRES_URL must be set in .env");
    dbg!(&database_url);

    let pool: PgPool = PgPoolBuilder::new()
        .url(database_url)
        .build()
        .await
        .map_err(std::io::Error::other)?;

    dbg!(pool.status());

    let conn = pool.get().await.map_err(std::io::Error::other)?;
    println!("conn created");

    conn.batch_execute(
        r#"
    CREATE TABLE IF NOT EXISTS todo (
        id UUID PRIMARY KEY,
        title TEXT NOT NULL,
        completed BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    "#,
    )
    .await
    .map_err(std::io::Error::other)?;
    println!("table creating pass");

    let uuid = uuid::Uuid::parse_str("a63df501-83fa-4cc6-9c94-8cff1927a6fc")
        .map_err(std::io::Error::other)?;

    conn.execute(
        r#"
        INSERT INTO todo (id, title, completed)
        VALUES ($1, '', FALSE)
        ON CONFLICT (id) DO NOTHING
        "#,
        &[&uuid],
    )
    .await
    .map_err(std::io::Error::other)?;
    println!("data creating pass");

    let client = PraxClient::new(PgEngine::new(pool));
    println!("db client created");

    let cache: SmallCache = Arc::new(ArcSwap::new(Arc::new(hashbrown::HashMap::with_hasher(
        foldhash::fast::RandomState::default(),
    ))));
    dbg!(&cache);
    let cache_worker_ch = tokio::sync::mpsc::channel(16 * 1024);
    dbg!(&cache_worker_ch);
    let (tx, rx) = cache_worker_ch;

    tokio::spawn(cache_worker(cache.clone(), rx));

    std::thread::spawn(|| {
        loop {
            let ptr = BigCache.load(Ordering::Relaxed);

            if !ptr.is_null() {
                unsafe {
                    _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
                }
            }

            std::hint::spin_loop();
        }
    });

    println!("Server spawned!");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(client.clone()))
            .app_data(web::Data::new(cache.clone()))
            .app_data(web::Data::new(tx.clone()))
            .service(get_by_id)
    })
    .backlog(8 * 1024)
    .max_connections(4 * 1024)
    .workers(usize::from(workers) * 2)
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
