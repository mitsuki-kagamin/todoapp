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

    let ptr = BigCache.load(Ordering::Acquire);

    if !ptr.is_null() {
        unsafe {
            let todo = &*ptr;

            if todo.0.id == id {
                return Ok(Json(todo.0.clone()));
            }
        }
    }

    if let Some(todo) = cache.load().get(&id) {
        return Ok(Json(todo.0.clone()));
    }

    let todo = db_client
        .todo()
        .find_first()
        .r#where(todo::id::equals(id))
        .exec()
        .await
        .map_err(|e| ErrorResp::internal(e.to_string()))?
        .ok_or_else(ErrorResp::not_found)?;

    let _ = cache_writer.send(todo.clone()).await;

    Ok(Json(todo))
}

fn update_cache(newdata: Todo) {
    let ptr = Box::into_raw(Box::new(Json(newdata)));

    BigCache.store(ptr, Ordering::Release);
}

async fn cache_worker(cache: SmallCache, mut rx: tokio::sync::mpsc::Receiver<Todo>) {
    println!("Cache worker succesfully started!");

    while let Some(data) = rx.recv().await {
        let mut new_cache = (**cache.load()).clone();

        let data1 = actix_web::web::Json(data.clone());

        update_cache(data1.clone());

        new_cache.insert(data.id, Arc::from(data1));

        cache.store(Arc::new(new_cache));
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    #[cfg(debug_assertions)]
    env_logger::init_from_env(Env::default().default_filter_or("debug"));

    dotenvy::dotenv().ok();
    let workers = available_parallelism()?;

    let database_url = std::env::var("POSTGRES_URL").expect("POSTGRES_URL must be set in .env");

    let pool: PgPool = PgPoolBuilder::new()
        .url(database_url)
        .build()
        .await
        .map_err(std::io::Error::other)?;

    let conn = pool.get().await.map_err(std::io::Error::other)?;

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

    let client = PraxClient::new(PgEngine::new(pool));

    let cache: SmallCache = Arc::new(ArcSwap::new(Arc::new(hashbrown::HashMap::with_hasher(
        foldhash::fast::RandomState::default(),
    ))));
    let cache_worker_ch = tokio::sync::mpsc::channel(16 * 1024);
    let (tx, rx) = cache_worker_ch;

    tokio::spawn(cache_worker(cache.clone(), rx));

    tokio::spawn(async move {
        loop {
            unsafe {
                if !BigCache.load(Ordering::Relaxed).is_null() {
                    _mm_prefetch(BigCache.load(Ordering::Relaxed) as *const i8, _MM_HINT_T0);
                }
            }
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
    .backlog(8096)
    .max_connections(4096)
    .workers(usize::from(workers))
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
