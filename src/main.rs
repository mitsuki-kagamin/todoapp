use actix_web::http::StatusCode;
use actix_web::web::Json;
use actix_web::{
    App, HttpResponse, HttpServer, Responder, ResponseError, delete, get, patch, post, web,
};
use ahash::AHashMap;
use arc_swap::ArcSwap;
use chrono::Utc;
#[cfg(debug_assertions)]
use env_logger::Env;
use prax_orm::{Model, PraxClient, client};
use prax_postgres::{PgEngine, PgPool, PgPoolBuilder};
use prax_query::{ErrorCode as PraxErrorCode, OrderByField};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

type Cache = Arc<ArcSwap<AHashMap<String, Arc<Todo>>>>;

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
#[prax(table = "todo")]
struct Todo {
    #[prax(unique, id)]
    id: uuid::Uuid,

    title: String,

    #[prax(default = "false")]
    completed: bool,

    #[prax(default = "now()")]
    #[serde(rename(serialize = "camelCase"))]
    created_at: chrono::DateTime<Utc>,

    #[prax(default = "now()")]
    #[serde(rename(serialize = "camelCase"))]
    updated_at: chrono::DateTime<Utc>,
}

client!(Todo);

#[derive(Debug, Serialize, Deserialize)]
struct AllTodos(Vec<Todo>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTodo {
    title: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateTodo {
    title: Option<String>,
    completed: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ErrorCode {
    InvalidRequest,
    TodoNotFound,
    InternalError,
}

#[derive(Serialize, Deserialize, Debug)]
struct ErrorBody {
    code: ErrorCode,
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ErrorResp {
    error: ErrorBody,
}

impl ErrorResp {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: ErrorCode::InvalidRequest,
                message: message.into(),
            },
        }
    }

    fn not_found() -> Self {
        Self {
            error: ErrorBody {
                code: ErrorCode::TodoNotFound,
                message: "Todo not found".to_string(),
            },
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: ErrorCode::InternalError,
                message: message.into(),
            },
        }
    }
}

impl fmt::Display for ErrorResp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error.message)
    }
}

impl ResponseError for ErrorResp {
    fn status_code(&self) -> StatusCode {
        match self.error.code {
            ErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
            ErrorCode::TodoNotFound => StatusCode::NOT_FOUND,
            ErrorCode::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(self)
    }
}

#[get("/todos")]
async fn get_all(db_client: web::Data<PraxClient<PgEngine>>) -> Result<impl Responder, ErrorResp> {
    let todos = db_client
        .todo()
        .find_many()
        .order_by(OrderByField::desc("created_at"))
        .exec()
        .await
        .map_err(|e| ErrorResp::internal(e.to_string()))?;

    Ok(HttpResponse::Ok().json(AllTodos(todos)))
}

#[get("/todos/{id}")]
async fn get_by_id(
    path: web::Path<(String,)>,
    db_client: web::Data<PraxClient<PgEngine>>,
    cache: web::Data<Cache>,
    cache_writer: web::Data<tokio::sync::mpsc::Sender<Todo>>,
) -> Result<Json<Todo>, ErrorResp> {
    let id = path.into_inner().0;

    if let Some(todo) = cache.load().get(&id).cloned() {
        return Ok(Json(*todo));
    }

    let uuid = uuid::Uuid::try_from(id).map_err(|e| ErrorResp { error: ErrorBody { code: ErrorCode::InvalidRequest, message: "invalid request".to_string() } })?;

    let todo = db_client
        .todo()
        .find_first()
        .r#where(todo::id::equals(uuid))
        .exec()
        .await
        .map_err(|e| ErrorResp::internal(e.to_string()))?
        .ok_or_else(ErrorResp::not_found)?;

    let _ = cache_writer.send(todo.clone()).await;

    Ok(Json(todo))
}

#[post("/todos")]
async fn create_todo(
    data: Json<CreateTodo>,
    db_client: web::Data<PraxClient<PgEngine>>,
) -> Result<impl Responder, ErrorResp> {
    let title = data.title.trim().to_string();

    if title.is_empty() {
        return Err(ErrorResp::invalid("title must not be empty"));
    }

    let todo = db_client
        .transaction(|tx| async move {
            tx.todo()
                .create()
                .set("id", uuid::Uuid::new_v4())
                .set("title", title.to_string())
                .exec()
                .await
        })
        .await
        .map_err(|e| ErrorResp::internal(e.to_string()))?;

    Ok(HttpResponse::Created()
        .insert_header(("Location", format!("/todos/{}", todo.id)))
        .json(todo))
}

#[patch("/todos/{id}")]
async fn patch_todo(
    path: web::Path<(uuid::Uuid,)>,
    data: Json<UpdateTodo>,
    db_client: web::Data<PraxClient<PgEngine>>,
) -> Result<impl Responder, ErrorResp> {
    let id = path.into_inner().0;

    if data.title.is_none() && data.completed.is_none() {
        return Err(ErrorResp::invalid("at least one field must be provided"));
    }

    if let Some(title) = &data.title
        && title.trim().is_empty()
    {
        return Err(ErrorResp::invalid("title must not be empty"));
    }

    let updated_at = Utc::now();

    let result = match (&data.title, data.completed) {
        (Some(title), Some(completed)) => {
            let title = title.trim().to_string();

            db_client
                .transaction(move |tx| async move {
                    tx.todo()
                        .update()
                        .r#where(todo::id::equals(id))
                        .set("title", title)
                        .set("completed", completed)
                        .set("updated_at", updated_at)
                        .exec()
                        .await
                })
                .await
        }

        (Some(title), None) => {
            let title = title.trim().to_string();

            db_client
                .transaction(move |tx| async move {
                    tx.todo()
                        .update()
                        .r#where(todo::id::equals(id))
                        .set("title", title)
                        .set("updated_at", updated_at)
                        .exec()
                        .await
                })
                .await
        }

        (None, Some(completed)) => {
            db_client
                .transaction(move |tx| async move {
                    tx.todo()
                        .update()
                        .r#where(todo::id::equals(id))
                        .set("completed", completed)
                        .set("updated_at", updated_at)
                        .exec()
                        .await
                })
                .await
        }

        (None, None) => unreachable!(),
    };

    match result {
        Ok(records) => {
            let todo = records.first().ok_or_else(ErrorResp::not_found)?;

            Ok(HttpResponse::Ok().json(todo))
        }

        Err(e) if e.code == PraxErrorCode::RecordNotFound => Err(ErrorResp::not_found()),

        Err(e) => Err(ErrorResp::internal(e.message)),
    }
}

#[delete("/todos/{id}")]
async fn delete_todo(
    path: web::Path<(uuid::Uuid,)>,
    db_client: web::Data<PraxClient<PgEngine>>,
) -> Result<impl Responder, ErrorResp> {
    let id = path.into_inner().0;

    let result = db_client
        .transaction(move |tx| async move {
            tx.todo()
                .delete()
                .r#where(todo::id::equals(id))
                .exec()
                .await
        })
        .await;

    match result {
        Ok(_) => Ok(HttpResponse::NoContent().finish()),

        Err(e) if e.code == PraxErrorCode::RecordNotFound => Err(ErrorResp::not_found()),

        Err(e) => Err(ErrorResp::internal(e.message)),
    }
}

async fn cache_worker(cache: Cache, mut rx: tokio::sync::mpsc::Receiver<Todo>) {
    while let Some(data) = rx.recv().await {
        let mut new_cache = (**cache.load()).clone();

        new_cache.insert(String::from(data.id), Arc::from(data));

        cache.store(Arc::new(new_cache));
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    #[cfg(debug_assertions)]
    env_logger::init_from_env(Env::default().default_filter_or("debug"));

    dotenvy::dotenv().ok();

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

    let cache = Arc::new(ArcSwap::new(Arc::new(AHashMap::new())));
    let cache_worker_ch = tokio::sync::mpsc::channel(16 * 1024);
    let (tx, rx) = cache_worker_ch;

    tokio::spawn((|| cache_worker(cache.clone(), rx))());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(client.clone()))
            .service(get_all)
            .service(create_todo)
            .service(patch_todo)
            .service(delete_todo)
            .app_data(web::Data::new(cache.clone()))
            .app_data(web::Data::new(tx.clone()))
            .service(get_by_id)
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
