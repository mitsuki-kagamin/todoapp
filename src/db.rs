use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use uuid::Uuid;

use crate::types::Todo;

type PgPool = Pool<PostgresConnectionManager<NoTls>>;

/// What a task wants done - the `Task` from `api_reference.md`.
pub enum Task {
    Get(Uuid),
    List,
    Insert(Todo),
    Patch(Uuid, Option<String>, Option<bool>),
    Delete(Uuid),
}

pub enum TaskResult {
    One(Option<Arc<Todo>>),
    Many(Vec<Arc<Todo>>),
    Deleted(bool),
    Error(String),
}

/// The DB tier from the L1 -> L2 -> DB pipeline, backed by real Postgres.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS todo (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)";

const SELECT_COLUMNS: &str = "id, title, completed, created_at, updated_at";

impl Db {
    pub fn connect(url: &str) -> Result<Self, Box<dyn Error>> {
        let mut config: postgres::Config = url.parse()?;
        // Without these, a request whose connection dies mid-query (Postgres
        // restarted, network blip) hangs until the OS's default TCP
        // retransmission timeout gives up - which can be tens of minutes.
        config
            .connect_timeout(Duration::from_secs(5))
            .tcp_user_timeout(Duration::from_secs(5));

        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .max_size(16)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)?;
        Ok(Self { pool })
    }

    pub fn migrate(&self) -> Result<(), Box<dyn Error>> {
        self.pool.get()?.batch_execute(SCHEMA)?;
        Ok(())
    }

    /// Dispatches `task` to the blocking pool and calls `continuation` with
    /// the result once it's done - `db.send_task(Task::Get(id), |channel| {
    /// ... })` from `api_reference.md`, not a plain `.await` hidden behind
    /// sugar. The response the caller is waiting on travels back through
    /// whatever channel `continuation` closes over (see `server.rs`).
    pub fn send_task<F>(&self, task: Task, continuation: F)
    where
        F: FnOnce(TaskResult) + 'static,
    {
        let db = self.clone();
        compio::runtime::spawn(async move {
            let result = compio::runtime::spawn_blocking(move || db.run(task))
                .await
                .unwrap_or_else(|_| TaskResult::Error("db worker task panicked".into()));
            continuation(result);
        })
        .detach();
    }

    fn run(&self, task: Task) -> TaskResult {
        match self.try_run(task) {
            Ok(result) => result,
            Err(e) => TaskResult::Error(e.to_string()),
        }
    }

    fn try_run(&self, task: Task) -> Result<TaskResult, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        Ok(match task {
            Task::Get(id) => {
                let sql = format!("SELECT {SELECT_COLUMNS} FROM todo WHERE id = $1");
                let row = conn.query_opt(&sql, &[&id])?;
                TaskResult::One(row.as_ref().map(row_to_todo).map(Arc::new))
            }

            Task::List => {
                let sql = format!("SELECT {SELECT_COLUMNS} FROM todo ORDER BY created_at DESC");
                let rows = conn.query(&sql, &[])?;
                TaskResult::Many(rows.iter().map(row_to_todo).map(Arc::new).collect())
            }

            Task::Insert(todo) => {
                conn.execute(
                    "INSERT INTO todo (id, title, completed, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &todo.id,
                        &todo.title,
                        &todo.completed,
                        &todo.created_at,
                        &todo.updated_at,
                    ],
                )?;
                TaskResult::One(Some(Arc::new(todo)))
            }

            Task::Patch(id, title, completed) => {
                let now = Utc::now();
                let sql = format!(
                    "UPDATE todo SET
                        title = COALESCE($2, title),
                        completed = COALESCE($3, completed),
                        updated_at = $4
                     WHERE id = $1
                     RETURNING {SELECT_COLUMNS}"
                );
                let row = conn.query_opt(&sql, &[&id, &title, &completed, &now])?;
                TaskResult::One(row.as_ref().map(row_to_todo).map(Arc::new))
            }

            Task::Delete(id) => {
                let n = conn.execute("DELETE FROM todo WHERE id = $1", &[&id])?;
                TaskResult::Deleted(n > 0)
            }
        })
    }
}

fn row_to_todo(row: &postgres::Row) -> Todo {
    Todo {
        id: row.get("id"),
        title: row.get("title"),
        completed: row.get("completed"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}
