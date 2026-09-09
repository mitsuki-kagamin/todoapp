use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use chrono::Utc;
use prax_orm::{Model, client};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Model, Debug, Serialize, Deserialize, Clone, Default)]
#[prax(table = "todo")]
pub struct Todo {
    #[prax(unique, id)]
    pub id: uuid::Uuid,

    pub title: String,

    #[prax(default = "false")]
    pub completed: bool,

    #[prax(default = "now()")]
    #[serde(rename(serialize = "camelCase"))]
    pub created_at: chrono::DateTime<Utc>,

    #[prax(default = "now()")]
    #[serde(rename(serialize = "camelCase"))]
    pub updated_at: chrono::DateTime<Utc>,
}

client!(Todo);

#[derive(Debug, Serialize, Deserialize)]
pub struct AllTodos(pub Vec<Todo>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTodo {
    pub title: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTodo {
    pub title: Option<String>,
    pub completed: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidRequest,
    TodoNotFound,
    InternalError,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResp {
    pub error: ErrorBody,
}

impl ErrorResp {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: ErrorCode::InvalidRequest,
                message: message.into(),
            },
        }
    }

    pub fn not_found() -> Self {
        Self {
            error: ErrorBody {
                code: ErrorCode::TodoNotFound,
                message: "Todo not found".to_string(),
            },
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
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
