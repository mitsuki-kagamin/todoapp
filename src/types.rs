use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use prax_orm::{Model, client};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Model, Debug, Serialize, Deserialize, Clone, Default)]
#[prax(table = "todo")]
#[serde(rename_all(serialize = "camelCase"))]
#[repr(align(64))]
pub struct Todo {
    #[prax(unique, id)]
    pub id: uuid::Uuid,

    pub title: String,

    #[prax(default = "false")]
    pub completed: bool,

    pub created_at: i64,
    pub updated_at: i64,
}

client!(Todo);

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
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
            ErrorCode::TodoNotFound => StatusCode::NOT_FOUND,
            ErrorCode::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(self)
    }
}
