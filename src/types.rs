use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Todo {
    pub id: Uuid,
    pub title: String,
    pub completed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Todo {
    pub fn new(title: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            title,
            completed: false,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateTodo {
    pub title: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct PatchTodo {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub completed: Option<bool>,
}

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    TodoNotFound,
    InvalidRequest,
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Serialize)]
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

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: ErrorCode::InvalidRequest,
                message: message.into(),
            },
        }
    }
}
