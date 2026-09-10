use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use uuid::Uuid;

use crate::types::Todo;

/// The authoritative "DB" tier from `api_reference.md`'s L1 -> L2 -> DB
/// pipeline. In-memory for now (no real database is wired up in this
/// sandbox); the cache tiers in front of it don't care what backs it.
#[derive(Default)]
pub struct Store {
    todos: RwLock<HashMap<Uuid, Arc<Todo>>>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn list(&self) -> Vec<Arc<Todo>> {
        let todos = self.todos.read().unwrap();
        let mut items: Vec<_> = todos.values().cloned().collect();
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        items
    }

    pub fn get(&self, id: Uuid) -> Option<Arc<Todo>> {
        self.todos.read().unwrap().get(&id).cloned()
    }

    pub fn insert(&self, todo: Todo) -> Arc<Todo> {
        let todo = Arc::new(todo);
        self.todos.write().unwrap().insert(todo.id, todo.clone());
        todo
    }

    pub fn patch(&self, id: Uuid, title: Option<String>, completed: Option<bool>) -> Option<Arc<Todo>> {
        let mut todos = self.todos.write().unwrap();
        let existing = todos.get(&id)?;
        let mut updated = (**existing).clone();
        if let Some(title) = title {
            updated.title = title;
        }
        if let Some(completed) = completed {
            updated.completed = completed;
        }
        updated.updated_at = chrono::Utc::now();
        let updated = Arc::new(updated);
        todos.insert(id, updated.clone());
        Some(updated)
    }

    pub fn delete(&self, id: Uuid) -> bool {
        self.todos.write().unwrap().remove(&id).is_some()
    }
}
