use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use compio::buf::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpSocket, TcpStream};
use uuid::Uuid;

use crate::cache::{Cache, CacheTask};
use crate::db::{Db, Task, TaskResult};
use crate::http::{self, Method};
use crate::types::{CreateTodo, ErrorResp, PatchTodo, Todo};

pub struct AppState {
    pub db: Db,
    pub cache: Arc<Cache>,
}

impl AppState {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            cache: Arc::new(Cache::new()),
        }
    }
}

/// One `compio` runtime per OS thread, all sharing one port via
/// `SO_REUSEPORT` - the usual thread-per-core layout for this kind of
/// completion-based runtime.
pub async fn run(addr: SocketAddr, state: Arc<AppState>) -> io::Result<()> {
    let listener = bind_reuseport(addr).await?;

    loop {
        let (stream, _peer) = listener.accept().await?;
        let state = state.clone();
        compio::runtime::spawn(async move {
            handle_connection(stream, state).await;
        })
        .detach();
    }
}

async fn bind_reuseport(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4().await?,
        SocketAddr::V6(_) => TcpSocket::new_v6().await?,
    };
    socket.set_reuseport(true)?;
    socket.set_reuseaddr(true)?;
    socket.bind(addr).await?;
    socket.listen(1024).await
}

/// A connection's unread bytes, accumulated across reads. Kept deliberately
/// simple (own the bytes, don't fight `compio`'s owned-buffer read API for
/// incremental in-place growth) over squeezing out the last copy.
struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
    scratch: Vec<u8>,
    /// Response buffer, reused across every request on this connection the
    /// same way `scratch` is: `compio`'s writes take the buffer by value and
    /// hand it back on completion, so there's no reason to allocate a fresh
    /// `Vec` per response - take it out, fill it, write it, get it back.
    resp_buf: Vec<u8>,
}

const SCRATCH_SIZE: usize = 8 * 1024;

impl Conn {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(SCRATCH_SIZE),
            scratch: Vec::with_capacity(SCRATCH_SIZE),
            resp_buf: Vec::with_capacity(SCRATCH_SIZE),
        }
    }

    /// Reads more bytes from the socket into `self.buf`. `Ok(false)` means EOF.
    async fn read_more(&mut self) -> io::Result<bool> {
        let mut scratch = std::mem::take(&mut self.scratch);
        if scratch.capacity() == 0 {
            scratch = Vec::with_capacity(SCRATCH_SIZE);
        }

        let BufResult(res, mut scratch) = self.stream.read(scratch).await;
        let n = res?;
        if n == 0 {
            self.scratch = scratch;
            return Ok(false);
        }

        self.buf.extend_from_slice(&scratch);
        scratch.clear();
        self.scratch = scratch;
        Ok(true)
    }

    /// Reads the next request head, if any. `Ok(None)` means the peer closed
    /// the connection cleanly between requests.
    async fn read_head(&mut self) -> io::Result<Option<http::Head>> {
        loop {
            match http::try_parse(&self.buf) {
                Ok(Some(head)) => {
                    self.buf.drain(..head.head_len);
                    return Ok(Some(head));
                }
                Ok(None) => {
                    if !self.read_more().await? {
                        if self.buf.is_empty() {
                            return Ok(None);
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed mid-request",
                        ));
                    }
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        }
    }

    async fn read_body(&mut self, len: usize) -> io::Result<Vec<u8>> {
        while self.buf.len() < len {
            if !self.read_more().await? {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed while reading body",
                ));
            }
        }
        Ok(self.buf.drain(..len).collect())
    }

    async fn write_response(&mut self) -> io::Result<()> {
        let out = std::mem::take(&mut self.resp_buf);
        let BufResult(res, mut out) = self.stream.write_all(out).await;
        out.clear();
        self.resp_buf = out;
        res
    }
}

async fn handle_connection(stream: TcpStream, state: Arc<AppState>) {
    stream.set_nodelay(true).ok();
    let mut conn = Conn::new(stream);

    loop {
        let head = match conn.read_head().await {
            Ok(Some(head)) => head,
            Ok(None) => return,
            Err(_) => return,
        };

        let body = match conn.read_body(head.content_length).await {
            Ok(body) => body,
            Err(_) => return,
        };

        route(&head, &body, &state, &mut conn.resp_buf).await;
        if conn.write_response().await.is_err() {
            return;
        }
    }
}

async fn route(head: &http::Head, body: &[u8], state: &Arc<AppState>, out: &mut Vec<u8>) {
    let path = head.path.split('?').next().unwrap_or("/");

    if path == "/todos" {
        return match head.method {
            Method::Get => list_todos(state, out).await,
            Method::Post => create_todo(body, state, out).await,
            _ => not_found(out),
        };
    }

    if let Some(rest) = path.strip_prefix("/todos/") {
        if !rest.is_empty() && !rest.contains('/') {
            let id = match parse_uuid_fast(rest) {
                Some(id) => id,
                None => return bad_request(out, "id must be a valid UUID"),
            };

            return match head.method {
                Method::Get => get_todo(id, state, out).await,
                Method::Patch => patch_todo(id, body, state, out).await,
                Method::Delete => delete_todo(id, state, out).await,
                _ => not_found(out),
            };
        }
    }

    not_found(out);
}

/// Parses the canonical hyphenated form (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
/// exactly 36 bytes) directly, instead of going through `uuid`'s
/// general-purpose parser - which also sniffs for braced/urn/simple/microsoft
/// variants we never see here, since every id in this API comes straight out
/// of a URL path or one of our own responses.
#[inline]
fn parse_uuid_fast(s: &str) -> Option<Uuid> {
    let b = s.as_bytes();
    if b.len() != 36 {
        return None;
    }
    if b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
        return None;
    }

    let mut bytes = [0u8; 16];
    let mut out_i = 0;
    let mut i = 0;
    while i < 36 {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            i += 1;
            continue;
        }
        let hi = hex_val(b[i])?;
        let lo = hex_val(b[i + 1])?;
        bytes[out_i] = (hi << 4) | lo;
        out_i += 1;
        i += 2;
    }
    Some(Uuid::from_bytes(bytes))
}

#[inline(always)]
fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Runs a DB task and waits for its answer. This is the bridge between
/// `Db::send_task`'s callback style and the fact that an HTTP response has
/// to go out on *this* request before we can read the next one: the
/// continuation drops the result into a oneshot channel, and we await that
/// channel here - not the task itself.
async fn db_call(state: &Arc<AppState>, task: Task) -> TaskResult {
    let (tx, rx) = flume::bounded(1);
    state.db.send_task(task, move |result| {
        let _ = tx.send(result);
    });
    rx.recv_async()
        .await
        .unwrap_or_else(|_| TaskResult::Error("db task dropped its reply channel".into()))
}

async fn list_todos(state: &Arc<AppState>, out: &mut Vec<u8>) {
    match db_call(state, Task::List).await {
        TaskResult::Many(items) => {
            let body = sonic_rs::to_vec(&items).unwrap_or_default();
            http::write_json_response(out, 200, &body, &[]);
        }
        TaskResult::Error(message) => internal_error(out, &message),
        _ => internal_error(out, "unexpected db result for List"),
    }
}

async fn create_todo(body: &[u8], state: &Arc<AppState>, out: &mut Vec<u8>) {
    let payload: CreateTodo = match sonic_rs::from_slice(body) {
        Ok(payload) => payload,
        Err(_) => return bad_request(out, "invalid request body"),
    };

    if payload.title.trim().is_empty() {
        return bad_request(out, "title must not be empty");
    }

    match db_call(state, Task::Insert(Todo::new(payload.title))).await {
        TaskResult::One(Some(todo)) => {
            let encoded = Bytes::from(sonic_rs::to_vec(&*todo).unwrap_or_default());
            state
                .cache
                .send_task(CacheTask::Write(todo.id, encoded.clone()), |_| {});

            let location = format!("/todos/{}", todo.id);
            http::write_json_response(out, 201, &encoded, &[("Location", &location)]);
        }
        TaskResult::Error(message) => internal_error(out, &message),
        _ => internal_error(out, "unexpected db result for Insert"),
    }
}

async fn get_todo(id: Uuid, state: &Arc<AppState>, out: &mut Vec<u8>) {
    let hot = state.cache.get_hot(id);
    if std::hint::likely(hot.is_some()) {
        http::write_json_response(out, 200, &hot.unwrap(), &[]);
        return;
    }

    if let Some(body) = state.cache.get_warm(id) {
        state
            .cache
            .send_task(CacheTask::FillL1(id, body.clone()), |_| {});
        http::write_json_response(out, 200, &body, &[]);
        return;
    }

    match db_call(state, Task::Get(id)).await {
        TaskResult::One(Some(todo)) => {
            let encoded = Bytes::from(sonic_rs::to_vec(&*todo).unwrap_or_default());
            state
                .cache
                .send_task(CacheTask::Write(id, encoded.clone()), |_| {});
            http::write_json_response(out, 200, &encoded, &[]);
        }
        TaskResult::One(None) => not_found(out),
        TaskResult::Error(message) => internal_error(out, &message),
        _ => internal_error(out, "unexpected db result for Get"),
    }
}

async fn patch_todo(id: Uuid, body: &[u8], state: &Arc<AppState>, out: &mut Vec<u8>) {
    let payload: PatchTodo = match sonic_rs::from_slice(body) {
        Ok(payload) => payload,
        Err(_) => return bad_request(out, "invalid request body"),
    };

    if let Some(ref title) = payload.title {
        if title.trim().is_empty() {
            return bad_request(out, "title must not be empty");
        }
    }

    match db_call(state, Task::Patch(id, payload.title, payload.completed)).await {
        TaskResult::One(Some(todo)) => {
            let encoded = Bytes::from(sonic_rs::to_vec(&*todo).unwrap_or_default());
            state
                .cache
                .send_task(CacheTask::Write(id, encoded.clone()), |_| {});
            http::write_json_response(out, 200, &encoded, &[]);
        }
        TaskResult::One(None) => {
            state.cache.send_task(CacheTask::Invalidate(id), |_| {});
            not_found(out);
        }
        TaskResult::Error(message) => internal_error(out, &message),
        _ => internal_error(out, "unexpected db result for Patch"),
    }
}

async fn delete_todo(id: Uuid, state: &Arc<AppState>, out: &mut Vec<u8>) {
    match db_call(state, Task::Delete(id)).await {
        TaskResult::Deleted(true) => {
            state.cache.send_task(CacheTask::Invalidate(id), |_| {});
            http::write_empty_response(out, 204);
        }
        TaskResult::Deleted(false) => not_found(out),
        TaskResult::Error(message) => internal_error(out, &message),
        _ => internal_error(out, "unexpected db result for Delete"),
    }
}

fn not_found(out: &mut Vec<u8>) {
    let body = sonic_rs::to_vec(&ErrorResp::not_found()).unwrap_or_default();
    http::write_json_response(out, 404, &body, &[]);
}

fn bad_request(out: &mut Vec<u8>, message: &str) {
    let body = sonic_rs::to_vec(&ErrorResp::invalid(message)).unwrap_or_default();
    http::write_json_response(out, 400, &body, &[]);
}

fn internal_error(out: &mut Vec<u8>, message: &str) {
    let body = sonic_rs::to_vec(&ErrorResp::internal(message)).unwrap_or_default();
    http::write_json_response(out, 500, &body, &[]);
}
