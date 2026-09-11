//! A todo API specialised for one request: `GET /todos/{uuid}`.
//!
//! Everything else (the other four endpoints, malformed input, anything that
//! isn't the hot shape) works correctly but takes the slow, general road.
//! The pipeline is the one from `api_reference.md`:
//!
//! ```text
//! packet -> parse -> L1 (hot slot) -> L2 (warm map) -> DB
//! ```
//!
//! Both cache tiers hold the *entire* precomputed HTTP response, so an L1 hit
//! is: one atomic load, one 36-byte memcmp, one write syscall. No header
//! building, no serialisation, no allocation, no copy.

#![feature(likely_unlikely)]

use std::error::Error;
use std::hash::BuildHasherDefault;
use std::hint::likely;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use compio::buf::{BufResult, IntoInner, IoBuf};
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpSocket, TcpStream};
use hashbrown::HashMap;
use nohash_hasher::NoHashHasher;
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use serde::Deserialize;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---------------------------------------------------------------- responses

const OK: &[u8] = b"HTTP/1.1 200 OK\r\n";
const CREATED: &[u8] = b"HTTP/1.1 201 Created\r\n";
const NO_CONTENT: &[u8] = b"HTTP/1.1 204 No Content\r\n";
const BAD_REQUEST: &[u8] = b"HTTP/1.1 400 Bad Request\r\n";
const NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\n";
const SERVER_ERROR: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\n";

/// Every error body this server can produce is a fixed string, so the whole
/// response is built once and handed out as a refcount bump afterwards.
static RESP_NOT_FOUND: LazyLock<Bytes> = LazyLock::new(|| {
    json_response(
        NOT_FOUND,
        br#"{"error":{"code":"TODO_NOT_FOUND","message":"Todo not found"}}"#,
        None,
    )
});
static RESP_BAD_ID: LazyLock<Bytes> = LazyLock::new(|| {
    json_response(
        BAD_REQUEST,
        br#"{"error":{"code":"INVALID_REQUEST","message":"id must be a valid UUID"}}"#,
        None,
    )
});
static RESP_BAD_BODY: LazyLock<Bytes> = LazyLock::new(|| {
    json_response(
        BAD_REQUEST,
        br#"{"error":{"code":"INVALID_REQUEST","message":"invalid request body"}}"#,
        None,
    )
});
static RESP_EMPTY_TITLE: LazyLock<Bytes> = LazyLock::new(|| {
    json_response(
        BAD_REQUEST,
        br#"{"error":{"code":"INVALID_REQUEST","message":"title must not be empty"}}"#,
        None,
    )
});
static RESP_NO_CONTENT: LazyLock<Bytes> = LazyLock::new(|| {
    let mut out = Vec::with_capacity(80);
    out.extend_from_slice(NO_CONTENT);
    out.extend_from_slice(b"Connection: keep-alive\r\nContent-Length: 0\r\n\r\n");
    Bytes::from(out)
});

fn json_response(status: &[u8], body: &[u8], location: Option<&[u8]>) -> Bytes {
    let mut out = Vec::with_capacity(160 + body.len());
    out.extend_from_slice(status);
    out.extend_from_slice(b"Content-Type: application/json\r\nConnection: keep-alive\r\n");
    if let Some(loc) = location {
        out.extend_from_slice(b"Location: ");
        out.extend_from_slice(loc);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Content-Length: ");
    out.extend_from_slice(itoa::Buffer::new().format(body.len()).as_bytes());
    out.extend_from_slice(b"\r\n\r\n");
    out.extend_from_slice(body);
    Bytes::from(out)
}

fn server_error(message: &str) -> Bytes {
    let mut body = Vec::with_capacity(64 + message.len());
    body.extend_from_slice(br#"{"error":{"code":"INTERNAL_ERROR","message":"#);
    push_json_string(&mut body, message);
    body.extend_from_slice(b"}}");
    json_response(SERVER_ERROR, &body, None)
}

// --------------------------------------------------------------------- todo

struct Todo {
    id: Uuid,
    title: String,
    completed: bool,
    created_at: SystemTime,
    updated_at: SystemTime,
}

/// `sonic_rs` handles the escaping for the one field that carries arbitrary
/// user text; everything else in a todo has a shape we already know.
fn push_json_string(out: &mut Vec<u8>, s: &str) {
    match sonic_rs::to_string(s) {
        Ok(escaped) => out.extend_from_slice(escaped.as_bytes()),
        Err(_) => out.extend_from_slice(b"\"\""),
    }
}

fn push_todo_json(out: &mut Vec<u8>, todo: &Todo) {
    let mut id_buf = [0u8; 36];
    out.extend_from_slice(br#"{"id":""#);
    out.extend_from_slice(todo.id.hyphenated().encode_lower(&mut id_buf).as_bytes());
    out.extend_from_slice(br#"","title":"#);
    push_json_string(out, &todo.title);
    out.extend_from_slice(br#","completed":"#);
    out.extend_from_slice(if todo.completed { b"true" } else { b"false" });
    out.extend_from_slice(br#","createdAt":""#);
    push_rfc3339(out, todo.created_at);
    out.extend_from_slice(br#"","updatedAt":""#);
    push_rfc3339(out, todo.updated_at);
    out.extend_from_slice(br#""}"#);
}

fn todo_response(status: &[u8], todo: &Todo, location: Option<&[u8]>) -> Bytes {
    let mut body = Vec::with_capacity(192 + todo.title.len());
    push_todo_json(&mut body, todo);
    json_response(status, &body, location)
}

// --------------------------------------------------------------------- time

fn push_u64_pad(out: &mut Vec<u8>, mut value: u64, width: usize) {
    let mut digits = [0u8; 20];
    let mut i = digits.len();
    loop {
        i -= 1;
        digits[i] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    for _ in (digits.len() - i)..width {
        out.push(b'0');
    }
    out.extend_from_slice(&digits[i..]);
}

/// `YYYY-MM-DDThh:mm:ss.ffffffZ`, microsecond precision to match what
/// Postgres actually stores in a `timestamptz`.
fn push_rfc3339(out: &mut Vec<u8>, t: SystemTime) {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = d.as_secs();
    let micros = d.subsec_micros() as u64;

    let days = (secs / 86_400) as i64;
    let tod = secs % 86_400;

    // civil_from_days (Howard Hinnant): shift the epoch to 0000-03-01 so leap
    // days land at the end of the 400-year era and the arithmetic stays exact.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u64;
    let month = (mp + if mp < 10 { 3 } else { -9 }) as u64;
    let year = (yoe + era * 400 + i64::from(month <= 2)) as u64;

    push_u64_pad(out, year, 4);
    out.push(b'-');
    push_u64_pad(out, month, 2);
    out.push(b'-');
    push_u64_pad(out, day, 2);
    out.push(b'T');
    push_u64_pad(out, tod / 3600, 2);
    out.push(b':');
    push_u64_pad(out, (tod % 3600) / 60, 2);
    out.push(b':');
    push_u64_pad(out, tod % 60, 2);
    out.push(b'.');
    push_u64_pad(out, micros, 6);
    out.push(b'Z');
}

// ----------------------------------------------------------------------- db

/// What a task wants done - the `Task` from `api_reference.md`.
enum Task {
    Get(Uuid),
    List,
    Insert(Todo),
    Patch(Uuid, Option<String>, Option<bool>),
    Delete(Uuid),
}

/// Results carry finished HTTP responses, not todos: whoever ran the query is
/// already holding the row, so it renders there and the response travels back
/// ready to write.
enum TaskResult {
    /// The response to send, plus the bytes to cache for this id (the cached
    /// form differs from the sent one for `201 Created`, which carries a
    /// `Location` header a later `GET` must not replay).
    Todo { send: Bytes, cache: Bytes },
    Response(Bytes),
    Missing,
    Deleted,
}

#[derive(Clone)]
struct Db {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

const COLUMNS: &str = "id, title, completed, created_at, updated_at";

impl Db {
    fn connect(url: &str) -> Result<Self, Box<dyn Error>> {
        let mut config: postgres::Config = url.parse()?;
        // Without these, a request whose connection dies mid-query hangs until
        // the OS's default TCP retransmission timeout gives up - tens of minutes.
        config
            .connect_timeout(Duration::from_secs(5))
            .tcp_user_timeout(Duration::from_secs(5));

        Ok(Self {
            pool: Pool::builder()
                .max_size(16)
                .connection_timeout(Duration::from_secs(5))
                .build(PostgresConnectionManager::new(config, NoTls))?,
        })
    }

    fn migrate(&self) -> Result<(), Box<dyn Error>> {
        self.pool.get()?.batch_execute(
            "CREATE TABLE IF NOT EXISTS todo (
                id UUID PRIMARY KEY,
                title TEXT NOT NULL,
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )?;
        Ok(())
    }

    /// `db.send_task(Task::Get(id), |result| { ... })` from `api_reference.md`:
    /// the query goes to the blocking pool and the continuation runs with the
    /// answer when it lands.
    fn send_task<F>(&self, task: Task, continuation: F)
    where
        F: FnOnce(TaskResult) + 'static,
    {
        let db = self.clone();
        compio::runtime::spawn(async move {
            let result = compio::runtime::spawn_blocking(move || db.run(task))
                .await
                .unwrap_or_else(|_| {
                    TaskResult::Response(server_error("db worker task panicked"))
                });
            continuation(result);
        })
        .detach();
    }

    fn run(&self, task: Task) -> TaskResult {
        match self.try_run(task) {
            Ok(result) => result,
            Err(e) => TaskResult::Response(server_error(&e.to_string())),
        }
    }

    fn try_run(&self, task: Task) -> Result<TaskResult, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        Ok(match task {
            Task::Get(id) => {
                let sql = format!("SELECT {COLUMNS} FROM todo WHERE id = $1");
                match conn.query_opt(&sql, &[&id])? {
                    Some(row) => ok_todo(&row_to_todo(&row)),
                    None => TaskResult::Missing,
                }
            }

            Task::List => {
                let sql = format!("SELECT {COLUMNS} FROM todo ORDER BY created_at DESC");
                let rows = conn.query(&sql, &[])?;
                let mut body = Vec::with_capacity(16 + rows.len() * 192);
                body.push(b'[');
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        body.push(b',');
                    }
                    push_todo_json(&mut body, &row_to_todo(row));
                }
                body.push(b']');
                TaskResult::Response(json_response(OK, &body, None))
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

                let mut location = Vec::with_capacity(43);
                let mut id_buf = [0u8; 36];
                location.extend_from_slice(b"/todos/");
                location.extend_from_slice(todo.id.hyphenated().encode_lower(&mut id_buf).as_bytes());

                TaskResult::Todo {
                    send: todo_response(CREATED, &todo, Some(&location)),
                    cache: todo_response(OK, &todo, None),
                }
            }

            Task::Patch(id, title, completed) => {
                let sql = format!(
                    "UPDATE todo SET
                        title = COALESCE($2, title),
                        completed = COALESCE($3, completed),
                        updated_at = $4
                     WHERE id = $1
                     RETURNING {COLUMNS}"
                );
                match conn.query_opt(&sql, &[&id, &title, &completed, &SystemTime::now()])? {
                    Some(row) => ok_todo(&row_to_todo(&row)),
                    None => TaskResult::Missing,
                }
            }

            Task::Delete(id) => {
                if conn.execute("DELETE FROM todo WHERE id = $1", &[&id])? > 0 {
                    TaskResult::Deleted
                } else {
                    TaskResult::Missing
                }
            }
        })
    }
}

fn ok_todo(todo: &Todo) -> TaskResult {
    let response = todo_response(OK, todo, None);
    TaskResult::Todo {
        send: response.clone(),
        cache: response,
    }
}

fn row_to_todo(row: &postgres::Row) -> Todo {
    Todo {
        id: row.get(0),
        title: row.get(1),
        completed: row.get(2),
        created_at: row.get(3),
        updated_at: row.get(4),
    }
}

// -------------------------------------------------------------------- cache

struct HotEntry {
    /// Canonical lowercase-hyphenated form of the id, so an L1 check memcmps
    /// the raw path bytes off the wire - no hex decode to compare equality.
    id_ascii: [u8; 36],
    id: Uuid,
    response: Bytes,
}

// A random UUID folded to 64 bits is already spread over the whole range, so
// hashing it again would just burn cycles - key the map on the fold itself.
type WarmMap = HashMap<u64, (Uuid, Bytes), BuildHasherDefault<NoHashHasher<u64>>>;

enum CacheTask {
    FillL1(Uuid, Bytes),
    /// DB hit: fill both tiers ("FillAll" in the diagram).
    Write(Uuid, Bytes),
    Invalidate(Uuid),
}

struct Cache {
    hot: ArcSwapOption<HotEntry>,
    warm: ArcSwap<WarmMap>,
}

#[inline(always)]
fn fold(id: Uuid) -> u64 {
    let n = id.as_u128();
    (n as u64) ^ ((n >> 64) as u64)
}

impl Cache {
    fn new() -> Self {
        Self {
            hot: ArcSwapOption::const_empty(),
            warm: ArcSwap::from_pointee(WarmMap::default()),
        }
    }

    #[inline]
    fn get_hot(&self, id_bytes: &[u8]) -> Option<Bytes> {
        let guard = self.hot.load();
        let entry = guard.as_deref()?;
        (entry.id_ascii == id_bytes).then(|| entry.response.clone())
    }

    fn get_warm(&self, id: Uuid) -> Option<Bytes> {
        let map = self.warm.load();
        let (stored, response) = map.get(&fold(id))?;
        (*stored == id).then(|| response.clone())
    }

    /// `cache.send_task(Task::Write(id, value), |_| {})` from
    /// `api_reference.md`. It's an atomic swap rather than I/O, but it still
    /// gets dispatched as its own task: filling the cache is not the
    /// responding request's problem, exactly like the diagram branches it off
    /// to the side of `answer`.
    fn send_task<F>(self: &Arc<Self>, task: CacheTask, continuation: F)
    where
        F: FnOnce(()) + 'static,
    {
        let cache = Arc::clone(self);
        compio::runtime::spawn(async move {
            match task {
                CacheTask::FillL1(id, response) => cache.promote(id, response),
                CacheTask::Write(id, response) => {
                    cache.put_warm(id, response.clone());
                    cache.promote(id, response);
                }
                CacheTask::Invalidate(id) => cache.invalidate(id),
            }
            continuation(());
        })
        .detach();
    }

    fn promote(&self, id: Uuid, response: Bytes) {
        let mut id_ascii = [0u8; 36];
        id.hyphenated().encode_lower(&mut id_ascii);
        self.hot.store(Some(Arc::new(HotEntry {
            id_ascii,
            id,
            response,
        })));
    }

    fn put_warm(&self, id: Uuid, response: Bytes) {
        let mut next = (**self.warm.load()).clone();
        next.insert(fold(id), (id, response));
        self.warm.store(Arc::new(next));
    }

    /// Drops a cached response from both tiers, so a stale body can never
    /// outlive the write that invalidated it.
    fn invalidate(&self, id: Uuid) {
        if matches!(self.hot.load().as_deref(), Some(e) if e.id == id) {
            self.hot.store(None);
        }

        let current = self.warm.load();
        if current.get(&fold(id)).is_some_and(|(stored, _)| *stored == id) {
            let mut next = (**current).clone();
            next.remove(&fold(id));
            self.warm.store(Arc::new(next));
        }
    }
}

// ------------------------------------------------------------------- parsing

const GET_PREFIX: &[u8] = b"GET /todos/";
const GET_SUFFIX: &[u8] = b" HTTP/1.1\r\n";
const ID_LEN: usize = 36;
const GET_LINE_LEN: usize = GET_PREFIX.len() + ID_LEN + GET_SUFFIX.len();

enum Head {
    /// `GET /todos/<uuid> HTTP/1.1\r\n...\r\n\r\n` - the only shape the hot
    /// loop ever takes, recognised without touching httparse.
    Get { id: [u8; ID_LEN], len: usize },
    Other {
        method: Method,
        path: String,
        content_length: usize,
        len: usize,
    },
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Method {
    Get,
    Post,
    Patch,
    Delete,
    Other,
}

enum Parsed {
    Head(Head),
    NeedMore,
    Bad,
}

fn parse(buf: &[u8]) -> Parsed {
    if buf.len() < GET_PREFIX.len() {
        return Parsed::NeedMore;
    }

    if &buf[..GET_PREFIX.len()] == GET_PREFIX {
        if buf.len() < GET_LINE_LEN {
            return Parsed::NeedMore;
        }
        if &buf[GET_PREFIX.len() + ID_LEN..GET_LINE_LEN] == GET_SUFFIX {
            return match memchr::memmem::find(&buf[GET_LINE_LEN..], b"\r\n\r\n") {
                Some(end) => {
                    let headers = &buf[GET_LINE_LEN..GET_LINE_LEN + end];
                    // A GET may legally carry a body. Ignoring its
                    // Content-Length would leave the body in the buffer and
                    // desync whatever request follows it on this connection,
                    // so hand those (vanishingly rare) requests to httparse.
                    if has_content_length(headers) {
                        general(buf)
                    } else {
                        let mut id = [0u8; ID_LEN];
                        id.copy_from_slice(&buf[GET_PREFIX.len()..GET_PREFIX.len() + ID_LEN]);
                        Parsed::Head(Head::Get {
                            id,
                            len: GET_LINE_LEN + end + 4,
                        })
                    }
                }
                None => Parsed::NeedMore,
            };
        }
    }

    general(buf)
}

fn general(buf: &[u8]) -> Parsed {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);

    match req.parse(buf) {
        Ok(httparse::Status::Complete(len)) => Parsed::Head(Head::Other {
            method: match req.method.unwrap_or("") {
                "GET" => Method::Get,
                "POST" => Method::Post,
                "PATCH" => Method::Patch,
                "DELETE" => Method::Delete,
                _ => Method::Other,
            },
            path: req.path.unwrap_or("/").to_string(),
            content_length: req
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                .and_then(|h| std::str::from_utf8(h.value).ok())
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0),
            len,
        }),
        Ok(httparse::Status::Partial) => Parsed::NeedMore,
        Err(_) => Parsed::Bad,
    }
}

/// Walks header lines rather than scanning every offset for the name - a
/// handful of `memchr` calls instead of a sliding compare over the block.
fn has_content_length(mut headers: &[u8]) -> bool {
    const NAME: &[u8] = b"content-length";
    loop {
        let end = memchr::memchr(b'\n', headers).unwrap_or(headers.len());
        let line = &headers[..end];
        if line.len() > NAME.len() && line[..NAME.len()].eq_ignore_ascii_case(NAME) {
            return true;
        }
        if end >= headers.len() {
            return false;
        }
        headers = &headers[end + 1..];
    }
}

/// The canonical hyphenated form, parsed directly. `uuid`'s parser also
/// sniffs braced/urn/simple variants, none of which this API ever sees.
#[inline]
fn parse_uuid(b: &[u8]) -> Option<Uuid> {
    if b.len() != ID_LEN || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
        return None;
    }

    let mut bytes = [0u8; 16];
    let mut out = 0;
    let mut i = 0;
    while i < ID_LEN {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            i += 1;
            continue;
        }
        bytes[out] = (hex(b[i])? << 4) | hex(b[i + 1])?;
        out += 1;
        i += 2;
    }
    Some(Uuid::from_bytes(bytes))
}

#[inline(always)]
fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// ------------------------------------------------------------------- serving

struct State {
    db: Db,
    cache: Arc<Cache>,
}

const READ_SIZE: usize = 8 * 1024;

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    /// Reads straight into the tail of the buffer - `compio` hands the buffer
    /// back with its length already advanced, so nothing is copied twice.
    /// `Ok(false)` means EOF.
    async fn fill(&mut self) -> io::Result<bool> {
        let mut buf = std::mem::take(&mut self.buf);
        if buf.capacity() - buf.len() < 1024 {
            buf.reserve(READ_SIZE);
        }
        let len = buf.len();

        let BufResult(res, slice) = self.stream.read(buf.slice(len..)).await;
        self.buf = slice.into_inner();
        Ok(res? != 0)
    }

    #[inline]
    fn consume(&mut self, n: usize) {
        if n == self.buf.len() {
            self.buf.clear();
        } else {
            self.buf.drain(..n);
        }
    }

    async fn read_head(&mut self) -> io::Result<Option<Head>> {
        loop {
            match parse(&self.buf) {
                Parsed::Head(head) => {
                    let len = match &head {
                        Head::Get { len, .. } | Head::Other { len, .. } => *len,
                    };
                    self.consume(len);
                    return Ok(Some(head));
                }
                Parsed::NeedMore => {
                    if !self.fill().await? {
                        return if self.buf.is_empty() {
                            Ok(None)
                        } else {
                            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "partial request"))
                        };
                    }
                }
                Parsed::Bad => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed request"));
                }
            }
        }
    }

    async fn read_body(&mut self, len: usize) -> io::Result<Vec<u8>> {
        while self.buf.len() < len {
            if !self.fill().await? {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "partial body"));
            }
        }
        let body = self.buf[..len].to_vec();
        self.consume(len);
        Ok(body)
    }

    async fn send(&mut self, response: Bytes) -> io::Result<()> {
        self.stream.write_all(response).await.0
    }
}

async fn serve(stream: TcpStream, state: Arc<State>) {
    stream.set_nodelay(true).ok();
    let mut conn = Conn {
        stream,
        buf: Vec::with_capacity(READ_SIZE),
    };

    loop {
        let head = match conn.read_head().await {
            Ok(Some(head)) => head,
            _ => return,
        };

        let response = match head {
            Head::Get { id, .. } => get(&id, &state).await,
            Head::Other {
                method,
                path,
                content_length,
                ..
            } => {
                let body = match conn.read_body(content_length).await {
                    Ok(body) => body,
                    Err(_) => return,
                };
                route(method, &path, &body, &state).await
            }
        };

        if conn.send(response).await.is_err() {
            return;
        }
    }
}

async fn get(id_bytes: &[u8], state: &Arc<State>) -> Bytes {
    // L1: one atomic load and a 36-byte compare against the raw path bytes.
    let hot = state.cache.get_hot(id_bytes);
    if likely(hot.is_some()) {
        return hot.unwrap();
    }

    let Some(id) = parse_uuid(id_bytes) else {
        return RESP_BAD_ID.clone();
    };

    // L2 hit: answer now, fill L1 off to the side.
    if let Some(response) = state.cache.get_warm(id) {
        state
            .cache
            .send_task(CacheTask::FillL1(id, response.clone()), |_| {});
        return response;
    }

    match db_call(state, Task::Get(id)).await {
        TaskResult::Todo { send, cache } => {
            state.cache.send_task(CacheTask::Write(id, cache), |_| {});
            send
        }
        TaskResult::Missing => RESP_NOT_FOUND.clone(),
        TaskResult::Response(response) => response,
        TaskResult::Deleted => server_error("unexpected db result for Get"),
    }
}

async fn route(method: Method, path: &str, body: &[u8], state: &Arc<State>) -> Bytes {
    let path = path.split('?').next().unwrap_or("/");

    if path == "/todos" {
        return match method {
            Method::Get => match db_call(state, Task::List).await {
                TaskResult::Response(response) => response,
                _ => server_error("unexpected db result for List"),
            },
            Method::Post => create(body, state).await,
            _ => RESP_NOT_FOUND.clone(),
        };
    }

    let Some(rest) = path.strip_prefix("/todos/") else {
        return RESP_NOT_FOUND.clone();
    };
    if rest.is_empty() || rest.contains('/') {
        return RESP_NOT_FOUND.clone();
    }

    // A GET here only arrives when the fast path declined it (HTTP/1.0, a
    // query string, a body); it still gets the cache tiers.
    if method == Method::Get {
        return get(rest.as_bytes(), state).await;
    }

    let Some(id) = parse_uuid(rest.as_bytes()) else {
        return RESP_BAD_ID.clone();
    };

    match method {
        Method::Patch => patch(id, body, state).await,
        Method::Delete => delete(id, state).await,
        _ => RESP_NOT_FOUND.clone(),
    }
}

async fn create(body: &[u8], state: &Arc<State>) -> Bytes {
    #[derive(Deserialize)]
    struct Create {
        title: String,
    }

    let Ok(payload) = sonic_rs::from_slice::<Create>(body) else {
        return RESP_BAD_BODY.clone();
    };
    if payload.title.trim().is_empty() {
        return RESP_EMPTY_TITLE.clone();
    }

    let now = SystemTime::now();
    let id = Uuid::new_v4();
    let todo = Todo {
        id,
        title: payload.title,
        completed: false,
        created_at: now,
        updated_at: now,
    };

    match db_call(state, Task::Insert(todo)).await {
        TaskResult::Todo { send, cache } => {
            state.cache.send_task(CacheTask::Write(id, cache), |_| {});
            send
        }
        TaskResult::Response(response) => response,
        _ => server_error("unexpected db result for Insert"),
    }
}

async fn patch(id: Uuid, body: &[u8], state: &Arc<State>) -> Bytes {
    #[derive(Deserialize)]
    struct Patch {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        completed: Option<bool>,
    }

    let Ok(payload) = sonic_rs::from_slice::<Patch>(body) else {
        return RESP_BAD_BODY.clone();
    };
    if payload.title.as_deref().is_some_and(|t| t.trim().is_empty()) {
        return RESP_EMPTY_TITLE.clone();
    }

    match db_call(state, Task::Patch(id, payload.title, payload.completed)).await {
        TaskResult::Todo { send, cache } => {
            state.cache.send_task(CacheTask::Write(id, cache), |_| {});
            send
        }
        TaskResult::Missing => {
            state.cache.send_task(CacheTask::Invalidate(id), |_| {});
            RESP_NOT_FOUND.clone()
        }
        TaskResult::Response(response) => response,
        TaskResult::Deleted => server_error("unexpected db result for Patch"),
    }
}

async fn delete(id: Uuid, state: &Arc<State>) -> Bytes {
    match db_call(state, Task::Delete(id)).await {
        TaskResult::Deleted => {
            state.cache.send_task(CacheTask::Invalidate(id), |_| {});
            RESP_NO_CONTENT.clone()
        }
        TaskResult::Missing => RESP_NOT_FOUND.clone(),
        TaskResult::Response(response) => response,
        TaskResult::Todo { .. } => server_error("unexpected db result for Delete"),
    }
}

/// Bridges `send_task`'s continuation to the fact that this request owes an
/// answer before the connection can read the next one: the continuation drops
/// the result into a oneshot, and we await that - not the task.
async fn db_call(state: &Arc<State>, task: Task) -> TaskResult {
    let (tx, rx) = flume::bounded(1);
    state.db.send_task(task, move |result| {
        let _ = tx.send(result);
    });
    rx.recv_async()
        .await
        .unwrap_or_else(|_| TaskResult::Response(server_error("db task dropped its reply channel")))
}

// ---------------------------------------------------------------------- main

fn main() {
    dotenvy::dotenv().ok();

    let addr: SocketAddr = std::env::var("TODOAPP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:13265".into())
        .parse()
        .expect("TODOAPP_ADDR must be a valid host:port");

    let url = std::env::var("POSTGRES_URL")
        .expect("POSTGRES_URL must be set (in the environment or .env)");

    let db = Db::connect(&url).expect("failed to connect to Postgres");
    db.migrate().expect("failed to run schema migration");

    let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    let state = Arc::new(State {
        db,
        cache: Arc::new(Cache::new()),
    });

    println!("todoapp listening on {addr} across {workers} worker thread(s)");

    let threads: Vec<_> = (0..workers)
        .map(|_| {
            let state = state.clone();
            std::thread::spawn(move || {
                runtime()
                    .expect("failed to start compio runtime")
                    .block_on(accept_loop(addr, state))
            })
        })
        .collect();

    for thread in threads {
        let _ = thread.join();
    }
}

/// The ring this worker owns.
///
/// `DEFER_TASKRUN` is the whole game: without it every completion arriving in softirq context
/// queues task_work and IPIs the owning thread, and that cost is paid *per completion*. With it
/// completions pile up and get reaped in a batch on the next wait. Measured on the bare ring:
/// 10.4 -> 4.9 us of kernel time per request. It is silently ignored unless `SINGLE_ISSUER` is
/// set too, and the kernel refuses it together with `SQPOLL`.
fn runtime() -> io::Result<compio::runtime::Runtime> {
    let off = |k: &str| std::env::var(k).map(|v| v == "0").unwrap_or(false);
    let mut proactor = compio::driver::ProactorBuilder::new();
    proactor.capacity(2048).cqsize(8192);
    if !off("TODOAPP_DEFER") {
        proactor.single_issuer(true).defer_taskrun(true);
    } else if !off("TODOAPP_COOP") {
        proactor.coop_taskrun(true).taskrun_flag(true);
    }
    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor)
        .build()
}

/// One `compio` runtime per core, every one of them accepting on the same port
/// through `SO_REUSEPORT`.
async fn accept_loop(addr: SocketAddr, state: Arc<State>) {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4().await,
        SocketAddr::V6(_) => TcpSocket::new_v6().await,
    }
    .expect("failed to create socket");

    socket.set_reuseport(true).expect("failed to set SO_REUSEPORT");
    socket.set_reuseaddr(true).expect("failed to set SO_REUSEADDR");
    socket.bind(addr).await.expect("failed to bind");
    let listener: TcpListener = socket.listen(1024).await.expect("failed to listen");

    while let Ok((stream, _)) = listener.accept().await {
        let state = state.clone();
        compio::runtime::spawn(serve(stream, state)).detach();
    }
}
