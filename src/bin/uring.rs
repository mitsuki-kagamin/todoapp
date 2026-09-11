//! A todo API on a hand-rolled io_uring loop.
//!
//! No `Future`, no waker, no executor. One ring per worker, `SO_REUSEPORT`, and a flat
//! `CQE -> match connection state -> build response -> SQE` path. Connection state lives in a
//! slot indexed by fd, and `user_data` carries (tag, generation, fd), so nothing is allocated
//! per operation -- compio spent 2 malloc/free pairs, ~8 locked RMWs, 4 SipHashes of a pointer
//! and ~3 task polls on every request just to get there.
//!
//! The pipeline is the one from `api_reference.md`:
//!
//! ```text
//! packet -> parse -> L1 (hot slot) -> L2 (warm map) -> DB
//! ```
//!
//! and `send_task(Task, continuation)` is finally what it wants to be: a continuation is a
//! plain enum value parked in the connection's slot, dispatching one is pushing a small value
//! onto a queue. Through compio the same shape had to be threaded through oneshot channels.
//!
//! Ring flags and mechanisms were picked by measurement, not by reputation (see README):
//! `SINGLE_ISSUER | DEFER_TASKRUN` was worth 2x, while direct descriptors measured *worse* and
//! multishot recv with a provided buffer ring was noise -- so this uses plain fds and
//! single-shot recv, which also avoids the ENOBUFS and IOU_PBUF_RING_INC traps entirely.
//!
//! Three invariants carry most of the correctness:
//!
//! * **At most one operation in flight per connection.** io_uring gives no ordering guarantee
//!   between two independently submitted SQEs, so two concurrent sends on one socket would
//!   interleave and corrupt the byte stream.
//! * **Responses stay in request order.** A connection waiting on the database stops parsing
//!   what is left in its buffer; otherwise a pipelined cache hit behind a cache miss would
//!   overtake it and break the client's request/response correspondence.
//! * **The fd is closed synchronously, and only with nothing in flight.** Closing through an
//!   SQE frees the fd inside the operation, so `accept` can hand the same number out before the
//!   close completion is posted -- and that late completion would then clobber the live
//!   connection that inherited the slot.

use std::error::Error;
use std::hash::BuildHasherDefault;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use hashbrown::HashMap;
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use nohash_hasher::NoHashHasher;
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use serde::Deserialize;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ------------------------------------------------ ported verbatim from the compio server
// These are already verified: the parser against the pathological request shapes, the date
// formatter byte-for-byte against postgres `to_char`, the escaping against hostile titles
// through a strict JSON parser. Rewriting them would risk the contract for nothing.

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

/// Only the fills are dispatched to the side. Invalidation runs inline in `finish`, before
/// the mutating response is staged: queued, it could land after that response is already on
/// the wire, leaving a window where another worker still serves the stale body.
enum CacheTask {
    FillL1(Uuid, Bytes),
    /// DB hit: fill both tiers ("FillAll" in the diagram).
    Write(Uuid, Bytes),
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

// ---------------------------------------------------------------- continuations

/// What to do with a database answer once it lands -- the continuation from
/// `api_reference.md`, as a value rather than a closure. Zero allocation, no future, no waker:
/// it just sits in the connection's slot until the reply arrives.
enum Cont {
    Get(Uuid),
    List,
    Create(Uuid),
    Patch(Uuid),
    Delete(Uuid),
}

/// Either the answer is already known, or a task has to be dispatched first.
enum Decision {
    Respond(Bytes),
    Await(Task, Cont),
}

/// `cache.send_task(...)` lands here: a queue drained at the end of each completion batch, so
/// filling the cache is never the responding request's problem -- exactly as the diagram
/// branches it off to the side of `answer`.
type CacheQ = Vec<CacheTask>;

fn plan(head: &Head, body: &[u8], cache: &Cache, q: &mut CacheQ) -> Decision {
    match head {
        Head::Get { id, .. } => plan_get(id, cache, q),
        Head::Other {
            method,
            path,
            ..
        } => plan_route(*method, path, body, cache, q),
    }
}

fn plan_get(id_bytes: &[u8], cache: &Cache, q: &mut CacheQ) -> Decision {
    // L1: one load and a 36-byte compare against the raw path bytes, no hex decode.
    if let Some(hot) = cache.get_hot(id_bytes) {
        return Decision::Respond(hot);
    }
    let Some(id) = parse_uuid(id_bytes) else {
        return Decision::Respond(RESP_BAD_ID.clone());
    };
    // L2 hit: answer now, fill L1 off to the side.
    if let Some(response) = cache.get_warm(id) {
        q.push(CacheTask::FillL1(id, response.clone()));
        return Decision::Respond(response);
    }
    Decision::Await(Task::Get(id), Cont::Get(id))
}

fn plan_route(method: Method, path: &str, body: &[u8], cache: &Cache, q: &mut CacheQ) -> Decision {
    let path = path.split('?').next().unwrap_or("/");

    if path == "/todos" {
        return match method {
            Method::Get => Decision::Await(Task::List, Cont::List),
            Method::Post => plan_create(body),
            _ => Decision::Respond(RESP_NOT_FOUND.clone()),
        };
    }

    let Some(rest) = path.strip_prefix("/todos/") else {
        return Decision::Respond(RESP_NOT_FOUND.clone());
    };
    if rest.is_empty() || rest.contains('/') {
        return Decision::Respond(RESP_NOT_FOUND.clone());
    }

    // A GET only arrives here when the fast path declined it (HTTP/1.0, a query string, a
    // body); it still gets both cache tiers.
    if method == Method::Get {
        return plan_get(rest.as_bytes(), cache, q);
    }

    let Some(id) = parse_uuid(rest.as_bytes()) else {
        return Decision::Respond(RESP_BAD_ID.clone());
    };

    match method {
        Method::Patch => plan_patch(id, body),
        Method::Delete => Decision::Await(Task::Delete(id), Cont::Delete(id)),
        _ => Decision::Respond(RESP_NOT_FOUND.clone()),
    }
}

fn plan_create(body: &[u8]) -> Decision {
    #[derive(Deserialize)]
    struct Create {
        title: String,
    }

    let Ok(payload) = sonic_rs::from_slice::<Create>(body) else {
        return Decision::Respond(RESP_BAD_BODY.clone());
    };
    if payload.title.trim().is_empty() {
        return Decision::Respond(RESP_EMPTY_TITLE.clone());
    }

    let now = SystemTime::now();
    let id = Uuid::new_v4();
    Decision::Await(
        Task::Insert(Todo {
            id,
            title: payload.title,
            completed: false,
            created_at: now,
            updated_at: now,
        }),
        Cont::Create(id),
    )
}

fn plan_patch(id: Uuid, body: &[u8]) -> Decision {
    #[derive(Deserialize)]
    struct Patch {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        completed: Option<bool>,
    }

    let Ok(payload) = sonic_rs::from_slice::<Patch>(body) else {
        return Decision::Respond(RESP_BAD_BODY.clone());
    };
    if payload.title.as_deref().is_some_and(|t| t.trim().is_empty()) {
        return Decision::Respond(RESP_EMPTY_TITLE.clone());
    }
    Decision::Await(
        Task::Patch(id, payload.title, payload.completed),
        Cont::Patch(id),
    )
}

/// Run a continuation against the answer that came back.
///
/// Writes that invalidate run **inline, before the response is staged**, not as a queued task.
/// Dispatching them to the side leaves a window where the mutating response is already on the
/// wire while another worker can still serve the stale body.
fn finish(cont: Cont, result: TaskResult, cache: &Cache, q: &mut CacheQ) -> Bytes {
    match cont {
        Cont::Get(id) => match result {
            TaskResult::Todo { send, cache: fill } => {
                q.push(CacheTask::Write(id, fill));
                send
            }
            TaskResult::Missing => RESP_NOT_FOUND.clone(),
            TaskResult::Response(response) => response,
            TaskResult::Deleted => server_error("unexpected db result for Get"),
        },
        Cont::List => match result {
            TaskResult::Response(response) => response,
            _ => server_error("unexpected db result for List"),
        },
        Cont::Create(id) => match result {
            TaskResult::Todo { send, cache: fill } => {
                q.push(CacheTask::Write(id, fill));
                send
            }
            TaskResult::Response(response) => response,
            _ => server_error("unexpected db result for Insert"),
        },
        Cont::Patch(id) => match result {
            TaskResult::Todo { send, cache: fill } => {
                cache.put_warm(id, fill.clone());
                cache.promote(id, fill);
                send
            }
            TaskResult::Missing => {
                cache.invalidate(id);
                RESP_NOT_FOUND.clone()
            }
            TaskResult::Response(response) => response,
            TaskResult::Deleted => server_error("unexpected db result for Patch"),
        },
        Cont::Delete(id) => match result {
            TaskResult::Deleted => {
                cache.invalidate(id);
                RESP_NO_CONTENT.clone()
            }
            TaskResult::Missing => RESP_NOT_FOUND.clone(),
            TaskResult::Response(response) => response,
            TaskResult::Todo { .. } => server_error("unexpected db result for Delete"),
        },
    }
}

fn run_cache_tasks(cache: &Cache, q: &mut CacheQ) {
    for task in q.drain(..) {
        match task {
            CacheTask::FillL1(id, response) => cache.promote(id, response),
            CacheTask::Write(id, response) => {
                cache.put_warm(id, response.clone());
                cache.promote(id, response);
            }
        }
    }
}

// ------------------------------------------------------------------ the blocking db pool

struct Job {
    worker: u16,
    fd: u32,
    generation: u32,
    task: Task,
    cont: Cont,
}

struct Reply {
    fd: u32,
    generation: u32,
    result: TaskResult,
    cont: Cont,
}

/// Where a worker's replies go, and how to wake it.
struct Mailbox {
    tx: flume::Sender<Reply>,
    efd: i32,
}

fn start_db_pool(db: Db, threads: usize, boxes: Arc<Vec<Mailbox>>) -> flume::Sender<Job> {
    let (tx, rx) = flume::unbounded::<Job>();
    for _ in 0..threads {
        let rx = rx.clone();
        let db = db.clone();
        let boxes = Arc::clone(&boxes);
        std::thread::spawn(move || {
            while let Ok(job) = rx.recv() {
                let result = db.run(job.task);
                let mailbox = &boxes[job.worker as usize];
                let reply = Reply {
                    fd: job.fd,
                    generation: job.generation,
                    result,
                    cont: job.cont,
                };
                // Queue first, signal second. The other order loses the wakeup: the worker can
                // read the eventfd and find nothing, then sleep with the reply already queued.
                //
                // These threads must never touch a worker's ring: SINGLE_ISSUER means exactly
                // one thread may ever submit to it, and violating that fails at runtime, not
                // at compile time. An 8-byte write is all they do.
                if mailbox.tx.send(reply).is_ok() {
                    let one: u64 = 1;
                    unsafe {
                        libc::write(mailbox.efd, &one as *const u64 as *const libc::c_void, 8)
                    };
                }
            }
        });
    }
    tx
}

// ------------------------------------------------------------------------ the event loop

/// The connection table is indexed by fd and never grows, so an in-flight recv can never have
/// its slot relocated. An fd at or beyond this is refused rather than accepted.
const MAX_CONNS: usize = 16384;
const RECV_CHUNK: usize = 4096;
/// A request that never completes is an attack, not a client.
const MAX_REQ: usize = 64 * 1024;
/// A send reporting no progress this many times running is not going to make any.
const MAX_ZERO_SENDS: u8 = 3;

const TAG_ACCEPT: u64 = 0;
const TAG_RECV: u64 = 1;
const TAG_SEND: u64 = 2;
/// Backoff timer: re-arm accept after the listener ran out of descriptors or memory.
const TAG_RETRY: u64 = 3;
/// The eventfd a database thread pokes when a reply is queued.
const TAG_EVENT: u64 = 4;

/// `user_data` is the whole per-operation bookkeeping: 8 bits of tag, 24 bits of generation,
/// 32 bits of fd. The generation is what lets a database reply that outlived its connection be
/// recognised and dropped -- a reply can land seconds late, long after the slot was reused.
#[inline(always)]
fn ud(tag: u64, generation: u32, fd: u32) -> u64 {
    (tag << 56) | ((generation as u64 & 0x00ff_ffff) << 32) | fd as u64
}
#[inline(always)]
fn ud_tag(u: u64) -> u64 {
    u >> 56
}
#[inline(always)]
fn ud_gen(u: u64) -> u32 {
    ((u >> 32) & 0x00ff_ffff) as u32
}
#[inline(always)]
fn ud_fd(u: u64) -> u32 {
    u as u32
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Phase {
    Free,
    Recv,
    Send,
    /// Nothing is in flight in the ring; the connection is parked on a database reply.
    Awaiting,
}

struct Conn {
    phase: Phase,
    /// Bumped on every close, so a reply from a previous occupant is recognisable.
    generation: u32,
    /// True while the ring owns an operation for this connection.
    inflight: bool,
    /// A database job is outstanding. While this is set the buffer is not parsed any further,
    /// which is what keeps a pipelined cache hit from overtaking the miss ahead of it.
    awaiting: bool,
    /// Bytes received and not yet consumed by a complete request.
    buf: Vec<u8>,
    /// Holds the single staged response alive for the duration of the send, so the kernel is
    /// handed the cached bytes where they lie instead of a copy.
    keep: Option<Bytes>,
    /// Backing store once more than one response is staged at a time.
    own: Vec<u8>,
    out_ptr: *const u8,
    out_len: usize,
    out_sent: usize,
    /// A reply that arrived while a send was still in flight; staged once that send finishes.
    reply_out: Option<Bytes>,
    zero_sends: u8,
}

impl Conn {
    const fn new() -> Self {
        Conn {
            phase: Phase::Free,
            generation: 0,
            inflight: false,
            awaiting: false,
            buf: Vec::new(),
            keep: None,
            own: Vec::new(),
            out_ptr: std::ptr::null(),
            out_len: 0,
            out_sent: 0,
            reply_out: None,
            zero_sends: 0,
        }
    }
}

#[inline]
fn consume(c: &mut Conn, n: usize) {
    if n == c.buf.len() {
        c.buf.clear();
    } else {
        c.buf.drain(..n);
    }
}

/// Add a response to what will go out next.
///
/// Only legal with no send in flight -- appending would otherwise mutate the very buffer the
/// kernel is reading from. Strict alternation guarantees it.
fn stage(c: &mut Conn, response: Bytes) {
    debug_assert!(!c.inflight, "staging into a buffer with a send in flight");
    if c.out_len == 0 && c.keep.is_none() {
        c.out_ptr = response.as_ptr();
        c.out_len = response.len();
        c.out_sent = 0;
        c.keep = Some(response);
    } else {
        if let Some(first) = c.keep.take() {
            c.own.clear();
            c.own.extend_from_slice(&first);
        }
        c.own.extend_from_slice(&response);
        c.out_ptr = c.own.as_ptr();
        c.out_len = c.own.len();
    }
}

fn reset_out(c: &mut Conn) {
    c.out_len = 0;
    c.out_sent = 0;
    c.keep = None;
    c.own.clear();
}

enum Pumped {
    /// Something is staged to send, or nothing more can be done without more bytes.
    Ready,
    /// Parked on the database.
    Awaiting,
    Bad,
}

/// Turn whatever complete requests are buffered into staged responses, stopping at the first
/// one that needs the database.
fn pump(
    c: &mut Conn,
    fd: u32,
    worker: u16,
    cache: &Cache,
    q: &mut CacheQ,
    db: &flume::Sender<Job>,
) -> Pumped {
    loop {
        let head = match parse(&c.buf) {
            Parsed::Head(head) => head,
            Parsed::NeedMore => {
                return if c.out_len == 0 && c.buf.len() > MAX_REQ {
                    Pumped::Bad
                } else {
                    Pumped::Ready
                };
            }
            Parsed::Bad => return Pumped::Bad,
        };

        let (head_len, body_len) = match &head {
            Head::Get { len, .. } => (*len, 0),
            Head::Other {
                len,
                content_length,
                ..
            } => (*len, *content_length),
        };
        // The body is part of the request: leaving it buffered would desync whatever follows.
        if c.buf.len() < head_len + body_len {
            return Pumped::Ready;
        }

        let body = c.buf[head_len..head_len + body_len].to_vec();
        let decision = plan(&head, &body, cache, q);
        consume(c, head_len + body_len);

        match decision {
            Decision::Respond(response) => stage(c, response),
            Decision::Await(task, cont) => {
                c.awaiting = true;
                let _ = db.send(Job {
                    worker,
                    fd,
                    generation: c.generation,
                    task,
                    cont,
                });
                return Pumped::Awaiting;
            }
        }
    }
}

/// Close the fd and free the slot in the same instruction stream.
///
/// Only legal with nothing in flight, which strict alternation guarantees. Bumping the
/// generation is what makes a late database reply for this connection detectably stale.
fn close_now(c: &mut Conn, fd: usize) {
    debug_assert!(!c.inflight, "closing fd {fd} with an operation in flight");
    unsafe { libc::close(fd as i32) };
    c.generation = c.generation.wrapping_add(1);
    c.phase = Phase::Free;
    c.awaiting = false;
    c.reply_out = None;
    c.zero_sends = 0;
    c.buf.clear();
    reset_out(c);
}

#[inline]
fn recv_op(c: &mut Conn, fd: u32) -> squeue::Entry {
    let len = c.buf.len();
    // Growing is safe only because nothing is in flight against the buffer. A recv armed with
    // zero spare capacity would return 0 and be indistinguishable from EOF.
    if c.buf.capacity() - len < RECV_CHUNK {
        c.buf.reserve(RECV_CHUNK);
    }
    let spare = c.buf.capacity() - len;
    let ptr = unsafe { c.buf.as_mut_ptr().add(len) };
    opcode::Recv::new(types::Fd(fd as i32), ptr, spare as u32)
        .build()
        .user_data(ud(TAG_RECV, c.generation, fd))
}

#[inline]
fn send_op(c: &Conn, fd: u32) -> squeue::Entry {
    let ptr = unsafe { c.out_ptr.add(c.out_sent) };
    let len = (c.out_len - c.out_sent) as u32;
    opcode::Send::new(types::Fd(fd as i32), ptr, len)
        .flags(libc::MSG_NOSIGNAL)
        .build()
        .user_data(ud(TAG_SEND, c.generation, fd))
}

fn report(res: i32, what: &str) {
    if res >= 0 {
        return;
    }
    let err = -res;
    if err != libc::ECONNRESET && err != libc::EPIPE && err != libc::ECANCELED {
        eprintln!("{what}: {}", std::io::Error::from_raw_os_error(err));
    }
}

fn env_flag(k: &str) -> bool {
    std::env::var(k).map(|v| v == "1").unwrap_or(false)
}
fn env_num(k: &str, d: u32) -> u32 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn cpus_in_mask() -> Vec<u32> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return vec![0];
        }
        (0..libc::CPU_SETSIZE as u32)
            .filter(|c| libc::CPU_ISSET(*c as usize, &set))
            .collect()
    }
}

fn pin_to(cpu: u32) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu as usize, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn make_listener(addr: std::net::SocketAddrV4) -> std::io::Result<i32> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let on: libc::c_int = 1;
        let p = &on as *const _ as *const libc::c_void;
        let sz = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, p, sz);
        libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, p, sz);
        let mut sa: libc::sockaddr_in = std::mem::zeroed();
        sa.sin_family = libc::AF_INET as u16;
        sa.sin_port = addr.port().to_be();
        sa.sin_addr.s_addr = u32::from_ne_bytes(addr.ip().octets());
        if libc::bind(
            fd,
            &sa as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        ) < 0
        {
            return Err(std::io::Error::last_os_error());
        }
        if libc::listen(fd, 4096) < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(fd)
    }
}

/// Raise the descriptor limit toward the connection table, since fds index it directly.
fn raise_nofile() {
    unsafe {
        let mut lim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 && lim.rlim_cur < lim.rlim_max {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

struct WorkerCfg {
    index: u16,
    addr: std::net::SocketAddrV4,
    cpu: u32,
    cache: Arc<Cache>,
    db: flume::Sender<Job>,
    replies: flume::Receiver<Reply>,
    efd: i32,
}

fn main() {
    // Writing to a peer that has gone away must not take the process down. Every send carries
    // MSG_NOSIGNAL; ignoring the signal is the second layer, because `panic = "abort"` turns a
    // stray SIGPIPE into a dead worker.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    raise_nofile();
    dotenvy::dotenv().ok();

    let addr: std::net::SocketAddrV4 = std::env::var("TODOAPP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:13267".into())
        .parse()
        .expect("TODOAPP_ADDR must be host:port");
    let url = std::env::var("POSTGRES_URL")
        .expect("POSTGRES_URL must be set (in the environment or .env)");

    let db = Db::connect(&url).expect("failed to connect to Postgres");
    db.migrate().expect("failed to run schema migration");

    let cpus = cpus_in_mask();
    let workers = env_num("TODOAPP_WORKERS", cpus.len() as u32).max(1) as usize;
    let db_threads = env_num("TODOAPP_DB_THREADS", 16).max(1) as usize;

    // One mailbox per worker, built before anything can post to one.
    let mut boxes = Vec::with_capacity(workers);
    let mut inboxes = Vec::with_capacity(workers);
    for _ in 0..workers {
        let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        assert!(efd >= 0, "eventfd: {}", std::io::Error::last_os_error());
        let (tx, rx) = flume::unbounded::<Reply>();
        boxes.push(Mailbox { tx, efd });
        inboxes.push((rx, efd));
    }
    let db_tx = start_db_pool(db, db_threads, Arc::new(boxes));

    let cache = Arc::new(Cache::new());
    println!("todoapp listening on {addr} across {workers} worker thread(s)");

    let handles: Vec<_> = inboxes
        .into_iter()
        .enumerate()
        .map(|(i, (replies, efd))| {
            let cfg = WorkerCfg {
                index: i as u16,
                addr,
                cpu: cpus[i % cpus.len()],
                cache: Arc::clone(&cache),
                db: db_tx.clone(),
                replies,
                efd,
            };
            std::thread::spawn(move || {
                if env_flag("TODOAPP_PIN") {
                    pin_to(cfg.cpu);
                }
                worker(cfg).expect("worker failed");
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
}

fn worker(cfg: WorkerCfg) -> std::io::Result<()> {
    let mut b: io_uring::Builder<squeue::Entry, cqueue::Entry> = IoUring::builder();
    b.setup_cqsize(8192);
    if !std::env::var("TODOAPP_DEFER")
        .map(|v| v == "0")
        .unwrap_or(false)
    {
        // Worth 2x on its own: without it every completion arriving in softirq context queues
        // task_work and IPIs this thread, and that cost is paid per completion. Silently
        // ignored unless SINGLE_ISSUER is set too.
        b.setup_single_issuer();
        b.setup_defer_taskrun();
    }
    let mut ring = b.build(2048)?;

    let lfd = make_listener(cfg.addr)?;
    let mut conns: Vec<Conn> = (0..MAX_CONNS).map(|_| Conn::new()).collect();
    let mut cache_q: CacheQ = Vec::with_capacity(64);
    let mut stale_replies: u64 = 0;

    // Both live as long as the loop, so the kernel's pointers into them stay valid.
    let retry_after = types::Timespec::new().sec(0).nsec(10_000_000);
    let mut event_buf: Box<u64> = Box::new(0);
    let event_ptr = &mut *event_buf as *mut u64 as *mut u8;

    let (submitter, mut sq, mut cq) = ring.split();

    // An SQE must never be silently dropped: if the queue is full, flush it and retry.
    macro_rules! push {
        ($e:expr) => {{
            let e = $e;
            loop {
                unsafe {
                    if sq.push(&e).is_ok() {
                        break;
                    }
                }
                sq.sync();
                submitter.submit()?;
                sq.sync();
            }
        }};
    }

    let accept = opcode::AcceptMulti::new(types::Fd(lfd))
        .build()
        .user_data(ud(TAG_ACCEPT, 0, 0));
    let read_event = opcode::Read::new(types::Fd(cfg.efd), event_ptr, 8)
        .build()
        .user_data(ud(TAG_EVENT, 0, 0));
    push!(accept.clone());
    push!(read_event.clone());

    loop {
        sq.sync();
        match submitter.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {}
            Err(e) => return Err(e),
        }
        cq.sync();

        while let Some(cqe) = cq.next() {
            let u = cqe.user_data();
            let res = cqe.result();

            match ud_tag(u) {
                TAG_RETRY => {
                    push!(accept.clone());
                    continue;
                }

                TAG_EVENT => {
                    // The counter is level-persistent, so a poke that arrives with no read
                    // armed is not lost, and a spurious wake just finds the queue empty.
                    while let Ok(reply) = cfg.replies.try_recv() {
                        let fd = reply.fd as usize;
                        if fd >= MAX_CONNS {
                            continue;
                        }
                        let c = &mut conns[fd];
                        if !c.awaiting || c.generation != reply.generation {
                            // The connection died while the query was running and the slot has
                            // moved on. Dropping the answer is the whole point of the
                            // generation; the cache fill it would have done is lost with it.
                            stale_replies += 1;
                            continue;
                        }
                        c.awaiting = false;
                        let response = finish(reply.cont, reply.result, &cfg.cache, &mut cache_q);
                        if c.inflight {
                            // A send is still draining; stage this behind it.
                            c.reply_out = Some(response);
                        } else {
                            stage(c, response);
                            c.phase = Phase::Send;
                            c.inflight = true;
                            push!(send_op(c, fd as u32));
                        }
                    }
                    push!(read_event.clone());
                    continue;
                }

                TAG_ACCEPT => {
                    let mut rearm = !cqueue::more(cqe.flags());
                    if res >= 0 {
                        let fd = res as usize;
                        if fd < MAX_CONNS && conns[fd].phase == Phase::Free {
                            let on: libc::c_int = 1;
                            unsafe {
                                libc::setsockopt(
                                    res,
                                    libc::IPPROTO_TCP,
                                    libc::TCP_NODELAY,
                                    &on as *const _ as *const libc::c_void,
                                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                                );
                            }
                            let c = &mut conns[fd];
                            c.phase = Phase::Recv;
                            c.inflight = true;
                            c.awaiting = false;
                            c.zero_sends = 0;
                            c.buf.clear();
                            reset_out(c);
                            push!(recv_op(c, fd as u32));
                        } else {
                            if fd < MAX_CONNS {
                                eprintln!("accept: fd {fd} still {:?}", conns[fd].phase);
                            }
                            unsafe { libc::close(res) };
                        }
                    } else {
                        let err = -res;
                        // Out of descriptors or memory ends the multishot. Re-arming straight
                        // away spins; silently ceasing to accept is the worst failure mode
                        // there is, so back off and try again.
                        if err == libc::EMFILE || err == libc::ENFILE || err == libc::ENOMEM {
                            eprintln!(
                                "accept: {}, backing off",
                                std::io::Error::from_raw_os_error(err)
                            );
                            push!(opcode::Timeout::new(&retry_after)
                                .build()
                                .user_data(ud(TAG_RETRY, 0, 0)));
                            rearm = false;
                        } else if err != libc::ECANCELED && err != libc::ECONNABORTED {
                            eprintln!("accept: {}", std::io::Error::from_raw_os_error(err));
                        }
                    }
                    if rearm {
                        push!(accept.clone());
                    }
                    continue;
                }

                _ => {}
            }

            let tag = ud_tag(u);
            let fd = ud_fd(u) as usize;
            if fd >= MAX_CONNS || ud_gen(u) != conns[fd].generation & 0x00ff_ffff {
                continue;
            }
            conns[fd].inflight = false;
            let mut fail = false;

            if tag == TAG_RECV {
                if res > 0 {
                    let c = &mut conns[fd];
                    let len = c.buf.len();
                    // The kernel wrote into the spare capacity; publish it.
                    unsafe { c.buf.set_len(len + res as usize) };
                    fail = matches!(
                        pump(c, fd as u32, cfg.index, &cfg.cache, &mut cache_q, &cfg.db),
                        Pumped::Bad
                    );
                } else {
                    // res == 0 is a clean EOF; a partial request is dropped without a reply,
                    // matching what the compio server does.
                    fail = true;
                    report(res, "recv");
                }
            } else {
                debug_assert_eq!(tag, TAG_SEND);
                let c = &mut conns[fd];
                if res > 0 {
                    // A short write is normal: resume where the kernel stopped rather than
                    // truncating the response.
                    c.out_sent += res as usize;
                    c.zero_sends = 0;
                } else if res == 0 {
                    c.zero_sends += 1;
                    fail = c.zero_sends >= MAX_ZERO_SENDS;
                } else {
                    fail = true;
                    report(res, "send");
                }

                if !fail && c.out_sent >= c.out_len {
                    reset_out(c);
                    if let Some(response) = c.reply_out.take() {
                        stage(c, response);
                    } else if !c.awaiting {
                        // The send may have cleared the way for another buffered request.
                        fail = matches!(
                            pump(c, fd as u32, cfg.index, &cfg.cache, &mut cache_q, &cfg.db),
                            Pumped::Bad
                        );
                    }
                }
            }

            let c = &mut conns[fd];
            if fail {
                close_now(c, fd);
            } else if c.out_sent < c.out_len {
                c.phase = Phase::Send;
                c.inflight = true;
                push!(send_op(c, fd as u32));
            } else if c.awaiting {
                // Parked on the database: nothing armed, the reply will restart it.
                c.phase = Phase::Awaiting;
            } else {
                c.phase = Phase::Recv;
                c.inflight = true;
                push!(recv_op(c, fd as u32));
            }
        }

        run_cache_tasks(&cfg.cache, &mut cache_q);

        if stale_replies != 0 {
            // Expected under connection churn, not a bug; worth seeing if it ever spikes.
            stale_replies = 0;
        }
    }
}
