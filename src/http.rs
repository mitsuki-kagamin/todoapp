#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Patch,
    Delete,
    Other,
}

/// A parsed request head. `FastGetTodo` is the one shape the hot loop
/// actually takes (`GET /todos/<uuid> HTTP/1.1\r\n...\r\n\r\n`) and skips
/// httparse entirely for it; everything else goes through `General`, which
/// is the same httparse-based parse as before.
pub enum Head {
    FastGetTodo { id: [u8; 36], head_len: usize },
    General(GeneralHead),
}

pub struct GeneralHead {
    pub method: Method,
    pub path: String,
    pub content_length: usize,
    pub head_len: usize,
}

impl Head {
    pub fn head_len(&self) -> usize {
        match self {
            Head::FastGetTodo { head_len, .. } => *head_len,
            Head::General(g) => g.head_len,
        }
    }

    /// A `FastGetTodo` never reaches this state with a body: the fast path
    /// bails out to the general parser the moment it sees a Content-Length
    /// header at all (see `header_block_has_content_length`), so `0` here is
    /// never a guess - it's the only way this variant gets constructed.
    pub fn content_length(&self) -> usize {
        match self {
            Head::FastGetTodo { .. } => 0,
            Head::General(g) => g.content_length,
        }
    }
}

const FAST_PREFIX: &[u8] = b"GET /todos/";
const FAST_SUFFIX: &[u8] = b" HTTP/1.1\r\n";
const FAST_ID_LEN: usize = 36;
const FAST_LINE_LEN: usize = FAST_PREFIX.len() + FAST_ID_LEN + FAST_SUFFIX.len();

enum FastResult {
    Matched { id: [u8; FAST_ID_LEN], head_len: usize },
    NeedMore,
    NotThisShape,
}

/// Checks for the literal `GET /todos/<36 bytes> HTTP/1.1\r\n` request line
/// and, if present, just enough of the header block to know it's safe to
/// skip parsing it: no Content-Length means no body means nothing to lose by
/// not reading the headers at all beyond finding where they end.
fn try_fast_get_todo(buf: &[u8]) -> FastResult {
    if buf.len() < FAST_PREFIX.len() {
        return FastResult::NeedMore;
    }
    if &buf[..FAST_PREFIX.len()] != FAST_PREFIX {
        return FastResult::NotThisShape;
    }
    if buf.len() < FAST_LINE_LEN {
        return FastResult::NeedMore;
    }
    if &buf[FAST_PREFIX.len() + FAST_ID_LEN..FAST_LINE_LEN] != FAST_SUFFIX {
        return FastResult::NotThisShape;
    }

    let mut id = [0u8; FAST_ID_LEN];
    id.copy_from_slice(&buf[FAST_PREFIX.len()..FAST_PREFIX.len() + FAST_ID_LEN]);

    match find_double_crlf(&buf[FAST_LINE_LEN..]) {
        Some(rel) => {
            let headers = &buf[FAST_LINE_LEN..FAST_LINE_LEN + rel];
            if header_block_has_content_length(headers) {
                // A GET with a body-bearing header is unusual enough that
                // it's not worth optimizing for - and silently ignoring the
                // header would desync the next pipelined request off
                // whatever body bytes follow it. Let the general parser
                // handle it correctly instead.
                FastResult::NotThisShape
            } else {
                FastResult::Matched {
                    id,
                    head_len: FAST_LINE_LEN + rel + 4,
                }
            }
        }
        None => FastResult::NeedMore,
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn header_block_has_content_length(headers: &[u8]) -> bool {
    const NAME: &[u8] = b"content-length";
    headers.windows(NAME.len()).any(|w| w.eq_ignore_ascii_case(NAME))
}

/// Tries to parse a request head out of `buf`. `Ok(None)` means the head
/// isn't fully buffered yet and the caller should read more and retry.
pub fn try_parse(buf: &[u8]) -> Result<Option<Head>, httparse::Error> {
    match try_fast_get_todo(buf) {
        FastResult::Matched { id, head_len } => return Ok(Some(Head::FastGetTodo { id, head_len })),
        FastResult::NeedMore => return Ok(None),
        FastResult::NotThisShape => {}
    }

    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);

    match req.parse(buf)? {
        httparse::Status::Complete(head_len) => {
            let method = match req.method.unwrap_or("") {
                "GET" => Method::Get,
                "POST" => Method::Post,
                "PATCH" => Method::Patch,
                "DELETE" => Method::Delete,
                _ => Method::Other,
            };

            let path = req.path.unwrap_or("/").to_string();

            let content_length = req
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                .and_then(|h| std::str::from_utf8(h.value).ok())
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);

            Ok(Some(Head::General(GeneralHead {
                method,
                path,
                content_length,
                head_len,
            })))
        }
        httparse::Status::Partial => Ok(None),
    }
}

fn status_line(code: u16) -> &'static str {
    match code {
        200 => "HTTP/1.1 200 OK\r\n",
        201 => "HTTP/1.1 201 Created\r\n",
        204 => "HTTP/1.1 204 No Content\r\n",
        400 => "HTTP/1.1 400 Bad Request\r\n",
        404 => "HTTP/1.1 404 Not Found\r\n",
        _ => "HTTP/1.1 500 Internal Server Error\r\n",
    }
}

/// Writes a `Connection: keep-alive` response carrying a JSON body into
/// `out`. Takes the buffer by reference instead of allocating one - callers
/// reuse the same `Vec` for every response on a connection (see
/// `Conn::write_response` in `server.rs`), so the only allocation left on
/// the hot path is growing that buffer the first few times it's used.
pub fn write_json_response(out: &mut Vec<u8>, code: u16, body: &[u8], extra_headers: &[(&str, &str)]) {
    out.extend_from_slice(status_line(code).as_bytes());
    out.extend_from_slice(b"Content-Type: application/json\r\n");
    out.extend_from_slice(b"Connection: keep-alive\r\n");
    for (name, value) in extra_headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Content-Length: ");
    // itoa over `write!`: no `core::fmt` machinery, no formatting flags to
    // interpret, just digits straight into the buffer.
    out.extend_from_slice(itoa::Buffer::new().format(body.len()).as_bytes());
    out.extend_from_slice(b"\r\n\r\n");
    out.extend_from_slice(body);
}

/// Writes a bodyless keep-alive response (e.g. `204 No Content`) into `out`.
pub fn write_empty_response(out: &mut Vec<u8>, code: u16) {
    out.extend_from_slice(status_line(code).as_bytes());
    out.extend_from_slice(b"Connection: keep-alive\r\n");
    out.extend_from_slice(b"Content-Length: 0\r\n\r\n");
}
