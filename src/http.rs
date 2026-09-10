use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Patch,
    Delete,
    Other,
}

/// A fully parsed request line + headers. The body (if any) is read
/// separately once `content_length` is known.
pub struct Head {
    pub method: Method,
    pub path: String,
    pub content_length: usize,
    /// Bytes the head occupied in the source buffer, so the caller can drain
    /// exactly that much and keep the rest (body, or the next pipelined
    /// request) around.
    pub head_len: usize,
}

/// Tries to parse a request head out of `buf`. `Ok(None)` means the head
/// isn't fully buffered yet and the caller should read more and retry.
pub fn try_parse(buf: &[u8]) -> Result<Option<Head>, httparse::Error> {
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

            Ok(Some(Head {
                method,
                path,
                content_length,
                head_len,
            }))
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

/// Builds a `Connection: keep-alive` response carrying a JSON body.
pub fn json_response(code: u16, body: &[u8], extra_headers: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(160 + body.len());
    out.extend_from_slice(status_line(code).as_bytes());
    out.extend_from_slice(b"Content-Type: application/json\r\n");
    out.extend_from_slice(b"Connection: keep-alive\r\n");
    for (name, value) in extra_headers {
        write!(out, "{name}: {value}\r\n").unwrap();
    }
    write!(out, "Content-Length: {}\r\n\r\n", body.len()).unwrap();
    out.extend_from_slice(body);
    out
}

/// Builds a bodyless keep-alive response (e.g. `204 No Content`).
pub fn empty_response(code: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(status_line(code).as_bytes());
    out.extend_from_slice(b"Connection: keep-alive\r\n");
    out.extend_from_slice(b"Content-Length: 0\r\n\r\n");
    out
}
