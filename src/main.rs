use std::error::Error;
use std::io::{self, Read, Write};
use std::ops::Shr;
use std::sync::atomic::{AtomicPtr, Ordering};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use std::sync::Arc;

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq, Hash)]
/// Example data:
///
/// ```http
/// HTTP/1.1 200 OK\r\n
/// Content-Type: application/json\r\n
/// Content-Length: 137\r\n
/// \r\n
/// {"id":"550e8400-e29b-41d4-a716-446655440000","title":"Buy milk","completed":false,"createdAt":"2026-09-08T12:00:00Z","updatedAt":"2026-09-08T12:00:00Z"}
/// ```
///
/// But! Content-Length: from 130B to 161B
struct TodoJ(bytes::Bytes); // yeah.

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq, Hash)]
struct Uuid(u128);

impl PartialEq<[u8; 36]> for TodoJ {
    fn eq(&self, other: &[u8; 36]) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<TodoJ> for [u8; 36] {
    fn eq(&self, other: &TodoJ) -> bool {
        self == other.0.as_ref()
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key(u64);

impl From<Uuid> for Key {
    #[inline(always)]
    fn from(uuid: Uuid) -> Self {
        Self((uuid.0 as u64) ^ (uuid.0 >> 64) as u64)
    }
}

static BIG_CACHE: AtomicPtr<TodoJ> = AtomicPtr::new(std::ptr::null_mut());

const PREFIX: &[u8; 11] = b"GET /todos/";
type BUF<'a> = [u8; 36];
const SUFFIX: &[u8; 11] = b" HTTP/1.1\r\n";

fn write_to_stream(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    let mut bytes_written = 0;

    // Loop until the entire slice of data is sent
    while bytes_written < data.len() {
        match stream.write(&data[bytes_written..]) {
            // Success: some bytes were written
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write any bytes",
                ));
            }
            Ok(n) => {
                bytes_written += n;
            }
            // Error: The system is not ready to take more data right now
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                break;
                // we are loooooooosseeeers! hell yeah
            }
            // Error: The system call was interrupted; try again immediately
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

fn handle_request(ch: *mut TcpStream) {
    let mut data = [0u8; 58];

    unsafe { ch.read() }.read_exact(&mut data).ok();
    if &data[0..11] != PREFIX || &data[47..58] != SUFFIX {
        return drop(ch);
    }

    let uuid = &data[11..47];

    let ptr = BIG_CACHE.load(Ordering::Relaxed);

    if !ptr.is_null() {
        let todo = unsafe { &*ptr };

        let toodoo = todo.0.as_ref();

        if toodoo == uuid {
            unsafe {
                if let Some(ch) = ch.as_mut() {
                    write_to_stream(ch, toodoo).ok();
                    return;
                }
            }
        }
    }

    return ();
}

// Some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

fn main() -> Result<(), Box<dyn Error>> {
    // Create a poll instance.
    let mut poll = Poll::new()?;
    // Create storage for events.
    let mut events = Events::with_capacity(128);

    // Setup the server socket.
    let addr = "127.0.0.1:13265".parse()?;
    let mut server = TcpListener::bind(addr)?;
    // Start listening for incoming connections.
    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    // Start an event loop.
    loop {
        // Poll Mio for events, blocking until we get an event.
        poll.poll(&mut events, None)?;

        // Process each event.
        for event in events.iter() {
            // We can use the token we previously provided to `register` to
            // determine for which socket the event is.
            match event.token() {
                SERVER => {
                    // If this is an event for the server, it means a connection
                    // is ready to be accepted.
                    //
                    // Accept the connection and drop it immediately. This will
                    // close the socket and notify the client of the EOF.
                    let connection = server.accept();
                    drop(connection);
                }

                // We don't expect any events with tokens other than those we provided.
                _ => unreachable!(),
            }
        }
    }
}

compile_error!(
    "i have mental issues and can't make this code work (and cant write in english, lol (sorry)).
    maybe later i finally rewrite this...

    small explain: i KNOW how make this works, but...,
    some bad things hapenned in my life. sorry.
    i cant do this right now..
    "
);
