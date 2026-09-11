//! S1: the bare io_uring event loop that will replace the compio server.
//!
//! No `Future`, no waker, no executor. One ring per worker, `SO_REUSEPORT`, and a flat
//! `CQE -> match connection state -> build response -> SQE` path. Connection state lives in a
//! slot indexed by fd; `user_data` carries (tag, generation, fd) so nothing is allocated per
//! operation.
//!
//! Ring flags and mechanisms were chosen by measurement, not by reputation (see README):
//! `SINGLE_ISSUER | DEFER_TASKRUN` was worth 2x, while direct descriptors measured *worse* and
//! multishot recv with a provided buffer ring was noise. So this uses plain fds and single-shot
//! recv, which also avoids the ENOBUFS and IOU_PBUF_RING_INC traps entirely.
//!
//! This step answers with a fixed response; the parser, cache tiers and Postgres path land in
//! S2-S4. What it does implement in full is the part that is easy to get quietly wrong:
//! request framing across reads, several requests in one read, resuming a partial send, and a
//! connection lifecycle that cannot hand a stale completion to a reused fd.
//!
//! Two invariants carry most of the correctness:
//!
//! * **At most one operation in flight per connection**, strictly alternating recv -> send.
//!   io_uring gives no ordering guarantee between two independently submitted SQEs, so two
//!   concurrent sends on one socket would interleave and corrupt the byte stream. Alternating
//!   also bounds buffering to one response per connection, which is the backpressure answer
//!   for a peer that stops reading.
//! * **The fd is closed synchronously, and only with nothing in flight.** Closing through an
//!   SQE frees the fd inside the operation, so `accept` can hand the same number out before the
//!   close completion is posted -- and that late completion would then clobber the live
//!   connection that inherited the slot. A plain `close(2)` at a point where the ring holds
//!   nothing removes the race by construction rather than by checking for it.

use io_uring::{cqueue, opcode, squeue, types, IoUring};

/// The connection table is indexed by fd and never grows, so an in-flight recv can never have
/// its slot relocated. An fd at or beyond this is refused rather than accepted.
const MAX_CONNS: usize = 16384;
/// Kept well above a request so a read rarely has to be repeated, well below a page-thrashing
/// size so idle connections stay cheap.
const RECV_CHUNK: usize = 4096;
/// A request that never completes is an attack, not a client.
const MAX_REQ: usize = 64 * 1024;
/// A send that reports no progress this many times in a row is not going to make any.
const MAX_ZERO_SENDS: u8 = 3;

const TAG_ACCEPT: u64 = 0;
const TAG_RECV: u64 = 1;
const TAG_SEND: u64 = 2;
/// Backoff timer: re-arm accept after the listener ran out of descriptors or memory.
const TAG_RETRY: u64 = 3;

const RESP: &[u8] = concat!(
    "HTTP/1.1 200 OK\r\n",
    "Content-Type: application/json\r\n",
    "Content-Length: 163\r\n\r\n",
    "{\"id\":\"6b802b22-9024-4f90-bfc1-9d994930538c\",\"title\":\"bench\",\"completed\":false,",
    "\"createdAt\":\"2026-09-11T09:07:01.965019Z\",\"updatedAt\":\"2026-09-11T09:07:01.965019Z\"}"
)
.as_bytes();

/// `user_data` is the whole per-operation bookkeeping: 8 bits of tag, 24 bits of generation,
/// 32 bits of fd. The generation guards the slot against a completion issued by a previous
/// occupant of the same fd number -- with synchronous close it should never fire, so it is
/// checked and counted rather than assumed, and a latent bug shows up as a statistic instead
/// of as corruption.
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
}

struct Conn {
    phase: Phase,
    /// Bumped on every close, so a completion from a previous occupant is recognisable.
    generation: u32,
    /// True while the ring owns an operation for this connection.
    inflight: bool,
    /// Bytes received and not yet consumed by a complete request.
    buf: Vec<u8>,
    /// The response still to be written. Points either at a static buffer or into `own`.
    out_ptr: *const u8,
    out_len: usize,
    out_sent: usize,
    zero_sends: u8,
    /// Backing store when several pipelined responses were concatenated into one send.
    own: Vec<u8>,
}

impl Conn {
    const fn new() -> Self {
        Conn {
            phase: Phase::Free,
            generation: 0,
            inflight: false,
            buf: Vec::new(),
            out_ptr: std::ptr::null(),
            out_len: 0,
            out_sent: 0,
            zero_sends: 0,
            own: Vec::new(),
        }
    }
}

/// Length of the next complete request in `buf`, or `None` if more bytes are needed.
///
/// A request is the header block plus whatever `Content-Length` says follows it. Ignoring a
/// body would leave it in the buffer and desync the next request on the connection.
fn frame(buf: &[u8]) -> Option<usize> {
    let end = memchr::memmem::find(buf, b"\r\n\r\n")? + 4;
    let body = content_length(&buf[..end - 2]);
    let total = end + body;
    (buf.len() >= total).then_some(total)
}

/// Walks header lines rather than sliding a compare over the whole block.
fn content_length(headers: &[u8]) -> usize {
    const NAME: &[u8] = b"content-length:";
    let mut rest = headers;
    loop {
        let end = memchr::memchr(b'\n', rest).unwrap_or(rest.len());
        let line = &rest[..end];
        if line.len() > NAME.len() && line[..NAME.len()].eq_ignore_ascii_case(NAME) {
            return std::str::from_utf8(&line[NAME.len()..])
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
        }
        if end >= rest.len() {
            return 0;
        }
        rest = &rest[end + 1..];
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

fn main() {
    // Writing to a peer that has gone away must not take the process down. compio sets
    // MSG_NOSIGNAL on every send; this loop does too, and ignoring the signal is the second
    // layer -- with `panic = "abort"` a stray SIGPIPE would kill a whole worker.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    raise_nofile();

    let addr: std::net::SocketAddrV4 = std::env::var("TODOAPP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:13267".into())
        .parse()
        .expect("TODOAPP_ADDR must be host:port");

    let cpus = cpus_in_mask();
    let workers = env_num("TODOAPP_WORKERS", cpus.len() as u32).max(1);
    eprintln!("uring on {addr} across {workers} worker(s)");

    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let cpus = cpus.clone();
            std::thread::spawn(move || {
                if env_flag("TODOAPP_PIN") {
                    pin_to(cpus[i as usize % cpus.len()]);
                }
                worker(addr).expect("worker failed");
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
}

fn worker(addr: std::net::SocketAddrV4) -> std::io::Result<()> {
    let mut b: io_uring::Builder<squeue::Entry, cqueue::Entry> = IoUring::builder();
    b.setup_cqsize(8192);
    let want_defer = !std::env::var("TODOAPP_DEFER")
        .map(|v| v == "0")
        .unwrap_or(false);
    if want_defer {
        b.setup_single_issuer();
        b.setup_defer_taskrun();
    }
    let mut ring = b.build(2048)?;

    let lfd = make_listener(addr)?;
    let mut conns: Vec<Conn> = (0..MAX_CONNS).map(|_| Conn::new()).collect();
    let mut stale_cqes: u64 = 0;

    // Lives for as long as the loop, so the kernel's pointer to it stays valid.
    let retry_after = types::Timespec::new().sec(0).nsec(10_000_000);

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
    push!(accept.clone());

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
            let tag = ud_tag(u);

            if tag == TAG_RETRY {
                push!(accept.clone());
                continue;
            }

            if tag == TAG_ACCEPT {
                let mut rearm_now = !cqueue::more(cqe.flags());
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
                        c.zero_sends = 0;
                        c.out_len = 0;
                        c.out_sent = 0;
                        c.buf.clear();
                        c.own.clear();
                        push!(recv_op(c, fd as u32));
                    } else {
                        // Beyond the table, or a slot that is somehow still occupied. Refusing
                        // the connection is the only safe answer; taking it would mean writing
                        // into another connection's state.
                        if fd < MAX_CONNS {
                            eprintln!("accept: fd {fd} still {:?}", conns[fd].phase);
                        }
                        unsafe { libc::close(res) };
                    }
                } else {
                    let err = -res;
                    // Out of descriptors or memory ends the multishot. Re-arming straight away
                    // spins; backing off and retrying keeps the listener alive, and silently
                    // ceasing to accept is the worst failure mode there is.
                    if err == libc::EMFILE || err == libc::ENFILE || err == libc::ENOMEM {
                        eprintln!(
                            "accept: {}, backing off",
                            std::io::Error::from_raw_os_error(err)
                        );
                        push!(opcode::Timeout::new(&retry_after)
                            .build()
                            .user_data(ud(TAG_RETRY, 0, 0)));
                        rearm_now = false;
                    } else if err != libc::ECANCELED && err != libc::ECONNABORTED {
                        eprintln!("accept: {}", std::io::Error::from_raw_os_error(err));
                    }
                }
                if rearm_now {
                    push!(accept.clone());
                }
                continue;
            }

            let fd = ud_fd(u) as usize;
            if fd >= MAX_CONNS {
                continue;
            }
            if ud_gen(u) != conns[fd].generation & 0x00ff_ffff {
                // Should be unreachable now that closes are synchronous; counted rather than
                // trusted, so a regression shows up here instead of as a corrupted stream.
                stale_cqes += 1;
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
                    fail = process(c).is_err();
                } else {
                    // res == 0 is a clean EOF. A partial request at that point is dropped
                    // without a response, matching what the compio server does.
                    fail = true;
                    report(res, "recv");
                }
            } else {
                debug_assert_eq!(tag, TAG_SEND);
                let c = &mut conns[fd];
                if res > 0 {
                    // A short write is normal, not an error: resume from where the kernel
                    // stopped rather than truncating the response.
                    c.out_sent += res as usize;
                    c.zero_sends = 0;
                } else if res == 0 {
                    c.zero_sends += 1;
                    fail = c.zero_sends >= MAX_ZERO_SENDS;
                } else {
                    fail = true;
                    report(res, "send");
                }
            }

            let c = &mut conns[fd];
            if fail {
                close_now(c, fd);
                continue;
            }

            if c.out_sent < c.out_len {
                c.phase = Phase::Send;
                c.inflight = true;
                push!(send_op(c, fd as u32));
                continue;
            }

            c.out_len = 0;
            c.out_sent = 0;
            // A completed send may have left another complete request behind it in the buffer;
            // answer that before reading again.
            if c.phase == Phase::Send && process(c).is_err() {
                close_now(c, fd);
                continue;
            }
            if c.out_len > 0 {
                c.phase = Phase::Send;
                c.inflight = true;
                push!(send_op(c, fd as u32));
            } else {
                c.phase = Phase::Recv;
                c.inflight = true;
                push!(recv_op(c, fd as u32));
            }
        }

        if stale_cqes != 0 {
            eprintln!("BUG: {stale_cqes} stale completions");
            stale_cqes = 0;
        }
    }
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

/// Consume every complete request in the buffer and stage the responses as one write.
///
/// `Err` means the connection is unusable: a request that will never complete.
fn process(c: &mut Conn) -> Result<(), ()> {
    let mut consumed = 0;
    let mut n = 0usize;
    while let Some(len) = frame(&c.buf[consumed..]) {
        consumed += len;
        n += 1;
    }

    if consumed == 0 {
        return if c.buf.len() > MAX_REQ { Err(()) } else { Ok(()) };
    }
    if consumed == c.buf.len() {
        c.buf.clear();
    } else {
        c.buf.drain(..consumed);
    }

    if n == 1 {
        // The common case stays zero-copy: the kernel is handed the response where it lies.
        c.out_ptr = RESP.as_ptr();
        c.out_len = RESP.len();
    } else {
        c.own.clear();
        c.own.reserve(n * RESP.len());
        for _ in 0..n {
            c.own.extend_from_slice(RESP);
        }
        c.out_ptr = c.own.as_ptr();
        c.out_len = c.own.len();
    }
    c.out_sent = 0;
    Ok(())
}

/// Close the fd and free the slot in the same instruction stream.
///
/// Only legal with nothing in flight, which strict recv/send alternation guarantees: by the
/// time any completion is being handled, the ring holds nothing else for this connection. That
/// is what makes it safe for `accept` to hand the fd straight back out.
fn close_now(c: &mut Conn, fd: usize) {
    debug_assert!(!c.inflight, "closing fd {fd} with an operation in flight");
    unsafe { libc::close(fd as i32) };
    c.generation = c.generation.wrapping_add(1);
    c.phase = Phase::Free;
    c.out_len = 0;
    c.out_sent = 0;
    c.zero_sends = 0;
    c.buf.clear();
    c.own.clear();
}

#[inline]
fn recv_op(c: &mut Conn, fd: u32) -> squeue::Entry {
    let len = c.buf.len();
    // Growing here is safe only because nothing is in flight against the buffer. A recv armed
    // with zero spare capacity would return 0 and be indistinguishable from EOF.
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
