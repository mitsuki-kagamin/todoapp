//! Ablation harness for the kernel-side cost of one HTTP request.
//!
//! This is a measurement tool, not the server: it answers "what does the packet path cost if
//! userspace does almost nothing", and lets each io_uring mechanism be switched on alone so its
//! contribution is measured instead of assumed.
//!
//! Env knobs:
//!   FLOOR_ADDR=127.0.0.1:13266
//!   FLOOR_WORKERS=<n>        default: CPUs in the affinity mask
//!   FLOOR_DIRECT=1           direct descriptors (IOSQE_FIXED_FILE) instead of real fds
//!   FLOOR_MULTISHOT=1        multishot recv + provided buffer ring instead of one recv per request
//!   FLOOR_DEFER=1            SINGLE_ISSUER | DEFER_TASKRUN
//!   FLOOR_COOP=1             COOP_TASKRUN | TASKRUN_FLAG
//!   FLOOR_SQPOLL=<idle_ms>   SQPOLL with that idle, 0 = off
//!   FLOOR_PIN=1              pin worker i to the i-th CPU of the affinity mask
//!   FLOOR_INCPU=1            SO_INCOMING_CPU on the listener, set to the worker's CPU
//!   FLOOR_PARSE=1            actually parse the request line instead of replying to any bytes
//!   FLOOR_STATS=1            dump counters to stderr every 5s

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

const MAX_CONNS: u32 = 8192;
const BUF_LEN: usize = 2048;
const BUF_COUNT: u16 = 1024;
const BGID: u16 = 1;
const MAX_BATCH: usize = 64;

const TAG_ACCEPT: u64 = 0;
const TAG_RECV: u64 = 1;
const TAG_SEND: u64 = 2;
const TAG_CLOSE: u64 = 3;
const TAG_SETOPT: u64 = 4;

/// Must outlive the SQE that points at it.
static NODELAY_ON: libc::c_int = 1;

const RESP: &[u8] = concat!(
    "HTTP/1.1 200 OK\r\n",
    "Content-Type: application/json\r\n",
    "Connection: keep-alive\r\n",
    "Content-Length: 163\r\n\r\n",
    "{\"id\":\"6b802b22-9024-4f90-bfc1-9d994930538c\",\"title\":\"bench\",\"completed\":false,",
    "\"createdAt\":\"2026-09-11T09:07:01.965019Z\",\"updatedAt\":\"2026-09-11T09:07:01.965019Z\"}"
).as_bytes();

static N_REQ: AtomicU64 = AtomicU64::new(0);
static N_SHORT_SEND: AtomicU64 = AtomicU64::new(0);
static N_ENOBUFS: AtomicU64 = AtomicU64::new(0);
static N_REARM: AtomicU64 = AtomicU64::new(0);
static N_ACCEPT: AtomicU64 = AtomicU64::new(0);
static N_SEND_ERR: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
fn ud(tag: u64, idx: u32) -> u64 {
    (tag << 32) | idx as u64
}

fn env_flag(k: &str) -> bool {
    std::env::var(k).map(|v| v == "1").unwrap_or(false)
}
fn env_num(k: &str, d: u32) -> u32 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// Provided buffer ring: one contiguous mmap for the ring entries, one for the buffers.
struct BufRing {
    ring: *mut types::BufRingEntry,
    bufs: *mut u8,
    mask: u16,
    tail: u16,
}

impl BufRing {
    unsafe fn new(sub: &io_uring::Submitter<'_>) -> std::io::Result<Self> {
        let entries = BUF_COUNT;
        let ring_sz = entries as usize * std::mem::size_of::<types::BufRingEntry>();
        let ring = libc::mmap(
            std::ptr::null_mut(), ring_sz,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_PRIVATE, -1, 0,
        );
        if ring == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        let bufs_sz = entries as usize * BUF_LEN;
        let bufs = libc::mmap(
            std::ptr::null_mut(), bufs_sz,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_PRIVATE, -1, 0,
        );
        if bufs == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        let ring = ring as *mut types::BufRingEntry;
        let bufs = bufs as *mut u8;
        sub.register_buf_ring_with_flags(ring as u64, entries, BGID, 0)?;

        for i in 0..entries {
            let e = &mut *ring.add(i as usize);
            e.set_addr(bufs.add(i as usize * BUF_LEN) as u64);
            e.set_len(BUF_LEN as u32);
            e.set_bid(i);
        }
        let me = BufRing { ring, bufs, mask: entries - 1, tail: entries };
        me.publish(entries);
        Ok(me)
    }

    #[inline(always)]
    unsafe fn publish(&self, tail: u16) {
        let p = types::BufRingEntry::tail(self.ring) as *const AtomicU16;
        (*p).store(tail, Ordering::Release);
    }

    #[inline(always)]
    unsafe fn data(&self, bid: u16, len: usize) -> &[u8] {
        std::slice::from_raw_parts(self.bufs.add(bid as usize * BUF_LEN), len)
    }

    /// Hand a consumed buffer back to the kernel. Always the whole buffer from offset 0 --
    /// this is why IOU_PBUF_RING_INC must stay off.
    #[inline(always)]
    unsafe fn recycle(&mut self, bid: u16) {
        let slot = self.tail & self.mask;
        let e = &mut *self.ring.add(slot as usize);
        e.set_addr(self.bufs.add(bid as usize * BUF_LEN) as u64);
        e.set_len(BUF_LEN as u32);
        e.set_bid(bid);
        self.tail = self.tail.wrapping_add(1);
        self.publish(self.tail);
    }
}

struct Conn {
    open: bool,
    buf: Vec<u8>, // only used when FLOOR_MULTISHOT=0
}

struct Cfg {
    direct: bool,
    multishot: bool,
    parse: bool,
}

/// Count complete requests in `b`. With FLOOR_PARSE=0 any non-empty read counts as one.
#[inline(always)]
fn count_requests(b: &[u8], cfg: &Cfg) -> usize {
    if !cfg.parse {
        return 1;
    }
    let mut n = 0;
    let mut i = 0;
    while i + 3 < b.len() {
        if b[i] == b'\r' && b[i + 1] == b'\n' && b[i + 2] == b'\r' && b[i + 3] == b'\n' {
            n += 1;
            i += 4;
        } else {
            i += 1;
        }
    }
    n
}

unsafe fn make_listener(addr: std::net::SocketAddrV4, incpu: Option<u32>) -> std::io::Result<i32> {
    let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let on: libc::c_int = 1;
    let p = &on as *const _ as *const libc::c_void;
    let sz = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, p, sz);
    libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, p, sz);
    if let Some(cpu) = incpu {
        let c = cpu as libc::c_int;
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_INCOMING_CPU,
            &c as *const _ as *const libc::c_void, sz,
        );
    }
    let mut sa: libc::sockaddr_in = std::mem::zeroed();
    sa.sin_family = libc::AF_INET as u16;
    sa.sin_port = addr.port().to_be();
    sa.sin_addr.s_addr = u32::from_ne_bytes(addr.ip().octets());
    if libc::bind(fd, &sa as *const _ as *const libc::sockaddr,
                  std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t) < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if libc::listen(fd, 4096) < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

fn cpus_in_mask() -> Vec<u32> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return (0..num_cpus_fallback()).collect();
        }
        (0..libc::CPU_SETSIZE as u32).filter(|c| libc::CPU_ISSET(*c as usize, &set)).collect()
    }
}
fn num_cpus_fallback() -> u32 {
    std::thread::available_parallelism().map_or(1, |n| n.get() as u32)
}

fn pin_to(cpu: u32) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu as usize, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn main() {
    let addr: std::net::SocketAddrV4 = std::env::var("FLOOR_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:13266".into())
        .parse()
        .expect("bad FLOOR_ADDR");

    let cpus = cpus_in_mask();
    let workers = env_num("FLOOR_WORKERS", cpus.len() as u32).max(1);
    let cfg_desc = format!(
        "workers={workers} direct={} multishot={} defer={} coop={} sqpoll={} pin={} incpu={} parse={}",
        env_flag("FLOOR_DIRECT"), env_flag("FLOOR_MULTISHOT"), env_flag("FLOOR_DEFER"),
        env_flag("FLOOR_COOP"), env_num("FLOOR_SQPOLL", 0), env_flag("FLOOR_PIN"),
        env_flag("FLOOR_INCPU"), env_flag("FLOOR_PARSE"),
    );
    eprintln!("floor on {addr} :: {cfg_desc}");

    if env_flag("FLOOR_STATS") {
        std::thread::spawn(|| loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            eprintln!(
                "req={} accept={} short_send={} enobufs={} rearm={} send_err={}",
                N_REQ.load(Ordering::Relaxed), N_ACCEPT.load(Ordering::Relaxed),
                N_SHORT_SEND.load(Ordering::Relaxed), N_ENOBUFS.load(Ordering::Relaxed),
                N_REARM.load(Ordering::Relaxed), N_SEND_ERR.load(Ordering::Relaxed),
            );
        });
    }

    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let cpus = cpus.clone();
            std::thread::spawn(move || {
                let cpu = cpus[i as usize % cpus.len()];
                if env_flag("FLOOR_PIN") {
                    pin_to(cpu);
                }
                worker(addr, cpu).expect("worker failed");
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
}

fn worker(addr: std::net::SocketAddrV4, cpu: u32) -> std::io::Result<()> {
    let cfg = Cfg {
        direct: env_flag("FLOOR_DIRECT"),
        multishot: env_flag("FLOOR_MULTISHOT"),
        parse: env_flag("FLOOR_PARSE"),
    };

    // One send per batch: a precomputed run of responses so a pipelined batch costs one op.
    let batch: &'static [u8] = Box::leak(RESP.repeat(MAX_BATCH).into_boxed_slice());

    let mut b: io_uring::Builder<squeue::Entry, cqueue::Entry> = IoUring::builder();
    b.setup_cqsize(8192);
    if env_flag("FLOOR_DEFER") {
        b.setup_single_issuer();
        b.setup_defer_taskrun();
    }
    if env_flag("FLOOR_COOP") {
        b.setup_coop_taskrun();
        b.setup_taskrun_flag();
    }
    let sqpoll = env_num("FLOOR_SQPOLL", 0);
    if sqpoll > 0 {
        b.setup_sqpoll(sqpoll);
        b.setup_sqpoll_cpu(cpu);
    }
    let mut ring = b.build(2048)?;

    let incpu = if env_flag("FLOOR_INCPU") { Some(cpu) } else { None };
    let lfd = unsafe { make_listener(addr, incpu)? };

    let mut bufring = None;
    {
        let sub = ring.submitter();
        if cfg.direct {
            sub.register_files_sparse(MAX_CONNS)?;
        }
        if cfg.multishot {
            bufring = Some(unsafe { BufRing::new(&sub)? });
        }
    }

    let mut conns: Vec<Conn> = (0..MAX_CONNS)
        .map(|_| Conn { open: false, buf: Vec::new() })
        .collect();

    let (submitter, mut sq, mut cq) = ring.split();

    // Pushing an SQE must never silently drop: if the queue is full, flush and retry.
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

    let accept = if cfg.direct {
        opcode::AcceptMulti::new(types::Fd(lfd)).allocate_file_index(true).build()
    } else {
        opcode::AcceptMulti::new(types::Fd(lfd)).build()
    }
    .user_data(ud(TAG_ACCEPT, 0));
    push!(accept.clone());

    let nodelay_on: libc::c_int = 1;

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
            let tag = u >> 32;
            let idx = u as u32;
            let res = cqe.result();

            match tag {
                TAG_ACCEPT => {
                    if res < 0 {
                        eprintln!("accept: {}", std::io::Error::from_raw_os_error(-res));
                    } else {
                        let i = res as u32;
                        N_ACCEPT.fetch_add(1, Ordering::Relaxed);
                        if !cfg.direct {
                            unsafe {
                                libc::setsockopt(
                                    res, libc::IPPROTO_TCP, libc::TCP_NODELAY,
                                    &nodelay_on as *const _ as *const libc::c_void,
                                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                                );
                            }
                        }
                        if cfg.direct {
                            push!(opcode::SetSockOpt::new(
                                types::Fixed(i),
                                libc::IPPROTO_TCP as u32,
                                libc::TCP_NODELAY as u32,
                                &NODELAY_ON as *const libc::c_int as *const libc::c_void,
                                std::mem::size_of::<libc::c_int>() as u32,
                            )
                            .build()
                            .user_data(ud(TAG_SETOPT, i)));
                        }
                        let c = &mut conns[i as usize];
                        c.open = true;
                        if !cfg.multishot && c.buf.is_empty() {
                            c.buf = vec![0u8; BUF_LEN];
                        }
                        push!(arm_recv(&cfg, i, &mut conns));
                    }
                    if !cqueue::more(cqe.flags()) {
                        push!(accept.clone());
                    }
                }

                TAG_RECV => {
                    let bid = cqueue::buffer_select(cqe.flags());

                    if res > 0 {
                        let n = res as usize;
                        let nreq = if cfg.multishot {
                            let br = bufring.as_ref().unwrap();
                            let data = unsafe { br.data(bid.unwrap(), n) };
                            count_requests(data, &cfg)
                        } else {
                            count_requests(&conns[idx as usize].buf[..n], &cfg)
                        };
                        // Buffer goes back to the kernel before the response is sent, so the
                        // pool size is decoupled from the number of live connections.
                        if let (Some(b), Some(br)) = (bid, bufring.as_mut()) {
                            unsafe { br.recycle(b) };
                        }
                        if nreq > 0 {
                            N_REQ.fetch_add(nreq as u64, Ordering::Relaxed);
                            let mut left = nreq;
                            while left > 0 {
                                let k = left.min(MAX_BATCH);
                                push!(send_op(&cfg, idx, batch.as_ptr(), k * RESP.len()));
                                left -= k;
                            }
                        }
                    } else if res == 0 {
                        if let (Some(b), Some(br)) = (bid, bufring.as_mut()) {
                            unsafe { br.recycle(b) };
                        }
                        conns[idx as usize].open = false;
                        push!(close_op(&cfg, idx));
                        continue;
                    } else {
                        let err = -res;
                        if err == libc::ENOBUFS {
                            // Kernel dropped F_MORE: the multishot is gone, re-arm it.
                            N_ENOBUFS.fetch_add(1, Ordering::Relaxed);
                        } else if err != libc::ECONNRESET && err != libc::ECANCELED {
                            eprintln!("recv: {}", std::io::Error::from_raw_os_error(err));
                        }
                        if err == libc::ECONNRESET || err == libc::ECANCELED {
                            conns[idx as usize].open = false;
                            push!(close_op(&cfg, idx));
                            continue;
                        }
                    }

                    if !cqueue::more(cqe.flags()) && conns[idx as usize].open {
                        N_REARM.fetch_add(1, Ordering::Relaxed);
                        push!(arm_recv(&cfg, idx, &mut conns));
                    }
                }

                TAG_SEND => {
                    if res < 0 {
                        N_SEND_ERR.fetch_add(1, Ordering::Relaxed);
                        if conns[idx as usize].open {
                            conns[idx as usize].open = false;
                            push!(close_op(&cfg, idx));
                        }
                    } else if (res as usize) % RESP.len() != 0 {
                        N_SHORT_SEND.fetch_add(1, Ordering::Relaxed);
                    }
                }

                TAG_CLOSE => {}
                TAG_SETOPT => {
                    if res < 0 {
                        eprintln!("setsockopt: {}", std::io::Error::from_raw_os_error(-res));
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

#[inline]
fn arm_recv(cfg: &Cfg, idx: u32, conns: &mut [Conn]) -> squeue::Entry {
    let e = if cfg.multishot {
        if cfg.direct {
            opcode::RecvMulti::new(types::Fixed(idx), BGID).build()
        } else {
            opcode::RecvMulti::new(types::Fd(idx as i32), BGID).build()
        }
    } else {
        let p = conns[idx as usize].buf.as_mut_ptr();
        if cfg.direct {
            opcode::Recv::new(types::Fixed(idx), p, BUF_LEN as u32).build()
        } else {
            opcode::Recv::new(types::Fd(idx as i32), p, BUF_LEN as u32).build()
        }
    };
    e.user_data(ud(TAG_RECV, idx))
}

#[inline]
fn send_op(cfg: &Cfg, idx: u32, p: *const u8, len: usize) -> squeue::Entry {
    let e = if cfg.direct {
        opcode::Send::new(types::Fixed(idx), p, len as u32).build()
    } else {
        opcode::Send::new(types::Fd(idx as i32), p, len as u32).build()
    };
    e.user_data(ud(TAG_SEND, idx))
}

#[inline]
fn close_op(cfg: &Cfg, idx: u32) -> squeue::Entry {
    let e = if cfg.direct {
        opcode::Close::new(types::Fixed(idx)).build()
    } else {
        opcode::Close::new(types::Fd(idx as i32)).build()
    };
    e.user_data(ud(TAG_CLOSE, idx))
}
