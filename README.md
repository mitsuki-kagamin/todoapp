some random things 
current speed: 527k RPS (rust)
and 520 (idk) -- 528k RPS ([kotlin](https://github.com/qmained/todo-project)) 
(I hope I win!!!)

---

status: alive again. one file, on [compio](https://github.com/compio-rs/compio) (io_uring, thread-per-core via
`SO_REUSEPORT`), implementing the L1 (hot slot) -> L2 (warm map) -> DB pipeline from `api_reference.md` as
actual dispatched tasks/continuations (`Db::send_task` / `Cache::send_task`), not flattened into `.await`s.
needs nightly (`compio-executor` uses `cfg_select`, pinned via `rust-toolchain.toml`).
the 527k number at the top is stale (different box, all cores); see the measured table below.

`GET /todos/{id}` is specialised end to end: the request line is recognised by memcmp against the literal
`GET /todos/<36 bytes> HTTP/1.1\r\n` (httparse only handles what doesn't match), the id is compared as raw
ASCII with no hex decode, and both cache tiers hold the **entire precomputed HTTP response** - so a hit is one
atomic load, one 36-byte compare and one write. Every error response is precomputed too.

DB tier is real Postgres (`postgres` + `r2d2`, schema auto-migrated on boot). Set `POSTGRES_URL`
(env or `.env`, e.g. `postgresql://user:pass@localhost:5432/todoapp`) before running.

### where the time actually goes

measured on a controlled stand (2 workers pinned to cpu0-1, `wrk -t2 -c100` on cpu2-3, so the
load generator never competes with the server), per request:

```
                    user     kernel    total     RPS
before             0.57 us   8.94 us   9.50 us   ~199k
after DEFER_TASKRUN 0.91 us  4.72 us   5.62 us   ~316k     (+57%)
```

the whole win is one ring setup flag. the reasoning that got there, and the three things that
turned out to be worth nothing:

**what was measured first.** `io_uring_enter` was already amortised 23:1 (0.043 enters/request,
50304 enters for 1.16M requests) and the worker only slept once per ~50 requests. that looks like
"the ring is already batched, flags cannot help" - and that inference was wrong. the cost of a
completion is not paid at `io_uring_enter`, it is paid *per completion*: without `DEFER_TASKRUN`
every CQE arriving in softirq context queues task_work and IPIs the owning thread. counting
enters measures the wrong thing.

**the floor.** `src/bin/floor.rs` is a bare io_uring HTTP responder built to answer "what does the
packet path cost when userspace does nothing", with every mechanism behind its own switch. on the
same stand, kernel us/request:

```
plain fds, one recv per request            10.12
+ direct descriptors (IOSQE_FIXED_FILE)    10.40
+ multishot recv + provided buffer ring    10.49
+ DEFER_TASKRUN | SINGLE_ISSUER             4.75   <-
  and then, on top of DEFER_TASKRUN:
  direct descriptors                        5.16   (worse)
  multishot recv + provided buffer ring     4.90   (noise)
  SO_INCOMING_CPU + worker pinning          5.13   (noise; on loopback softirq runs on the
                                                    client's core, so steering cannot help here)
  SQPOLL                                    n/a    (kernel refuses it with DEFER_TASKRUN)
```

so direct descriptors, provided buffer rings and multishot recv - the whole "advanced io_uring"
toolbox - bought nothing here, and a hand-rolled ring loop was *slower* than compio until the flag
went on. after it, compio sits exactly on the bare-ring floor (4.72 vs 4.75), which means there is
nothing left to win on the io_uring side.

**cross-check.** pipelining depth sweep, same stand, RPS before -> after the flag:

```
depth  1    179k -> 292k   (+63%)
depth  2    612k -> 939k   (+53%)
depth  4   2.12M -> 2.14M  (+1%)
depth 16   8.23M -> 8.15M  (0%)
```

exactly the expected shape: at depth >= 4 the ring never sleeps (`voluntary_ctxt_switches/req` is
0), so there are no deferrable completions and the flag has nothing to give. it pays precisely
where completions arrive sparsely.

the remaining per-request cost is the TCP/loopback packet path, which is ~2 softirqs and ~1.6 us
per io_uring op regardless of payload size up to ~10 KB. userspace is now 16% of the total rather
than 10%, so it is worth more than it was.
