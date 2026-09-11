some random things 
current speed: 527k RPS (rust)
and 520 (idk) -- 528k RPS ([kotlin](https://github.com/qmained/todo-project)) 
(I hope I win!!!)

---

status: alive again, and off compio. One file, a hand-rolled io_uring event loop -- no `Future`,
no waker, no executor -- thread-per-core via `SO_REUSEPORT`, implementing the L1 -> L2 -> DB
pipeline from `api_reference.md` as real dispatched tasks and continuations, not flattened into
`.await`s. Builds on **stable** now; nightly was only ever needed because compio was.

DB tier is real Postgres (`postgres` + `r2d2`, schema auto-migrated on boot) behind a blocking
thread pool, with replies returning to the owning worker through an eventfd. Set `POSTGRES_URL`
(env or `.env`). Listens on `127.0.0.1:8080` by default, which is what `test.js` expects; override
with `TODOAPP_ADDR`.

`GET /todos/{id}` is specialised end to end: the request line is recognised by memcmp against the
literal `GET /todos/<36 bytes> HTTP/1.1\r\n`, the id is compared as raw ASCII with no hex decode,
and both cache tiers hold the **entire precomputed HTTP response**. On top of that each worker
keeps a one-entry mirror of the last request it answered, so a byte-identical repeat skips the
parse and both cache tiers entirely -- guarded by a global version counter bumped before any
mutating response goes out.

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

### bespoke io_uring loop

Replaces the runtime, not the ring: connection state in a slot indexed by fd, `user_data`
carrying (tag, generation, fd), so nothing is allocated per operation. compio spent 2
`malloc`/`free` pairs, ~8 locked RMWs, 4 SipHashes of a pointer and ~3 task polls on every
request to get to the same place. `send_task(Task, continuation)` is finally what it wants to
be: a continuation is a plain enum value parked in the connection's slot, dispatching one is
pushing a small value onto a queue.

Measured one server at a time, 6 randomized rounds, medians:

```
              user CPU/req          kernel   total    RPS
compio        1.11 us (1.07-1.19)   7.32 us  8.43 us  206k
bespoke loop  0.50 us (0.42-0.55)   6.87 us  7.34 us  208k
```

and the per-worker speculation mirror on top of that, its own 6 randomized rounds:

```
              user CPU/req
mirror off    0.54 us (0.51-0.58)
mirror on     0.42 us (0.37-0.42)
```

**Userspace is the result that survives the noise** -- none of those ranges overlap.
**Neither RPS difference is established**: the distributions overlap almost completely, and this
host cannot resolve ~10% of throughput. Kernel time is unchanged throughout, which is what should
happen: same ring mechanics, different userspace.

Pipelining, same stand, shows what the single-send batching does -- every complete request in a
read is answered in one `send`:

```
depth  1    201k RPS   6.79 us/req   2.00 softirq/req
depth  2    826k        1.69          0.50
depth  4    2.9M        0.45          0.13
depth  8    9.8M        0.13          0.03
depth 16   32.0M        0.04          0.01
```

Note on absolute numbers: this container moved to a different host CPU partway through the work
(a `target-cpu=native` binary SIGILLed, which is how it was noticed), and the host is noisy.
Figures are only comparable within a block measured together. Every table here is.

the remaining per-request cost is the TCP/loopback packet path, which is ~2 softirqs and ~1.6 us
per io_uring op regardless of payload size up to ~10 KB. userspace is now 16% of the total rather
than 10%, so it is worth more than it was.
