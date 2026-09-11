some random things 
current speed: 527k RPS (rust)
and 520 (idk) -- 528k RPS ([kotlin](https://github.com/qmained/todo-project)) 
(I hope I win!!!)

---

status: alive again. one file, on [compio](https://github.com/compio-rs/compio) (io_uring, thread-per-core via
`SO_REUSEPORT`), implementing the L1 (hot slot) -> L2 (warm map) -> DB pipeline from `api_reference.md` as
actual dispatched tasks/continuations (`Db::send_task` / `Cache::send_task`), not flattened into `.await`s.
needs nightly (`compio-executor` uses `cfg_select`, pinned via `rust-toolchain.toml`).
RPS numbers above are stale, haven't re-benched yet.

`GET /todos/{id}` is specialised end to end: the request line is recognised by memcmp against the literal
`GET /todos/<36 bytes> HTTP/1.1\r\n` (httparse only handles what doesn't match), the id is compared as raw
ASCII with no hex decode, and both cache tiers hold the **entire precomputed HTTP response** - so a hit is one
atomic load, one 36-byte compare and one write. Every error response is precomputed too.

DB tier is real Postgres (`postgres` + `r2d2`, schema auto-migrated on boot). Set `POSTGRES_URL`
(env or `.env`, e.g. `postgresql://user:pass@localhost:5432/todoapp`) before running.

### where the time actually goes

measured on the hot path (4 shared cores, wrk `-t4 -c100`, ~245k RPS):

```
total    ~7.8 us CPU / request
  kernel ~7.0 us  (90%)   <- network stack, io_uring submit/complete, context switches
  user   ~0.7 us  (10%)   <- everything above: parse, cache lookup, write
```

so the userspace path is already ~10% of the cost. A/B against the previous, more general version (two
binaries, alternating wrk runs) showed **no measurable difference** in wall clock, total CPU/req or even
userspace CPU/req - all the specialisation lives inside that 0.7us. The remaining lever is kernel work per
request: fewer syscalls (multishot recv, SQPOLL) or pipelining, which amortises them (pipelined depth 64+
measured ~555k RPS on the same box vs ~165k unpipelined).
