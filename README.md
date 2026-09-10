some random things 
current speed: 527k RPS (rust)
and 520 (idk) -- 528k RPS ([kotlin](https://github.com/qmained/todo-project)) 
(I hope I win!!!)

---

status: alive again. rewritten on [compio](https://github.com/compio-rs/compio) (io_uring, thread-per-core via `SO_REUSEPORT`),
implementing the L1 (hot slot) -> L2 (warm map) -> DB pipeline from `api_reference.md` as actual dispatched
tasks/continuations (`Db::send_task` / `Cache::send_task`), not flattened into plain `.await` calls.
needs nightly (`compio-executor` uses `cfg_select`, pinned via `rust-toolchain.toml`).
RPS numbers above are stale, haven't re-benched yet.

DB tier is real Postgres now (`postgres` + `r2d2`, schema auto-migrated on boot). Set `POSTGRES_URL`
(env or `.env`, e.g. `postgresql://user:pass@localhost:5432/todoapp`) before running.
