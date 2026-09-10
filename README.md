some random things 
current speed: 527k RPS (rust)
and 520 (idk) -- 528k RPS ([kotlin](https://github.com/qmained/todo-project)) 
(I hope I win!!!)

---

status: alive again. rewritten on [compio](https://github.com/compio-rs/compio) (io_uring, thread-per-core via `SO_REUSEPORT`),
implementing the L1 (hot slot) -> L2 (warm map) -> DB pipeline from `api_reference.md`.
needs nightly (`compio-executor` uses `cfg_select`, pinned via `rust-toolchain.toml`).
RPS numbers above are stale, haven't re-benched yet.
