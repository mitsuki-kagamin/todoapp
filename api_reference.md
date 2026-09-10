fn handle(channel: &mut TcpChannel) {
    // parse...

    db.send_task(Task::Get(id), |channel| {
        channel.send(answer);
        channel.close();

        cache.send_task(
            Task::Write(id, answer),
            |_| {},
        );
    });
}

db.send_task(Task::Get(id), continuation);
cache.send_task(Task::Write(id, value), |_| {});

packet
  │
  ▼
task
  │
  ▼
parse
  │
  ▼
L1
 ├─ hit ───────────────► answer
 │
 └─ miss
      │
      ▼
     L2
      ├─ hit ─────────► answer
      │                  └─ send_task(FillL1, |_| {})
      │
      └─ miss
           │
           ▼
          DB
           │
           └─ send_task(Req(id), |handle| {
                  handle.send(answer);
                  
                  cache.send_task(FillAll, |_| {});
              });
