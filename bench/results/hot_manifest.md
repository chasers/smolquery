# Hot-tier reads — `bench/hot_manifest.exs`

| | |
|---|---|
| Run | 2026-08-19 |
| Commit | `dd16a12` (T-315/T-316 applied; T-317 not yet) |
| Command | `mix run bench/hot_manifest.exs`, then `BENCH_SECTION=contention mix run bench/hot_manifest.exs` |
| Machine | Apple M1 Max, 10 cores, 64 GiB, macOS 26.6.1 |
| Runtime | Elixir 1.20.2 / OTP 29, 10 schedulers (10 online), 10 dirty CPU |

## manifest read cost against backlog depth (T-316)

32 columns; the scoped read names 64 ids and skips stats.

```
  backlog     route         served     ms p50     ms p99      KiB    B/entry

       64     GET whole         64        8.3       99.8    183.3       2932
       64     POST claim        64        1.2       14.3     16.9        269

      256     GET whole        256       36.0       56.8    737.8       2951
      256     POST claim        64        0.9        1.5     16.9        270

     1024     GET whole       1024      213.6      596.5   2957.8       2957
     1024     POST claim        64        1.1        2.5     16.9        270

     4096     GET whole       4096      970.2     1552.5  11892.7       2973
     4096     POST claim        64        1.1        9.3     16.9        270
```

## what an entry costs, against table width

```
  columns    with stats    without    stats share

        4           586        270          53.9%
       16          1511        270          82.1%
       32          2752        271          90.2%
       63          5184        271          94.8%
```

## served reads per second, and the seal rate that buys

4 concurrent readers for 5s; a seal attempt is two scoped reads.

```
  backlog     route         reads/s     MiB/s     ms p50     ms p99     seals/s

       64     GET whole       361.7      64.7        9.0       40.8       180.9
       64     POST claim     3018.5      49.7        1.1        4.5      1509.3

      256     GET whole        91.5      65.9       43.0       61.7        45.7
      256     POST claim     3401.2      56.1        1.1        2.9      1700.6

     1024     GET whole        15.3      44.1      256.2      327.5         7.6
     1024     POST claim     3454.7      56.9        1.1        2.2      1727.4

     4096     GET whole         2.4      27.6     1292.2     2538.7         1.2
     4096     POST claim     2866.5      47.2        1.3        3.2      1433.2
```

## what the reads cost the commits (PL-45)

8 writers of 200 rows against a 1024-entry backlog, 32 columns, 5s; 4 readers
offering 20 reads/s.

```
  read load        commits/s      rows/s     ack p50     ack p99     reads/s

  none                 151.5     30303.4        50.0       246.7         0.0
  GET whole            113.9     22788.1        68.0       131.4        11.9
  POST claim           161.0     32193.8        50.0        65.4        20.7
```

## What this settles

- **The whole-manifest read cannot keep up with the buffer, and the scoped read
  has an order of magnitude of headroom.** At a 1,024-entry backlog the `GET`
  route sustains 15.3 reads/s — 7.6 seal attempts per second — while the same
  node commits about 150 batches/s. At 4,096 it sustains 2.4 reads/s, so **1.2
  seal attempts per second**. Sealing cannot drain a backlog it takes a second
  per attempt to read. The `POST` route sustains 2,866–3,455 reads/s at every
  depth measured, which is 1,433–1,727 seal attempts per second. That is the
  feedback loop in PL-45 closed: seal cost no longer scales with the backlog
  the seal is trying to drain.

- **The scoped read is flat, and the whole read is not flat on any axis.**
  1.1 ms and 16.9 KiB at every backlog from 64 to 4,096, against 8.3 ms/183 KiB
  rising to 970 ms/11.9 MiB. At 4,096 that is **880× the latency and 700× the
  bytes** for an answer the sealer discards all but 64 entries of.

- **The stats block is the entry, once a table is real.** 53.9% of an entry at
  4 columns, **94.8% at 63** — the width the 2026-08-19 soak ran. A stats-free
  entry is 270 bytes and does not move with width, because nothing else in the
  record is per column. `stats: false` is therefore most of the win at
  production widths, and the id filter is most of it at production depths.

- **Contention is real, and it is a throughput loss rather than a latency
  loss.** At 20 offered reads/s against a 1,024 backlog, the `GET` route costs
  **25% of commit throughput** (151.5 → 113.9 batches/s) *and still fails to
  meet the offered rate* — it achieves 11.9 of 20. The `POST` route meets the
  full rate (20.7) at no measurable commit cost. This is the shape the soak
  reported: commit service time barely moves while commit concurrency falls.

- **The 45 MB estimate in PL-45 was conservative in one direction and generous
  in the other.** A 63-column entry measures 5,184 bytes against the plan's
  ~6.7 KB estimate, so a 6,738-entry backlog is nearer **35 MB** than 45 MB.
  The estimate's conclusion stands: the response is tens of megabytes, built on
  a pod with a 4 GiB limit that is also committing.

- **Filling the fixture found a second O(backlog) term, on the write path.**
  Writing 4,096 unsealed entries took far longer than 4× writing 1,024 — with
  no reader running at all. That is `TableBuffer`'s maintenance tick, not
  `HotServer`, and it closes the same loop from the other side. Filed as T-317;
  see `bench/results/buffer.md`'s `backlog_drag` section.
