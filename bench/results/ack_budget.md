# `bench/ack_budget.exs` — the ack budget under overload (PL-9, T-56)

| | |
|---|---|
| Run | 2026-08-01 |
| Commit | `c669177` (M6 L7 tip + the PL-9 admission changes) |
| Command | `mix run bench/ack_budget.exs` (defaults: `WRITERS=512`, `CALLS=10`, `ROWS=2000`) |
| Machine | Apple M1 Max · 10 cores · 64 GiB · macOS 26.5.2 |
| Runtime | Elixir 1.20.2 / OTP 29 |

The T-53 overload, re-armed: a 20-column event table at the default flush
config, 512 synchronous writers of 2,000-row batches, no retries. One write
warms the rate estimate before the storm (a cold meter admits by design —
PL-9 D3).

## Headline

**The budget bounds the ack.** Unbounded, this storm queues a **6.1 s p50**
in the buffer's mailbox — the T-53 defect, scaled up by the bigger herd. At
`ack_budget_ms: 1000` every accepted percentile lands inside the budget
(p50 634 ms, p99 983 ms) at *equal-or-better* throughput, and the 88% of
attempts shed are refused in microseconds carrying a prediction
(~1 s median) a client could sleep on. The aggressive 250 ms budget holds
p50 at 160 ms but costs ~31% throughput — small admitted waves under-pack
the flush — so the default stays 5 s: a guardrail against pathology, not a
latency SLO.

## Ack under overload — 20-col schema, 512 writers × 10 calls × 2000 rows

```
budget      accepted  shed %  krows/s  p50 ms  p95 ms  p99 ms  shed p50 ms
:infinity       5120     0.0    164.4    6066    6567    6756            -
5000            1567    69.4    165.1    1695    1980    2255         5004
1000             595    88.4    170.4     634     891     983         1009
250              183    96.4    113.0     160     264     305          259
```

(The `5000` row — the shipped default, `BUDGETS=5000` — was measured in a
follow-up run of the same command.)

Reading it:

- **`:infinity` is the pre-PL-9 write path.** Every attempt is accepted and
  every attempt waits ~6 s — Little's Law on 1.02M outstanding rows at
  ~170K rows/s service. `flush_interval_ms` is nowhere in that number.
- **At 1000 ms the contract inverts**: the *predicted-over-budget* attempts
  are refused instantly, so what remains waits what the budget promised.
  Shed p50 prediction (1009 ms) sits just over the budget — the meter is
  rejecting exactly the marginal writes.
- **Throughput does not pay for admission** at 1000 ms (170.4 vs 164.4
  krows/s — shedding removes mailbox pressure, it does not idle the
  encoder). At 250 ms it does (−31%): admitted waves of ~40K rows no longer
  fill `flush_max_bytes`, so the encoder runs smaller, less efficient
  flushes.
- **p95/p99 overshoot the budget by ≤22%** (891/983 at 1000; 264/305 at
  250): a batch admitted at the line still waits behind the encode of the
  flush ahead of it. The budget bounds the *queue*, and the residual is one
  service time.

## What this settles

- **T-56 / PL-9's exit criterion: met.** Accepted-write p50 lands under the
  budget in both bounded cells; the 6 s silent queue is gone; shed writes
  carry an actionable number (the API returns it as `retry-after`).
- **The default `ack_budget_ms: 5_000`** never fires below saturation
  (predicted wait ~0) and converts the pathological cell from a 15 s
  `write_timeout_ms` cliff into a typed refusal with headroom to spare.
  Measured at the default: 165 krows/s accepted — the same throughput as
  unbounded — with p99 at 2.3 s instead of 6.8 s. The budget picks who
  waits, not how fast the encoder runs.
- **Budgets at or below the flush interval are a real trade** (−31%
  throughput at 250 ms): the knob expresses it, the default avoids it.
- **PL-6's partitioning remains worth doing** — it raises the saturation
  point so the budget fires less often; this work defines what happens when
  it fires anyway.
