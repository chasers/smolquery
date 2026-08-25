# TLA+ models

Formal models of the safety-critical distributed algorithms in smolquery.
Each spec models one algorithm. TLC, the TLA+ model checker, explores every
reachable state and reports the first invariant violation.

This work started on a fork branch by @edgurgel
(`edgurgel:smolquery:tla`). [`FINDINGS.md`](FINDINGS.md) is the full log:
what each spec models, what TLC found, and the roadmap of algorithms not yet
modeled. Read it before you add or change a spec.

## How to run

```sh
./tla/run all                       # every config (also: mise run tla)
./tla/run ReleasedClaim_nocrash     # one config
```

The script downloads `tla2tools.jar` into `tla/` on first use. The jar and
the `tla/states/` scratch directory are git-ignored. The script runs TLC with
`-deadlock` because these specs have valid terminal states. A settled state
is not a bug; the `NoPermanentDouble` invariant catches a bad terminal state.

## Layout

- `<Module>.tla` — the spec.
- `<Module>[_variant].cfg` — one TLC configuration per scenario. The runner
  derives the module from the text before the first `_` in the config name.

## Current matrix

A VIOLATED row is intentional when it demonstrates a bug or proves a
property is load-bearing.

| config | checks | result |
|---|---|---|
| `SealHandoff` | base handoff + atomic compaction + correct dedup rule | PASS |
| `SealHandoff_naive` | dedup off the at-S listing instead of `registered_through` | VIOLATED (by design) |
| `SealHandoff_nonatomic` | compaction as two snapshots | VIOLATED (by design) |
| `ReleasedClaim` | T-294 fence as coded before PR #260, crash allowed | VIOLATED — F-1 |
| `ReleasedClaim_nocrash` | same, pure timing race, no crash | VIOLATED — F-1 |
| `ReleasedClaim_fix_nocrash` | gate fix modeled, reap allowed | VIOLATED — reap residual |
| `ReleasedClaim_fix_nocrash_noreap` | gate fix, no reap | PASS |
| `ReleasedClaim_fix_crash` | gate fix, crash allowed | VIOLATED — crash residual |
| `ReleasedClaim_reconciler` | gate fix + durable reconciler | PASS — complete fix |
| `SegmentShipping_core` | T-96 ship-before-ack durability, owner crash | PASS |
| `SegmentShipping_ackfirst` | ack the caller before the ship | VIOLATED (by design) |
| `SegmentShipping` | failed-flush compensation pre-T-390, no crash | VIOLATED — F-2a |
| `SegmentShipping_retry` | pre-T-390, caller retries the failed batch | VIOLATED — F-2b |
| `SegmentShipping_durabledrop` | drop owed durably, re-shipped until delivered | PASS — the T-390 fix |

The `GateFix` constant models the fix PR #260 applies (T-385). The
`Reconciler` constant models the durable reconciler built as T-386 — the
release tombstone plus the level-triggered `reconcile_released` signal; see
`FINDINGS.md` for the mapping from the spec's standing rule to the code.

## Conventions

- Keep a spec's header comment pointing at the exact Elixir functions and
  lines it models. When the code moves, update the spec or mark it stale.
- Every claimed property gets a negative config that breaks it. A spec that
  only passes proves little.
- When TLC confirms a bug, replicate it in a real Elixir test (snabbkaffe
  `force_ordering` works well) before you trust a fix.
- Log findings and results in `FINDINGS.md`. Track fixes and new specs in
  the project tracker (plan PL-54).
