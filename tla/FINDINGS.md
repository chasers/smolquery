# TLA+ modeling of smolquery — findings & roadmap

Goal: use TLA+/TLC to formally check the **safety-critical distributed
algorithms** in smolquery — the places where a crash, retry, or concurrent
actor could lose rows or count them twice. Track what has been modeled, what
TLC found, and what is next so a follow-up agent can pick up.

Toolchain: `./tla/run <config>` (fetches `tla2tools.jar` on first use). Specs
live in `tla/`.

> **Provenance / state note (2026-08-25).** This document and the specs come
> from the fork branch `edgurgel:smolquery:tla`. The code changes on that
> branch — the F-1 gate fix in `seal.ex`/`hot_manifest.ex`, the snabbkaffe
> replication test, and the snabbkaffex dep — are **not merged**; on this
> repo's `main` the F-1 bug is live. The "gate fix APPLIED" section below
> describes the fork branch, not `main`. T-385 tracks the merge decision and
> T-386 the durable reconciler (plan PL-54).

## What to cover, ranked by safety value

The architecture doc (`docs/architecture.md`) and the two memory notes name the
algorithms that *state a safety guarantee*. Ranked by "a bug here loses or
double-counts acked rows":

1. **Exactly-once row accounting across the hot→sealed boundary** (THE crown
   jewel). Combines:
   - the seal handoff `merge → put → register → retire`
     (`lib/smolquery/storage_service/handoff/seal.ex`), idempotent from any
     crash point;
   - the query planner's dedup rule
     (`lib/smolquery/query_service/planner.ex:582` `include?/2` +
     `Catalog.registered_through/3`, `begin_snapshot <= S`);
   - the sealed side read `AT (VERSION => S)` (visible iff `begin<=S<end`).
   Invariant: a query at *any* snapshot counts each acked micro-segment's rows
   **exactly once**, across concurrent seal attempts, crashes, retries.
   → **Spec 1: `SealHandoff.tla`** (in progress).

2. **T-294 three-gate fence for a released oversized claim.** A claim released
   and re-derived under new keys must not double-commit. Gates: `claim_live`
   before merge, `claim_live` between merge and register, retire key-fence, plus
   `compensate_stale` dropping an already-registered orphan. The doc *admits* a
   transient double-count window (register → compensating drop). Prime bug-hunt
   target: is that window the *only* one, and does compensation always fire?
   → **Spec 2: extend Spec 1 with release/re-derive** (next).

3. **Compaction ↔ dedup interaction.** `replace_segments/4` sets `end_snapshot`
   on inputs (drops them from `AT (VERSION=>S)`) while a live hot entry still
   names those keys. Correctness hinges on `registered_through` using
   `begin<=S` (all history) *not* the at-S file listing. Model compaction as a
   drop+add in one snapshot and check exactly-once still holds.
   → folds into Spec 1.

4. **RingEpoch ownership fence (T-92).** At most one buffer owner accepts writes
   under eventually-consistent `:pg` membership + Postgres CAS epoch + settle
   window. Mutual-exclusion safety.
   → **Spec 3: `RingEpoch.tla`** (later).

5. **Exactly-once inserts (batch_id dedup).** Crash-before-reply + log replay;
   an id'd batch retried through any failure lands once.
   → **Spec 4** (later).

6. **Segment shipping replication (T-96).** All-replicas ack; a failed shipment
   compensates the local commit away — "no side keeps rows the caller was told
   failed."
   → **Spec 5** (later).

## Toolchain notes

- Java comes from mise (`java = "corretto"` in `mise.toml`). `./tla/run`
  wraps the TLC invocation (`java -XX:+UseParallelGC -cp tla2tools.jar
  tlc2.TLC -deadlock -config <cfg> <tla>`).
- The seal/release specs have valid terminal states, so run them with
  `-deadlock` (disables deadlock checking) — a deadlock there is just a settled
  state, not a bug. `NoPermanentDouble` is the invariant that catches a bad
  terminal state.

## Status log

- 2026-08-25: Read architecture + seal/planner/hot_manifest code. Ranked
  targets. Built + ran **Spec 1** (`SealHandoff.tla`) and **Spec 2**
  (`ReleasedClaim.tla`). Spec 1 PASSES; Spec 2 found a real permanent
  double-count (F-1 below). Specs 3-6 not started.

## Spec 1 — `SealHandoff.tla` (base handoff + compaction): PASS

Models `merge→put→register→retire` (idempotent, crashable) + atomic compaction
+ the planner dedup rule (`include?/2` over `registered_through`, `begin<=S`) +
sealed read `AT (VERSION=>S)` (`begin<=S<end`). Invariant: a query at the
current snapshot counts each claim's rows exactly once.

- `SealHandoff.cfg` (correct rule, atomic compaction): **No error**, exhaustive.
- `SealHandoff_naive.cfg` (`NaiveDedup=TRUE`, dedup off the at-S file listing
  instead of registered-through): **VIOLATED** — after compaction drops K from
  the at-S listing, a live hot entry naming K is re-included alongside the merged
  K2 → double count. Confirms `registered_through` spanning history is
  load-bearing.
- `SealHandoff_nonatomic.cfg` (`AtomicCompact=FALSE`): **VIOLATED** — a window
  where both K and K2 are visible → double count. Confirms "the swap is atomic"
  is load-bearing.

Takeaway: the base exactly-once design is sound, and the two properties the doc
leans on (registered-through history, atomic swap) each have teeth.

## Findings

### F-1 (CONFIRMED by TLC): T-294 fence has a hole — a released oversized claim's in-flight attempt can strand a double-counting orphan segment

**Severity:** high (permanent silent double-count of acked rows; no data loss).
**Where:** `storage_service/handoff/seal.ex` (`claim_live/2` at :177, the
`merge→register→retire` flow at :117), `buffer_service/hot_manifest.ex`
(`retire/6` at :660, `unsealed`). **Model:** `tla/ReleasedClaim.tla`.

**The claim (from the design, T-294):** when a claim frozen larger than the
current valves is *released* and re-derived under new keys, an in-flight seal
attempt for the *original* claim must not leave its segment registered. Three
gates + `compensate_stale` are meant to guarantee this: `claim_live` before
merge, `claim_live` before register, the retire key-fence, and dropping an
already-registered orphan on any `stale_claim` error.

**The hole:** all three gates **skip `sealed_at` entries** as "reconciliation".
`claim_live/2` does `Enum.reject(&(&1["sealed_at"] || &1["claim_keys"] == keys))`,
and `retire/6` acts only on `unsealed(ids)` (returns `:ok` when that is empty).
So if the **re-derived valve-sized claims seal (and stamp `sealed_at` on) the
released claim's micro-segments before the in-flight original attempt reaches
its `claim_live`/register/retire steps**, the original attempt sees *no* stale
entry at any gate, registers its oversized orphan segment, and `retire` returns
`:ok` **without** tripping the fence or `compensate_stale`. Because a released
claim is *never re-signalled* (`table_buffer.ex:700-701`) and GC deliberately
spares registered segments, nothing ever removes the orphan → the released
claim's rows are counted **twice forever** (once via the orphan `K`, once via
the re-derived `K1`/`K2`), against every query snapshot.

The timing is *favoured*, not adversarial: the released claim is oversized, so
its merge is the slowest in the system, while the re-derived claims are
valve-sized and seal fast. **No crash is required** — it is enough that the
original attempt's separate `retire` control-plane call is delayed past the
re-derived seals. A crash between register and retire strands it too.

TLC counterexample (no-crash, `ReleasedClaim_nocrash.cfg`, 13 steps):
```
OStart → OGate1(live) → OGate2Commit(register K)   \* original registers its segment
→ Release                                          \* claim released, ids → pending ([])
→ Reclaim(m1) → ReRegister(K1) → ReRetire(m1 sealed) → ReReap(m1)
→ Reclaim(m2) → ReRegister(K2) → ReRetire(m2 sealed)
→ ORetireEmpty                                     \* original retire: all sealed → :ok, NO fence
\* terminal: K, K1, K2 all registered → r1,r2 double-counted, nothing can drop K
```
Both `ReleasedClaim.cfg` (crash allowed) and `ReleasedClaim_nocrash.cfg`
(`AllowCrash=FALSE`, pure timing) violate `NoPermanentDouble`. `NoLoss` holds
throughout — this is over-counting, never loss. The model uses the faithful
three-state `claim_keys` transition `orig → none([]) → re` matching
`hot_manifest.ex:587` (`Entry.claim(_, [])`).

**Why the design missed it:** the doc's argument is "a crash between commit and
retire is closed the same way by the next actor to look." For a *released* claim
there is no next actor for that claim, and the in-flight attempt itself can pass
every gate because the gates skip the now-sealed entries. The fence detects
"claim_keys moved" only on entries that are still *unsealed*.

**Fix directions (not yet applied — needs owner decision):**
1. Make `commit`/`merge_and_register` re-check membership of *the re-derived
   coverage*, not just `committed?(K)`: before registering `K`, verify the
   claim's ids are still claimed under `K` (treat a `sealed_at` set by a
   *different* claim key as stale, not as reconciliation). I.e. `claim_live`
   should reject `sealed_at` only when the entry was sealed under *this* claim's
   keys.
2. Or: on release, record the released claim's output key(s) as tombstones so a
   later `register` of `K` is refused / auto-compensated.
3. Or: gate `register_segments` for a seal on "no other registered segment
   already covers these ids' rows".
Any fix must keep the legitimate reconciliation case (this claim's own earlier
attempt sealed the ids) working — that is why the skip exists.

**Replication (snabbkaffe): DONE — reproduced in real Elixir code.**
`test/smolquery/storage_service/handoff/released_claim_double_count_test.exs`
(`:integration`, `async: false`). snabbkaffe was not a dep and the old memory's
tracepoints did not exist; added `{:snabbkaffex, "~> 0.1.0", runtime: false}`
(snabbkaffe itself was already locked via gen_rpc) and three tracepoints to
`seal.ex` via `use Snabbkaffex, only: :trace` (no-op in prod):
`storage.seal.registered`, `storage.seal.before_retire`, `storage.seal.retired`
(each carrying `table_ref` + claim `keys`).

The test drives the real concurrent `Handoff.seal/4`: it runs the original
oversized attempt in a `Task`, uses `force_ordering(delay:
storage.seal.before_retire[orig keys], until: storage.seal.retired[table],
count: 2)` to park it right before retire until the two re-derived seals have
retired, then unparks it. Results (deterministic across seeds):
- the original attempt returns `:ok` (NOT `{:error, {:stale_claim, _}}`), so the
  fence never tripped and `compensate_stale` never ran;
- the catalog ends with **3** registered segments (orphan `K` + re-derived
  `K1`,`K2`);
- `visible_ids/1` (the planner's own dedup rule applied to the real catalog + hot
  manifest, then the rows actually read) returns every row **twice**:
  `[1,1,2,2,3,3,4,4]`.

A PASS of this test means the bug is present; it is expected to FAIL (and should
be inverted to the correct assertion) once F-1 is fixed. The existing 19 tests in
`seal_test.exs` stay green with the added tracepoints.

## F-1 fix — status: gate fix APPLIED; durable reconciler still OPEN

**Applied (gate fix).** Two edits make the fence treat a `sealed_at` stamped
under a *different* claim key as stale rather than reconciliation:
- `storage_service/handoff/seal.ex` `claim_live/2`: reject only entries whose
  `claim_keys == keys` (dropped the `sealed_at ||` skip). A sealed entry under
  other keys is now stale, not skipped.
- `buffer_service/hot_manifest.ex` `retire/6`: check staleness over *every*
  still-present entry (sealed or not), not just `unsealed(ids)`. Previously
  `unsealed == [] -> :ok` blinded the fence when the re-derived claims had
  already sealed the ids.

With this, the original attempt's `retire` returns `{:error, {:stale_claim,
...}}` and `compensate_stale` drops the orphan. Verified:
- Replication test inverted to the correct outcome and PASSES:
  `released_claim_double_count_test.exs` — original seal returns `stale_claim`,
  `visible_ids == [1,2,3,4]`, `sealed_count == 2`, compensate log fires.
- `claim_test.exs` retire-fence test split/updated to the corrected contract
  (a sealed id retired under its own keys/none stays `:ok`; under different keys
  is now `{:error, {:stale_claim}}`). Affected suites green (handoff + hot_manifest
  = 57 tests; buffer+storage = all but one pre-existing env-flaky
  `multi_node_seal_ownership_test` that fails on clean `main` too, `:eaddrinuse`).

**Still open (needs a durable reconciler).** Re-running the TLA model after
modelling the gate fix shows it is necessary but NOT sufficient — two residuals
remain where the in-flight attempt's compensation opportunity is lost:
1. **crash-after-register** (case c): the attempt dies between register and
   retire; a released claim is never re-signalled, so nothing re-runs it.
2. **reap-before-retire**: the original's `retire` is delayed past
   `retire_grace_ms`, so the grace reaper deletes the entries; `retire`'s
   `lookup` then returns `[]` and the fence has no evidence.

Both need a durable reconciler independent of the in-flight attempt: on release,
tombstone the released claim's output key(s); a storage sweep (piggybacked on
GC, which already scans the catalog) drops any registered segment whose key is
tombstoned **once the re-derived segments cover its rows** (safe: no loss,
because the rows are then in the sealed tier).

TLA config matrix (`tla/ReleasedClaim.tla`, `AllowCrash`/`GateFix`/`AllowReap`/
`Reconciler`):

| config | Crash | GateFix | Reap | Reconciler | result |
|---|:--:|:--:|:--:|:--:|---|
| `ReleasedClaim` | ✓ | – | ✓ | – | VIOLATED (original F-1) |
| `ReleasedClaim_nocrash` | – | – | ✓ | – | VIOLATED |
| `ReleasedClaim_fix_nocrash` | – | ✓ | ✓ | – | VIOLATED (reap residual) |
| `ReleasedClaim_fix_nocrash_noreap` | – | ✓ | – | – | **PASS** (= Elixir test window) |
| `ReleasedClaim_fix_crash` | ✓ | ✓ | ✓ | – | VIOLATED (crash residual) |
| `ReleasedClaim_reconciler` | ✓ | ✓ | ✓ | ✓ | **PASS** (complete fix) |

## Next up for a follow-up agent

- **Durable reconciler — deferred by decision (2026-08-25).** The owner chose to
  ship the gate fix only for now and leave the two residuals (crash-after-register,
  retire-past-grace) documented here. When picked up: build the release-key
  tombstone + a GC-piggybacked sweep that drops a tombstoned orphan once the
  re-derived coverage is registered. The model already confirms it closes both
  residuals (`ReleasedClaim_reconciler.cfg` PASSES with crashes+reaps). Add a
  snabbkaffe test for the crash residual (`inject_crash` at
  `storage.seal.before_retire`) once built.
- Then Specs 3-6 (RingEpoch T-92, exactly-once inserts, segment shipping T-96).
- Spec 3: RingEpoch ownership fence (T-92) — at-most-one-owner mutual exclusion.
- Spec 4: exactly-once inserts (batch_id dedup, crash-before-reply + replay).
- Spec 5: segment-shipping replication (T-96) all-replicas ack + compensation.
- Consider extending Spec 1/2 to model in-flight (frozen) queries + grace
  windows (retire_grace < snapshot_keep) — the "physical durability of a frozen
  plan" property (B), distinct from the logical dedup (A) checked so far.
