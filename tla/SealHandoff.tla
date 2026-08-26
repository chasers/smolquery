---------------------------- MODULE SealHandoff ----------------------------
(*****************************************************************************)
(* Spec 1: exactly-once row accounting across the hot -> sealed boundary.   *)
(*                                                                           *)
(* Models the crown-jewel safety property of smolquery: a query planned at  *)
(* the current catalog snapshot counts each acked micro-segment's rows       *)
(* EXACTLY ONCE, while the seal handoff (merge -> put -> register -> retire) *)
(* and compaction run underneath, with crashes and idempotent retries.       *)
(*                                                                           *)
(* Faithful to:                                                              *)
(*   seal.ex             merge, put, register, retire                         *)
(*   planner.ex:582      include?/2                                           *)
(*   ducklake.ex:428     registered_through, using begin_snapshot <= S        *)
(*   sealed read AT S    a segment is visible iff begin <= S < end            *)
(*                                                                           *)
(* One claim R (a frozen set of micro-segments sharing claim_keys = {"K"}).  *)
(* Its seal produces sealed key "K"; compaction re-merges "K" into "K2".     *)
(* Both "K" and "K2" carry R's rows. The handoff steps are idempotent, so    *)
(* crashes / concurrent retries are modelled as each step being separately   *)
(* enabled and repeatable rather than as explicit attempt processes.         *)
(*****************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
  MaxSnap,      \* bound on the catalog snapshot counter (state-space cap)
  NaiveDedup,   \* TRUE => planner uses the at-S file listing (the WRONG rule)
                \* FALSE => planner uses registered_through (begin<=S, all history)
  AtomicCompact \* TRUE => compaction swaps in one snapshot; FALSE => two steps

KEYS == {"K", "K2"}   \* K = seal output; K2 = compaction merge of K. Both cover R.

NONE == 0             \* end_snapshot = NONE (open segment). Real begins/ends are >= 1.

VARIABLES
  snap,        \* current catalog snapshot (monotonic)
  catalog,     \* [KEYS -> [present, begin, end]] registered sealed segments
  rInManifest, \* is R's micro-segment set still served by the hot manifest?
  rRetired,    \* has R been retired (sealed_at stamped)?
  k2half       \* only for AtomicCompact=FALSE: K2 registered but K not yet dropped

vars == <<snap, catalog, rInManifest, rRetired, k2half>>

Absent == [present |-> FALSE, begin |-> 0, end |-> NONE]

TypeOK ==
  /\ snap \in 0..MaxSnap
  /\ catalog \in [KEYS -> [present: BOOLEAN, begin: 0..MaxSnap, end: 0..MaxSnap]]
  /\ rInManifest \in BOOLEAN
  /\ rRetired \in BOOLEAN
  /\ k2half \in BOOLEAN

Init ==
  /\ snap = 0
  /\ catalog = [k \in KEYS |-> Absent]
  /\ rInManifest = TRUE
  /\ rRetired = FALSE
  /\ k2half = FALSE

(***************************************************************************)
(* The seal handoff. register is idempotent (guarded by committed?), so a  *)
(* crash before it just re-runs the merge and re-registers the same key.    *)
(***************************************************************************)

\* commit: merge -> put -> register K. Idempotent: no-op if K already present.
Register ==
  /\ snap < MaxSnap
  /\ ~catalog["K"].present
  /\ catalog' = [catalog EXCEPT !["K"] = [present |-> TRUE, begin |-> snap + 1, end |-> NONE]]
  /\ snap' = snap + 1
  /\ UNCHANGED <<rInManifest, rRetired, k2half>>

\* retire: stamp R sealed. Reachable only once K is committed. Idempotent.
Retire ==
  /\ catalog["K"].present
  /\ ~rRetired
  /\ rRetired' = TRUE
  /\ UNCHANGED <<snap, catalog, rInManifest, k2half>>

\* the grace-period reaper drops R's entries from the manifest after retire.
Reap ==
  /\ rRetired
  /\ rInManifest
  /\ rInManifest' = FALSE
  /\ UNCHANGED <<snap, catalog, rRetired, k2half>>

(***************************************************************************)
(* Compaction: replace_segments/4 registers merged K2 and drops K in one    *)
(* DuckLake transaction (one snapshot). AtomicCompact=FALSE splits it to     *)
(* demonstrate the "swap is atomic" requirement.                            *)
(***************************************************************************)

CompactAtomic ==
  /\ AtomicCompact
  /\ snap < MaxSnap
  /\ catalog["K"].present
  /\ catalog["K"].end = NONE
  /\ ~catalog["K2"].present
  /\ LET s == snap + 1 IN
       catalog' = [catalog EXCEPT !["K"].end = s,
                                  !["K2"] = [present |-> TRUE, begin |-> s, end |-> NONE]]
  /\ snap' = snap + 1
  /\ UNCHANGED <<rInManifest, rRetired, k2half>>

\* Non-atomic: register K2 first (a bad implementation) ...
CompactAddK2 ==
  /\ ~AtomicCompact
  /\ snap < MaxSnap
  /\ catalog["K"].present
  /\ catalog["K"].end = NONE
  /\ ~catalog["K2"].present
  /\ catalog' = [catalog EXCEPT !["K2"] = [present |-> TRUE, begin |-> snap + 1, end |-> NONE]]
  /\ snap' = snap + 1
  /\ k2half' = TRUE
  /\ UNCHANGED <<rInManifest, rRetired>>

\* ... then drop K in a later snapshot.
CompactDropK ==
  /\ ~AtomicCompact
  /\ snap < MaxSnap
  /\ k2half
  /\ catalog["K"].end = NONE
  /\ catalog' = [catalog EXCEPT !["K"].end = snap + 1]
  /\ snap' = snap + 1
  /\ k2half' = FALSE
  /\ UNCHANGED <<rInManifest, rRetired>>

Compact == CompactAtomic \/ CompactAddK2 \/ CompactDropK

Next == Register \/ Retire \/ Reap \/ Compact

\* allow stuttering at a terminal state so TLC does not report a false deadlock
Stutter ==
  /\ ~ENABLED Next
  /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Stutter]_vars

(***************************************************************************)
(* The query planner, reading at the current snapshot S = snap.             *)
(***************************************************************************)

VisibleAt(k, S) ==
  /\ catalog[k].present
  /\ catalog[k].begin <= S
  /\ (catalog[k].end = NONE \/ catalog[k].end > S)

\* registered_through(S): every key ever registered by S, ignoring end_snapshot
RegisteredThrough(S) == {k \in KEYS : catalog[k].present /\ catalog[k].begin <= S}

\* the WRONG rule some readers might reach for: the file listing visible AT S
VisibleKeys(S) == {k \in KEYS : VisibleAt(k, S)}

\* include?/2 : R is included in the hot tier iff NOT all its claim keys ({"K"})
\* are in the reference set. Correct reference = registered_through; naive = at-S.
Included(S) ==
  IF NaiveDedup
  THEN ~("K" \in VisibleKeys(S))
  ELSE ~("K" \in RegisteredThrough(S))

HotCount(S)    == IF rInManifest /\ Included(S) THEN 1 ELSE 0
SealedCount(S) == Cardinality({k \in KEYS : VisibleAt(k, S)})

\* THE invariant: at the current snapshot, R's rows are counted exactly once.
ExactlyOnce == LET S == snap IN HotCount(S) + SealedCount(S) = 1

=============================================================================
