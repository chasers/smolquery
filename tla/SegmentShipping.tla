-------------------------- MODULE SegmentShipping --------------------------
(*****************************************************************************)
(* Spec 5: segment-shipping replication (T-96, PL-5 Stage 1).                *)
(*                                                                           *)
(* The owner group-commits locally (store-put + manifest add), then ships    *)
(* the entry+bytes to its follower and waits for the fsync ack; only then    *)
(* is the caller acked (segment_shipping.ex commit/2, committer.ex           *)
(* replicate/3). A failed shipment compensates: a drop is BEST-EFFORT        *)
(* shipped to the follower, the entry is dropped locally (which also         *)
(* forgets its batch_id record — hot_manifest.ex forget_batches), and the    *)
(* caller is told the flush failed.                                          *)
(*                                                                           *)
(* The design claims (moduledoc): "no side keeps rows the caller was told    *)
(* failed", with one documented residual — a follower that applied the       *)
(* entry, missed the drop, and was PROMOTED before the owner could           *)
(* compensate.                                                               *)
(*                                                                           *)
(* This spec checks that claim against the read side as actually built:      *)
(* the planner fans hot-manifest reads out to EVERY member                   *)
(* (planner.ex manifest_urls, Client.manifest_nodes) and dedupes by entry    *)
(* id — so a follower's un-dropped entry is served WITHOUT any promotion,    *)
(* and a retried batch gets a fresh id the dedupe cannot tie to the zombie.  *)
(*                                                                           *)
(* One batch of rows; e1 is the first flush's entry, e2 the retry's. The     *)
(* follower does not crash (absence_tolerance = RF-1 = 1 covers one loss;    *)
(* losing both copies loses rows trivially).                                 *)
(*****************************************************************************)
EXTENDS Naturals

CONSTANT AckBeforeShip \* TRUE => the WRONG order: ack the caller before shipping
CONSTANT DurableDrop   \* TRUE => a lost compensating drop is re-shipped until
                       \*         delivered (fix direction), not fire-and-forget
CONSTANT AllowRetry    \* TRUE => the caller retries a failed batch (fresh entry id)
CONSTANT AllowCrash    \* TRUE => the owner can crash

E == {"e1", "e2"}

VARIABLES
  mO,          \* [E -> BOOLEAN] : entry present in the owner's manifest
  mF,          \* [E -> BOOLEAN] : entry present in the follower's manifest
  dropPending, \* [E -> BOOLEAN] : a compensating drop still owed to the follower
  crashed,     \* the owner has crashed
  cur,         \* the entry the current flush attempt writes
  caller,      \* what the caller believes: "waiting" | "acked" | "failed"
  pc           \* the owner's flush machine

vars == <<mO, mF, dropPending, crashed, cur, caller, pc>>

TypeOK ==
  /\ mO \in [E -> BOOLEAN]
  /\ mF \in [E -> BOOLEAN]
  /\ dropPending \in [E -> BOOLEAN]
  /\ crashed \in BOOLEAN
  /\ cur \in E
  /\ caller \in {"waiting", "acked", "failed"}
  /\ pc \in {"idle", "added", "early", "shipped", "shipfail", "comp", "done"}

Init ==
  /\ mO = [e \in E |-> FALSE]
  /\ mF = [e \in E |-> FALSE]
  /\ dropPending = [e \in E |-> FALSE]
  /\ crashed = FALSE
  /\ cur = "e1"
  /\ caller = "waiting"
  /\ pc = "idle"

(***************************************************************************)
(* The flush: local add, ship to the follower, ack or compensate.           *)
(***************************************************************************)

Add ==
  /\ pc = "idle" /\ ~crashed /\ caller = "waiting"
  /\ mO' = [mO EXCEPT ![cur] = TRUE]
  /\ pc' = "added"
  /\ UNCHANGED <<mF, dropPending, crashed, cur, caller>>

\* the wrong order, for the negative control: caller acked before the ship
AckEarly ==
  /\ AckBeforeShip
  /\ pc = "added" /\ ~crashed
  /\ caller' = "acked"
  /\ pc' = "early"
  /\ UNCHANGED <<mO, mF, dropPending, crashed, cur>>

EarlyShip ==
  /\ pc = "early" /\ ~crashed
  /\ \/ mF' = [mF EXCEPT ![cur] = TRUE]
     \/ UNCHANGED mF
  /\ pc' = "done"
  /\ UNCHANGED <<mO, dropPending, crashed, cur, caller>>

\* the follower applied and fsynced; the ack reached the owner
ShipOk ==
  /\ ~AckBeforeShip
  /\ pc = "added" /\ ~crashed
  /\ mF' = [mF EXCEPT ![cur] = TRUE]
  /\ pc' = "shipped"
  /\ UNCHANGED <<mO, dropPending, crashed, cur, caller>>

\* the follower applied and fsynced, but the ack was lost (timeout): the
\* owner sees a failed flush while the follower holds the entry
ShipAppliedAckLost ==
  /\ ~AckBeforeShip
  /\ pc = "added" /\ ~crashed
  /\ mF' = [mF EXCEPT ![cur] = TRUE]
  /\ pc' = "shipfail"
  /\ UNCHANGED <<mO, dropPending, crashed, cur, caller>>

\* the follower never applied (unreachable / refused)
ShipUnreachable ==
  /\ ~AckBeforeShip
  /\ pc = "added" /\ ~crashed
  /\ pc' = "shipfail"
  /\ UNCHANGED <<mO, mF, dropPending, crashed, cur, caller>>

Ack ==
  /\ pc = "shipped" /\ ~crashed
  /\ caller' = "acked"
  /\ pc' = "done"
  /\ UNCHANGED <<mO, mF, dropPending, crashed, cur>>

(***************************************************************************)
(* Compensation. The drop ship is fire-and-forget in the code               *)
(* (segment_shipping.ex compensate/3 discards the result); DurableDrop       *)
(* models the fix — a lost drop stays owed and is re-shipped until it        *)
(* lands.                                                                   *)
(***************************************************************************)

DropDelivered ==
  /\ pc = "shipfail" /\ ~crashed
  /\ mF' = [mF EXCEPT ![cur] = FALSE]
  /\ pc' = "comp"
  /\ UNCHANGED <<mO, dropPending, crashed, cur, caller>>

DropLost ==
  /\ pc = "shipfail" /\ ~crashed
  /\ dropPending' =
       IF DurableDrop THEN [dropPending EXCEPT ![cur] = TRUE] ELSE dropPending
  /\ pc' = "comp"
  /\ UNCHANGED <<mO, mF, crashed, cur, caller>>

RedropDelivered(e) ==
  /\ dropPending[e] /\ ~crashed
  /\ mF' = [mF EXCEPT ![e] = FALSE]
  /\ dropPending' = [dropPending EXCEPT ![e] = FALSE]
  /\ UNCHANGED <<mO, crashed, cur, caller, pc>>

\* local drop (also forgets the batch_id record), then the error reply. The
\* caller either retries under a FRESH entry id, or is finally told failed.
LocalDropAndReply ==
  /\ pc = "comp" /\ ~crashed
  /\ mO' = [mO EXCEPT ![cur] = FALSE]
  /\ IF AllowRetry /\ cur = "e1"
     THEN /\ cur' = "e2"
          /\ pc' = "idle"
          /\ UNCHANGED caller
     ELSE /\ caller' = "failed"
          /\ pc' = "done"
          /\ UNCHANGED cur
  /\ UNCHANGED <<mF, dropPending, crashed>>

OCrash ==
  /\ AllowCrash /\ ~crashed
  /\ crashed' = TRUE
  /\ UNCHANGED <<mO, mF, dropPending, cur, caller, pc>>

Next ==
  \/ Add \/ AckEarly \/ EarlyShip
  \/ ShipOk \/ ShipAppliedAckLost \/ ShipUnreachable \/ Ack
  \/ DropDelivered \/ DropLost \/ LocalDropAndReply \/ OCrash
  \/ \E e \in E : RedropDelivered(e)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The read, as the planner performs it: hot manifests fetched from EVERY   *)
(* live member, merged, deduped by entry id. Each entry visible anywhere     *)
(* contributes its copy of the batch's rows once.                            *)
(***************************************************************************)

Visible(e) == (mO[e] /\ ~crashed) \/ mF[e]

Count == (IF Visible("e1") THEN 1 ELSE 0) + (IF Visible("e2") THEN 1 ELSE 0)

\* An acked batch's rows survive one node loss (ship-before-ack is what
\* absence_tolerance = RF-1 stands on).
AckedDurable == caller = "acked" => Count >= 1

\* A settled system serves an acked batch exactly once — a retry's zombie
\* predecessor is a permanent double count.
NoPermanentDouble == (ENABLED Next) \/ caller # "acked" \/ Count = 1

\* A settled system serves nothing of a batch whose caller was finally told
\* failed — a zombie follower entry is a resurrection.
NoFailedResurrection == (ENABLED Next) \/ caller # "failed" \/ Count = 0

=============================================================================
