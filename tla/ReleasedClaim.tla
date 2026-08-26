--------------------------- MODULE ReleasedClaim ---------------------------
(*****************************************************************************)
(* Spec 2: the T-294 released-claim three-gate fence.                        *)
(*                                                                           *)
(* A claim R frozen larger than the current valves is RELEASED by its owner  *)
(* and re-derived as valve-sized claims covering the SAME rows under NEW     *)
(* keys. An in-flight seal attempt for the ORIGINAL released claim must not   *)
(* leave its output segment registered, or the rows count twice.             *)
(*                                                                           *)
(* Three gates (seal.ex) plus compensate_stale are supposed to hold that:    *)
(*   gate1 claim_live before merge                                           *)
(*   gate2 claim_live before register                                        *)
(*   retire key-fence                                                        *)
(*   compensate_stale drops an already-registered orphan on a stale error    *)
(*                                                                           *)
(* BUT: claim_live (seal.ex:177) rejects entries whose sealed_at is truthy,  *)
(* and retire (hot_manifest.ex:660) skips already-sealed ids. So a gate that  *)
(* looks at micro-segments the RE-DERIVED claims already sealed sees them as  *)
(* "reconciliation, skip" and reports :ok. This spec checks whether that lets *)
(* the original attempt register an orphan with no compensation.             *)
(*                                                                           *)
(* Modelling: one claim over two micro-segments m1 (row r1) and m2 (row r2). *)
(* Original claim key "K" covers {r1,r2}; re-derived keys "K1"/"K2" cover     *)
(* {r1}/{r2}. Everything is read at the current snapshot, so a sealed key is  *)
(* counted iff it is registered (Spec 1 already covers snapshot/time-travel   *)
(* and compaction). A released claim is NOT re-signalled, so a crashed        *)
(* original attempt can only restart while ~released (OStart guard).          *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANT AllowCrash   \* FALSE => the original attempt never crashes (pure timing race)
CONSTANT GateFix      \* TRUE => claim_live/retire treat a sealed-under-other-key entry as stale
CONSTANT Reconciler   \* TRUE => a durable sweep drops a released claim's orphan once
                      \*         the re-derived segments cover its rows (independent of the
                      \*         in-flight attempt and of the manifest still holding the entries)
CONSTANT AllowReap    \* FALSE => sealed entries are not reaped (models retire_grace holding
                      \*          the evidence until the original attempt's retire runs)

M    == {"m1", "m2"}
KEYS == {"K", "K1", "K2"}

rowOf   == [m1 |-> "r1", m2 |-> "r2"]
reKeyOf == [m1 |-> "K1", m2 |-> "K2"]
keyRows == [K |-> {"r1", "r2"}, K1 |-> {"r1"}, K2 |-> {"r2"}]

VARIABLES
  cat,       \* [KEYS -> BOOLEAN] : is the sealed key registered (visible now)?
  mp,        \* [M -> BOOLEAN]    : is the micro-segment still in the manifest?
  mclaim,    \* [M -> {"orig","none","re"}] : claim_keys tag. orig=[K] (frozen claim),
             \*   none=[] (released, hot_manifest.ex:587 Entry.claim(_, [])),
             \*   re=[K1]/[K2] (re-derived valve-sized claim)
  msealed,   \* [M -> BOOLEAN]    : sealed_at stamped?
  released,  \* has the original claim been released & re-derived?
  pcO        \* the original attempt's program counter

vars == <<cat, mp, mclaim, msealed, released, pcO>>

\* only ever asked of an orig/re entry (a "none" entry is unclaimed -> Include=TRUE)
claimKeyOf(m) == IF mclaim[m] = "orig" THEN "K" ELSE reKeyOf[m]

TypeOK ==
  /\ cat \in [KEYS -> BOOLEAN]
  /\ mp \in [M -> BOOLEAN]
  /\ mclaim \in [M -> {"orig", "none", "re"}]
  /\ msealed \in [M -> BOOLEAN]
  /\ released \in BOOLEAN
  /\ pcO \in {"idle", "g1", "merged", "registered", "stale", "done", "comp"}

Init ==
  /\ cat = [k \in KEYS |-> FALSE]
  /\ mp = [m \in M |-> TRUE]
  /\ mclaim = [m \in M |-> "orig"]
  /\ msealed = [m \in M |-> FALSE]
  /\ released = FALSE
  /\ pcO = "idle"

(***************************************************************************)
(* claim_live / retire staleness, EXACTLY as coded: an entry is "stale"     *)
(* (would block) only if it is claimed, NOT sealed, and its claim_keys have  *)
(* moved. Sealed entries and entries gone from the manifest are skipped.     *)
(***************************************************************************)
\* Pre-fix staleness: only an UNSEALED entry whose claim moved counts. A sealed
\* entry (even one sealed under a different claim's keys) is skipped.
Stale(ids) == \E m \in ids : mp[m] /\ ~msealed[m] /\ mclaim[m] # "orig"

\* The fix: a present entry whose claim_keys moved off this claim is stale
\* whether or not it is already sealed (seal.ex claim_live + hot_manifest retire).
Moved(m)   == mp[m] /\ mclaim[m] # "orig"
StaleFix(ids) == \E m \in ids : Moved(m)

\* the staleness predicate the gates actually use, under the GateFix switch
StaleEff(ids) == IF GateFix THEN StaleFix(ids) ELSE Stale(ids)

Pending == {m \in M : mp[m] /\ ~msealed[m]}   \* unsealed(ids) in retire

(***************************************************************************)
(* The original attempt: seal claim K over ids M with keys={K}.             *)
(***************************************************************************)

\* level-triggered signal for the ORIGINAL claim: only fires while ~released
OStart ==
  /\ pcO = "idle"
  /\ ~released
  /\ pcO' = "g1"
  /\ UNCHANGED <<cat, mp, mclaim, msealed, released>>

\* gate1: claim_live before merge
OGate1 ==
  /\ pcO = "g1"
  /\ pcO' = IF StaleEff(M) THEN "stale" ELSE "merged"
  /\ UNCHANGED <<cat, mp, mclaim, msealed, released>>

\* gate2 stale: claim_live before register catches a moved claim
OGate2Stale ==
  /\ pcO = "merged"
  /\ StaleEff(M)
  /\ pcO' = "stale"
  /\ UNCHANGED <<cat, mp, mclaim, msealed, released>>

\* gate2 ok: commit -> register K (committed? is false; only this attempt writes K)
OGate2Commit ==
  /\ pcO = "merged"
  /\ ~StaleEff(M)
  /\ ~cat["K"]
  /\ cat' = [cat EXCEPT !["K"] = TRUE]
  /\ pcO' = "registered"
  /\ UNCHANGED <<mp, mclaim, msealed, released>>

\* retire, nothing left to seal and no claim moved -> :ok. Pre-fix this fired the
\* moment nothing was unsealed (blind to a claim sealed under other keys).
ORetireEmpty ==
  /\ pcO = "registered"
  /\ Pending = {}
  /\ ~StaleEff(M)
  /\ pcO' = "done"
  /\ UNCHANGED <<cat, mp, mclaim, msealed, released>>

\* retire fence trips: a named id's claim moved. Pre-fix only unsealed ids count;
\* with the fix a sealed-under-other-key id trips it too.
ORetireStale ==
  /\ pcO = "registered"
  /\ (IF GateFix THEN StaleFix(M) ELSE (Pending # {} /\ \E m \in Pending : mclaim[m] # "orig"))
  /\ pcO' = "stale"
  /\ UNCHANGED <<cat, mp, mclaim, msealed, released>>

\* retire seals the still-original pending ids
ORetireSeal ==
  /\ pcO = "registered"
  /\ Pending # {}
  /\ \A m \in Pending : mclaim[m] = "orig"
  /\ ~StaleEff(M)
  /\ msealed' = [m \in M |-> IF m \in Pending THEN TRUE ELSE msealed[m]]
  /\ pcO' = "done"
  /\ UNCHANGED <<cat, mp, mclaim, released>>

\* compensate_stale: drop an orphan registration of K
OCompensate ==
  /\ pcO = "stale"
  /\ cat' = [cat EXCEPT !["K"] = FALSE]
  /\ pcO' = "comp"
  /\ UNCHANGED <<mp, mclaim, msealed, released>>

\* the monitored seal task crashes; a released claim is never re-signalled,
\* so it can only restart via OStart, which requires ~released.
OCrash ==
  /\ AllowCrash
  /\ pcO \in {"g1", "merged", "registered", "stale"}
  /\ pcO' = "idle"
  /\ UNCHANGED <<cat, mp, mclaim, msealed, released>>

(***************************************************************************)
(* The owner releases the oversized claim and re-derives it (writes the     *)
(* re-derived claim records: the entries now carry the new keys).           *)
(***************************************************************************)
\* Release: still-live (present, unsealed) entries return to pending with an
\* EMPTY claim_keys (hot_manifest.ex:587). Sealed entries keep their tag.
Release ==
  /\ ~released
  /\ \E m \in M : mp[m] /\ ~msealed[m]
  /\ released' = TRUE
  /\ mclaim' = [m \in M |-> IF mp[m] /\ ~msealed[m] THEN "none" ELSE mclaim[m]]
  /\ UNCHANGED <<cat, mp, msealed, pcO>>

\* Reclaim: the top-up re-freezes an unclaimed entry under the valves, stamping
\* the re-derived key. Only then can its valve-sized claim be sealed.
Reclaim(m) ==
  /\ released
  /\ mclaim[m] = "none"
  /\ mp[m]
  /\ ~msealed[m]
  /\ mclaim' = [mclaim EXCEPT ![m] = "re"]
  /\ UNCHANGED <<cat, mp, msealed, released, pcO>>

(***************************************************************************)
(* The re-derived valve-sized claims seal normally: register, retire, reap.  *)
(***************************************************************************)
ReRegister(m) ==
  /\ released
  /\ ~cat[reKeyOf[m]]
  /\ cat' = [cat EXCEPT ![reKeyOf[m]] = TRUE]
  /\ UNCHANGED <<mp, mclaim, msealed, released, pcO>>

ReRetire(m) ==
  /\ cat[reKeyOf[m]]
  /\ mp[m]
  /\ ~msealed[m]
  /\ mclaim[m] = "re"
  /\ msealed' = [msealed EXCEPT ![m] = TRUE]
  /\ UNCHANGED <<cat, mp, mclaim, released, pcO>>

\* The grace reaper deletes a sealed entry from the manifest. AllowReap=FALSE
\* models retire_grace holding the evidence long enough that the original
\* attempt's retire runs before the entry is gone (the realistic window the
\* Elixir regression test exercises).
ReReap(m) ==
  /\ AllowReap
  /\ msealed[m]
  /\ mp[m]
  /\ mp' = [mp EXCEPT ![m] = FALSE]
  /\ UNCHANGED <<cat, mclaim, msealed, released, pcO>>

\* The durable reconciler (proposed complete fix): once a claim is released and
\* every one of its rows is re-committed under a re-derived key, drop the
\* released claim's orphan output K. Durable + independent of the in-flight
\* attempt and of whether the manifest still holds the entries, so it closes the
\* crash-after-register and reap-before-retire residuals the gates cannot.
\* Dropping is safe (no loss): it fires only once the re-derived segments are
\* registered, so the rows are covered in the sealed tier.
Reconcile ==
  /\ Reconciler
  /\ released
  /\ cat["K"]
  /\ \A m \in M : cat[reKeyOf[m]]
  /\ cat' = [cat EXCEPT !["K"] = FALSE]
  /\ UNCHANGED <<mp, mclaim, msealed, released, pcO>>

Next ==
  \/ OStart \/ OGate1 \/ OGate2Stale \/ OGate2Commit
  \/ ORetireEmpty \/ ORetireStale \/ ORetireSeal \/ OCompensate \/ OCrash
  \/ Release \/ Reconcile
  \/ \E m \in M : Reclaim(m) \/ ReRegister(m) \/ ReRetire(m) \/ ReReap(m)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Row accounting, read at the current snapshot.                            *)
(***************************************************************************)
Include(m)  == IF mclaim[m] = "none" THEN TRUE ELSE ~cat[claimKeyOf(m)]
HotM(m)     == IF mp[m] /\ Include(m) THEN 1 ELSE 0
SealedM(m)  == Cardinality({k \in KEYS : rowOf[m] \in keyRows[k] /\ cat[k]})
TotalM(m)   == HotM(m) + SealedM(m)

NoLoss           == \A m \in M : TotalM(m) >= 1
ExactlyOnce      == \A m \in M : TotalM(m) = 1

\* A transient double-count is admitted by the design; a PERMANENT one is not.
\* If a terminal state (no further step possible) violates exactly-once, the
\* fence failed to close the window.
NoPermanentDouble == (ENABLED Next) \/ ExactlyOnce

=============================================================================
