# Benchmark results

One file per script in `bench/`, holding the last recorded run: the numbers, the
machine they came from, and what they settled. The scripts say *what is measured*;
these files say *what it measured to*.

The exception is `bench/profile.exs`, which settles nothing by itself: it is a
diagnostic, and its output is read at the commit it ran against rather than
recorded here.

A decision made on a benchmark is only re-checkable if the old numbers are still
around to compare against. Re-run a script after a DuckDB, DuckLake, Explorer, or
OTP upgrade — or before revisiting the decision it settled — and overwrite its
file here with the new run.

Every file opens with the same header, because a number without a machine and a
commit is not a measurement:

| | |
|---|---|
| Run | the date |
| Commit | the sha the numbers came from |
| Command | the invocation, including any non-default env knobs |
| Machine | CPU, cores, memory, OS |
| Runtime | Elixir / OTP, scheduler count |

Then the raw tables verbatim — copied, not summarized, so a later run can be
diffed against them — and a closing **What this settles** section tying the
numbers back to the decisions in the tracker (`PL-*` plans, `T-*` tasks) that
depend on them.
