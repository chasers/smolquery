---
name: pr-review
description: >-
  Review a GitHub pull request in this repo and post the findings as PR
  comments in the house style: every comment starts with the robot emoji and
  is written in simple technical English for readers with ADHD (one idea per
  line, the claim first, short words, no nested clauses). Use whenever asked
  to review a PR, post review comments, or comment on a PR. Triggers on:
  "review PR", "review 477", "post review comments", "comment on the PR",
  "leave feedback on the PR".
---

# PR review comments

Reviewing is two steps: find the problems, then say them so a tired reader
gets each one in a glance.

## 1. Find the problems

Run the `code-review` skill on the PR number (`/code-review <n> <level>`;
`high` for anything over a few hundred lines). Wait for its findings. Then
read the PR description and the changed files yourself: the review may miss
what the description promises and the code does not do.

Keep a finding only if you can say what input makes it wrong. Drop style
nits that `mix precommit` already enforces. If a finding is a guess, keep it
and mark it as one (see "Maybe" below); do not drop it and do not state it as
fact.

## 2. Write each comment

One comment per finding, on the line it is about. Shape:

```
🤖 **<The claim, one short sentence. What is wrong, not why.>**

**Where it breaks:** <the input or state, then what happens. One or two lines.>
**Fix:** <the smallest change that closes it. One line. Name the function or option.>
```

Rules, each one load-bearing for a reader with ADHD:

- **Start with `🤖`** so the reader knows who is talking before they read.
- **Claim first, bold.** They decide in one line whether to read on.
- **One idea per line.** A new idea is a new line, not a comma.
- **Short words, plain verbs.** "reads the file twice", not "incurs a redundant
  read". Name things by their names in the code.
- **No nested clauses, no parentheticals, no em-dashes.** Split the sentence.
- **Under about 80 words.** If it needs more, the comment is two findings.
- **A guess is labelled.** Start the claim with `Maybe:` and say what you could
  not check. Never dress a guess as a fact.
- **No praise comments, no restating the diff.** If nothing is wrong, the
  summary says so and that is the whole review.
- **Severity in the claim, not a tag.** "The pod dies when..." says more than
  "P1".

Example:

```
🤖 **A string bound is dropped when every row group cut it, even if one of them was exact.**

**Where it breaks:** a column whose 257-byte values are all identical. Every row group marks `max_is_exact` false, so `max` becomes `nil` and the planner stops pruning on it.
**Fix:** in `exact_string_bounds/2`, keep the bound when `min = max` in the `COPY` stats, since an equal pair cannot have been cut.
```

## 3. Write the summary

One top-level comment, posted with the inline ones:

```
🤖 **Review: <n> findings, <m> of them bugs.** <One sentence on the PR as a whole.>

- `<file>:<line>` <claim of finding 1>
- `<file>:<line>` <claim of finding 2>

Not checked: <what the review did not cover, one line>.
```

With no findings: `🤖 **Review: nothing to fix.** <What was checked, one line.>`

## 4. Post it

Post inline comments and the summary as one review with `event: COMMENT`,
never `APPROVE` or `REQUEST_CHANGES` (the maintainer decides). A line must be
in the PR's diff to take an inline comment; a finding on an unchanged line
goes in the summary with its `file:line` instead.

```sh
# findings.json: [{"path": "lib/x.ex", "line": 42, "body": "🤖 **...**\n\n..."}, ...]
jq -n --rawfile summary summary.md --slurpfile c findings.json \
  '{event: "COMMENT", body: $summary, comments: ($c[0] | map(. + {side: "RIGHT"}))}' \
  > review.json
gh api --method POST "repos/{owner}/{repo}/pulls/<n>/reviews" --input review.json
```

A 422 names the comment whose line is not in the diff: move that one into the
summary and post again.

## 5. Record it

If the PR cites a tracker key (`T-123`), append a one-line note to that task
through the `smolquery-pm` skill: the PR, the head commit, how many findings,
and the serious ones. If the PR is someone else's and cites no key, nothing to
record.
