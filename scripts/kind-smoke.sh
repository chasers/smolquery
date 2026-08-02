#!/usr/bin/env bash
# The cluster suite, against a running kind cluster.
#
# The assertions this used to make in bash now live in
# test/smolquery/cluster/kind_cluster_test.exs, tagged :cluster and excluded
# from `mix test` — real assertions, runnable one at a time, and gateable in CI.
# This stays as the one-command path.
#
# Bring the cluster up first with scripts/kind-up.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

# The repo-scoped kubeconfig kind-up.sh writes — works without direnv too.
export KUBECONFIG="$PWD/.kube/config"

exec mix test --only cluster "$@"
