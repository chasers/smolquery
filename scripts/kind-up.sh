#!/usr/bin/env bash
# Boots the full smolquery stack in a local kind cluster (Milestone 8 L7).
#
# SMOLQUERY_KIND_OVERLAY picks the topology:
#   kind (default)  — one api/ingest/query/web pod, a 3-pod buffer
#                     StatefulSet, 2 storage pods
#   kind-symmetric  — three identical all-role servers
# Both get Postgres (DuckLake catalog + libcluster discovery) and MinIO as
# the sealed tier.
set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER=smolquery
KCTX="kind-${CLUSTER}"
OVERLAY="${SMOLQUERY_KIND_OVERLAY:-kind}"

case "$OVERLAY" in
  kind) CERT_SPECS=(smolquery-api:1 smolquery-buffer:3 smolquery-storage:2) ;;
  kind-symmetric) CERT_SPECS=(smolquery-server:3) ;;
  *)
    echo "unknown SMOLQUERY_KIND_OVERLAY '$OVERLAY' (expected kind or kind-symmetric)" >&2
    exit 1
    ;;
esac

# Everything writes to the repo-scoped kubeconfig (the file .envrc points
# KUBECONFIG at), never the global ~/.kube/config.
mkdir -p .kube
export KUBECONFIG="$PWD/.kube/config"

# Pin every kubectl below to the local kind context, so a stray ambient
# current-context can never route these applies to another cluster.
kubectl() { command kubectl --context "$KCTX" "$@"; }

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/overlays/kind/kind-config.yaml
else
  kind export kubeconfig --name "$CLUSTER"
fi

echo "==> generating dev TLS certs"
./scripts/gen-dev-certs.sh "deploy/overlays/${OVERLAY}/tls" "${CERT_SPECS[@]}"

echo "==> building image"
docker build -t smolquery:dev .

echo "==> loading image into kind"
kind load docker-image smolquery:dev --name "$CLUSTER"

echo "==> applying manifests ($OVERLAY)"
kubectl apply -k "deploy/overlays/${OVERLAY}"

# The smolquery StatefulSets are the overlay's business — discover rather
# than hard-code them, so both topologies restart what they actually run.
smolquery_sts=$(kubectl -n smolquery get statefulsets -o name | grep /smolquery-)

echo "==> restarting workloads so a reloaded :dev image takes effect"
# shellcheck disable=SC2086
kubectl -n smolquery rollout restart $smolquery_sts

echo "==> waiting for postgres"
kubectl -n smolquery rollout status statefulset/postgres --timeout=180s

echo "==> waiting for minio"
kubectl -n smolquery rollout status deployment/minio --timeout=180s

for sts in $smolquery_sts; do
  echo "==> waiting for $sts"
  kubectl -n smolquery rollout status "$sts" --timeout=300s
done

echo "==> done — API at http://localhost:8080 (Bearer kind-only-api-key), web UI at http://localhost:8082 (smolquery / kind-only-web-password)"
kubectl -n smolquery get pods
