#!/usr/bin/env bash
# Boots the full smolquery stack in a local kind cluster (Milestone 8 L7):
# one api/ingest/query/web pod, a 3-pod buffer StatefulSet, 2 storage pods,
# Postgres (DuckLake catalog + libcluster discovery), and MinIO as the
# sealed tier.
set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER=smolquery
KCTX="kind-${CLUSTER}"

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
./scripts/gen-dev-certs.sh deploy/overlays/kind/tls

echo "==> building image"
docker build -t smolquery:dev .

echo "==> loading image into kind"
kind load docker-image smolquery:dev --name "$CLUSTER"

echo "==> applying manifests"
kubectl apply -k deploy/overlays/kind

echo "==> restarting workloads so a reloaded :dev image takes effect"
kubectl -n smolquery rollout restart \
  statefulset/smolquery-api statefulset/smolquery-buffer statefulset/smolquery-storage

echo "==> waiting for postgres"
kubectl -n smolquery rollout status statefulset/postgres --timeout=180s

echo "==> waiting for minio"
kubectl -n smolquery rollout status deployment/minio --timeout=180s

for sts in smolquery-buffer smolquery-storage smolquery-api; do
  echo "==> waiting for $sts"
  kubectl -n smolquery rollout status "statefulset/$sts" --timeout=300s
done

echo "==> done — API at http://localhost:8080 (Bearer kind-only-api-key), web UI at http://localhost:8082"
kubectl -n smolquery get pods
