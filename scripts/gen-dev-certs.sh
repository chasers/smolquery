#!/usr/bin/env bash
# Generates a dev CA plus per-pod gen_rpc/dist TLS certificates for the kind
# overlay (Milestone 8 L7). Certificates carry the pod FQDN as a DNS SAN —
# Erlang distribution TLS verifies the peer certificate against the hostname
# part of the node name, and SANs are what hostname verification consults
# (the CN holds only the short pod name: CN caps at 64 bytes, which the pod
# FQDNs exceed). gen_rpc (emqx fork) verifies the chain only.
#
# Usage: gen-dev-certs.sh [OUT_DIR] [pod-prefix:count ...]
# Defaults match deploy/overlays/kind: one api pod, three buffer, two storage,
# all behind the one smolquery-headless service.
set -euo pipefail

OUT=${1:-deploy/overlays/kind/tls}
shift $(($# > 0 ? 1 : 0))
SPECS=("${@:-}")
[ -z "${SPECS[0]:-}" ] && SPECS=(smolquery-api:1 smolquery-buffer:3 smolquery-storage:2)

HEADLESS=${HEADLESS:-smolquery-headless}
NAMESPACE=${NAMESPACE:-smolquery}
DAYS=${DAYS:-365}

mkdir -p "$OUT"

if [ ! -f "$OUT/ca.pem" ]; then
  openssl genrsa -out "$OUT/ca.key" 2048 2>/dev/null
  openssl req -x509 -new -nodes -key "$OUT/ca.key" -sha256 -days "$DAYS" \
    -subj "/CN=smolquery-dev-ca" -out "$OUT/ca.pem"
fi

pods=()
for spec in "${SPECS[@]}"; do
  prefix=${spec%%:*}
  count=${spec##*:}
  for i in $(seq 0 $((count - 1))); do
    pod="${prefix}-${i}"
    pods+=("$pod")
    fqdn="${pod}.${HEADLESS}.${NAMESPACE}.svc.cluster.local"
    [ -f "$OUT/$pod.pem" ] && continue

    openssl genrsa -out "$OUT/$pod.key" 2048 2>/dev/null
    openssl req -new -key "$OUT/$pod.key" -subj "/CN=$pod" -out "$OUT/$pod.csr"
    openssl x509 -req -in "$OUT/$pod.csr" -CA "$OUT/ca.pem" -CAkey "$OUT/ca.key" \
      -CAcreateserial -days "$DAYS" -sha256 -out "$OUT/$pod.pem" \
      -extfile <(printf "subjectAltName=DNS:%s,DNS:%s" "$fqdn" "$pod") 2>/dev/null
    rm -f "$OUT/$pod.csr"
  done
done

echo "certs in $OUT: ca.pem + ${pods[*]}"
