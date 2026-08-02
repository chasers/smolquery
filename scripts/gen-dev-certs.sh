#!/usr/bin/env bash
# Generates a dev CA plus per-node gen_rpc/dist TLS certificates for the kind
# overlay (Milestone 8 L7). Certificates carry the pod FQDN (CN + DNS SAN):
# Erlang distribution TLS verifies the peer certificate against the hostname
# part of the node name; gen_rpc (emqx fork) verifies the chain only.
set -euo pipefail

OUT=${1:-deploy/overlays/kind/tls}
REPLICAS=${REPLICAS:-3}
DAYS=${DAYS:-365}

mkdir -p "$OUT"

if [ ! -f "$OUT/ca.pem" ]; then
  openssl genrsa -out "$OUT/ca.key" 2048 2>/dev/null
  openssl req -x509 -new -nodes -key "$OUT/ca.key" -sha256 -days "$DAYS" \
    -subj "/CN=smolquery-dev-ca" -out "$OUT/ca.pem"
fi

for i in $(seq 0 $((REPLICAS - 1))); do
  pod="smolquery-$i"
  fqdn="${pod}.smolquery-headless.smolquery.svc.cluster.local"
  [ -f "$OUT/$pod.pem" ] && continue

  openssl genrsa -out "$OUT/$pod.key" 2048 2>/dev/null
  openssl req -new -key "$OUT/$pod.key" -subj "/CN=$fqdn" -out "$OUT/$pod.csr"
  openssl x509 -req -in "$OUT/$pod.csr" -CA "$OUT/ca.pem" -CAkey "$OUT/ca.key" \
    -CAcreateserial -days "$DAYS" -sha256 -out "$OUT/$pod.pem" \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:%s" "$fqdn" "$pod") 2>/dev/null
  rm -f "$OUT/$pod.csr"
done

echo "certs in $OUT: ca.pem + $(seq -s ', ' -f 'smolquery-%g' 0 $((REPLICAS - 1)))"
