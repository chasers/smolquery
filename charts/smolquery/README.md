# smolquery Helm chart

This Helm 3 chart installs smolquery 0.13.0 as StatefulSets. It does not
install PostgreSQL, object storage, certificates, or credentials.

## Install

Create the runtime Secret first. It is consumed with `envFrom` and is never
rendered by this chart:

```sh
kubectl create namespace smolquery --dry-run=client -o yaml | kubectl apply -f -
kubectl -n smolquery create secret generic smolquery-env \
  --from-literal=SMOLQUERY_API_KEY=change-me \
  --from-literal=SMOLQUERY_INTERNAL_SECRET=change-me-too \
  --from-literal=CATALOG_DATABASE_URL=postgres://user:password@postgres/smolquery \
  --from-literal=SMOLQUERY_S3_BUCKET=smolquery \
  --from-literal=SMOLQUERY_WEB_USERNAME=smolquery \
  --from-literal=SMOLQUERY_WEB_PASSWORD=change-me-web \
  --from-literal=SMOLQUERY_SECRET_KEY_BASE=smolquery-secret-key-base-01234567890123456789012345678901234567890123456789 \
  --from-literal=RELEASE_COOKIE=smolquery-release-cookie
helm upgrade --install smolquery ./charts/smolquery \
  --namespace smolquery --wait --timeout 10m
```

The default split topology creates `api`, `buffer`, and `storage` StatefulSets.
Use `--set topology=symmetric` for a server StatefulSet whose pods run every
role. All StatefulSets use the shared release-qualified headless Service for
stable peer DNS. The front-door ClusterIP Service selects api in split mode and
server in symmetric mode.

The default image is the released v0.13.0 digest. Set `image.digest` to a
lowercase 64-hex `sha256:` digest when selecting another release; a digest
takes precedence over the tag. `imagePullSecrets`,
resources, scheduling, pod/container security contexts, probes, labels,
annotations, init containers, extra environment, volumes, and mounts are
exposed in `values.yaml`.

## Durable data and TLS

Only split buffer and symmetric server StatefulSets receive a PVC. API and
storage use `emptyDir`; sealed data belongs in the configured object store and
catalog. Configure `persistence.accessModes`, `persistence.size`,
`persistence.storageClass`, and `persistence.annotations` before installation.

TLS is external. Set `tls.enabled=true` and `tls.secretName` to mount a
pre-created Secret containing `ca.pem` and per-pod `<POD_NAME>.pem` and
`<POD_NAME>.key` entries at `/etc/smolquery/gen-rpc-tls`. Enabling chart TLS
also sets `GEN_RPC_TLS=true` and `DIST_TLS=true`; the chart does not issue or
create certificates.

External Secret changes do not alter the pod template. Roll the release or
install a Secret reloader when rotating credentials/configuration.

## Operations

`replicationFactor` must not exceed the active buffer/server replica count.
Replica environment bootstraps `ExpectedNodes` only on first install; later env
changes alone do nothing. Scale up by making new pods live and then applying an
`ExpectedNodes.current`/`resize` CAS. Scale down by draining, CAS-removing
nodes, waiting for propagation, and lowering replicas. Split and symmetric are
not in-place upgrades because identities and PVCs change; use a fresh release
and migration. PVCs retain by default after scale-down or uninstall.

`podOperations.enabled` is off by default. When enabled, set
`serviceAccount.create=true` to create a namespaced Role and RoleBinding that
grant only pod get/list/delete; this can delete any pod in the namespace, so
use a dedicated namespace. Token automount is otherwise disabled. Optional
per-role PDBs are disabled by default so a one-replica API remains deployable.
API and web Service traffic is HTTP and needs operator-provided ingress/gateway
TLS and network isolation. The v0.13 image runs as root and requests lack
universal limits; restricted Pod Security needs a compatible non-root image and
context, and operators should set limits plus aligned engine/pool environment.

Run the installed health check with `helm test smolquery --namespace smolquery`. See
[`docs/deployment.md`](../../docs/deployment.md) and
[`docs/configuration.md`](../../docs/configuration.md) for operational detail.
