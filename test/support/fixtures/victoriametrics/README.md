# vmagent remote write bodies

Two request bodies exactly as vmagent sent them, captured on 2026-09-22 for
PL-70 / T-561. Each file is one `POST /api/v1/write` body; `headers.json`
records the method, path and request headers that came with it.

| file | protocol | Content-Encoding | series | samples |
|---|---|---|---|---|
| `write_zstd.bin` | VictoriaMetrics remote write (vmagent's default) | `zstd` | 835 | 835 |
| `write_snappy.bin` | Prometheus remote write 1.0 (`-remoteWrite.forcePromProto=true`) | `snappy` | 835 | 835 |

Every series is one of vmagent's own metrics from one scrape of itself, with
labels `job="vmagent"` and `instance="127.0.0.1:8429"`.

## Recapturing

- vmagent `v1.152.0`, the non-enterprise `vmutils-linux-arm64-v1.152.0.tar.gz`
  from the GitHub release, binary `vmagent-prod`
  (`vmagent-20260911-125957-tags-v1.152.0-0-g540b91da03`).
- A throwaway Bandit plug on `127.0.0.1:18499` that wrote each request body and
  its headers to files and answered `204`.
- `prom.yml`:

  ```yaml
  global:
    scrape_interval: 1s
  scrape_configs:
    - job_name: vmagent
      static_configs:
        - targets: ["127.0.0.1:8429"]
  ```

- zstd, the default protocol:

  ```sh
  vmagent-prod -httpListenAddr=127.0.0.1:8429 -promscrape.config=prom.yml \
    -remoteWrite.url=http://127.0.0.1:18499/api/v1/write \
    -remoteWrite.tmpDataPath=vmdata1
  ```

- snappy, the same plus `-remoteWrite.forcePromProto=true` and a fresh
  `-remoteWrite.tmpDataPath`.

Each run was stopped after about six seconds, and the first request of each
was kept. A recapture will differ in timestamps and values, and possibly in
which metrics vmagent exposes; the tests assert on shape and on metric names
vmagent has long exposed (`vm_app_version`, `process_cpu_cores_available`).
