# Qentra Agent — deploy observability to your own cluster

The **Qentra Agent** is a deliberately tiny DaemonSet (≈16 MiB RAM) that turns one
Helm install into **full-cluster observability** — no dashboards to build, no queries
to write. On every node it collects, and ships to Qentra:

1. **Errors** — tails container logs, keeps error lines + **HTTP 4xx/401/404/5xx**, and
   Qentra **auto-clusters, categorizes (Databases / Backend / Network / Infra / HTTP),
   and graphs** them with sub-type drill-downs.
2. **Kubernetes health** — failing / pending pods, CrashLoops, restarts, **OOMKilled**,
   **evicted**, **unschedulable**, missing requests, and **CPU / memory / disk / network
   pressure** per node — as a topology Health Map, Resource Pressure Heatmap, Namespace
   Scoreboard, Restart Constellation, and Risk Radar.
3. **Capacity & cost** — per-pod usage → **resource waste**, **workload DNA**, **node
   aging**, and a **saturation forecast** (days-to-full).
4. **Network / CNI** — per-pod & per-node throughput + errors. **Database inventory**
   (CNPG / PgBouncer / Postgres / Redis / Mongo / …).
5. **Live detail + logs** *(opt-in, audited)* — click any namespace/pod for live
   workloads, **HPA / VPA**, services, ingress, events, and **pod logs** — fetched
   on-demand through a scoped, read-only, Head-of-DevOps-gated command channel.

> One agent. Apple-style liquid gauges, click-through runbooks, and a full cluster
> dashboard that renders the moment the agents are ready — automatically.

Everything ships from **Docker Hub** — public, no login, works for everyone:
- **Helm chart (OCI):** `oci://registry-1.docker.io/qentra/qentra-agent`
- **Image:** `qentra/qentra-deamonsets`
- **Repo (source):** https://github.com/Qentra-on-call/qentra-deamonsets
- **Helm repo (alt):** `https://qentra-on-call.github.io/qentra-deamonsets`

---

## 1. Prerequisites
- A Kubernetes cluster (any: EKS / GKE / AKS / k3s / on-prem) with Helm 3.8+.
- Outbound HTTPS from the cluster to your Qentra URL (e.g. `https://crm.qentra.it.com`).
- A Qentra **agent token** (scope `logs:write`).

## 2. Get your agent token
In Qentra → **Observability → Connect your cluster → Generate token** (Head of
DevOps / CTO / Owner). Copy it — it's shown once.

> Prefer the API? `POST /api/api-tokens` with `{ "name": "Qentra agent", "scopes": ["logs:write"] }`.

## 3. Install with Helm — straight from Docker Hub (recommended)
No `helm repo add`, no registry login, **no URL to set** — the chart and image are
public on Docker Hub and the agent connects to Qentra directly. **You only paste
your token:**
```bash
helm install qentra-agent oci://registry-1.docker.io/qentra/qentra-agent --version 0.5.0 \
  --namespace qentra --create-namespace \
  --set token=<YOUR_LOGS_WRITE_TOKEN> \
  --set clusterName=prod
```
That's it — within a minute, errors appear under **Observability** in Qentra.

> The agent ships to Qentra's hosted API by default (no endpoint to create).
> **Self-hosting Qentra?** Add `--set qentraUrl=https://your-qentra`.

<details><summary>Alternative: classic Helm repo (auto-tracks the latest chart)</summary>

```bash
helm repo add qentra https://qentra-on-call.github.io/qentra-deamonsets
helm repo update
helm install qentra-agent qentra/qentra-agent \
  --namespace qentra --create-namespace --set token=<YOUR_LOGS_WRITE_TOKEN>
```
</details>

### Verify
```bash
kubectl -n qentra get daemonset qentra-agent     # DESIRED == READY (one per node)
kubectl -n qentra logs ds/qentra-agent | tail    # "→ https://… (cluster=prod)"
```

## 4. Configuration (`values.yaml`)
| Key | Default | What |
|-----|---------|------|
| `token` | `""` | **Required.** Org `logs:write` token. (The only thing you must set.) |
| `qentraUrl` | `https://crm.qentra.it.com` | Preset to Qentra's hosted API — leave as-is unless self-hosting. |
| `clusterName` | `default` | Label so you can tell clusters apart. |
| `flushSeconds` | `10` | How often a batch of errors is sent. |
| `logDir` | `/var/log/containers` | Host path of container logs. |
| `resources.requests` | `10m` / `16Mi` | Tiny by design. |
| `resources.limits` | `100m` / `64Mi` | |
| `tolerations` | run everywhere | Includes control-plane nodes. |
| `kubeMetrics.enabled` | `true` | Kubernetes health (pods/nodes). Grants a **read-only** ClusterRole (`get`/`list` on pods, nodes + metrics + `nodes/proxy` for network/disk). Set `false` to disable. |
| `kubeMetrics.intervalSeconds` | `30` | How often node health is reported. |
| `remoteAccess.enabled` | `true` | **On-demand live detail + pod logs.** The agent short-polls Qentra for **scoped, audited, read-only** requests from Head-of-DevOps/owner users (workloads, HPA/VPA, services, ingress, events, pod logs). Set `false` for an operator **kill-switch** — the agent stops polling and the extra RBAC isn't granted. |
| `remoteAccess.pollSeconds` | `4` | How often the agent checks for a request. |
| `image.repository` / `image.tag` | `qentra/qentra-deamonsets` / `0.5` | |

Override on install, e.g. `--set flushSeconds=5`, or pass `-f my-values.yaml`.

> **CPU / memory gauges** need [metrics-server](https://github.com/kubernetes-sigs/metrics-server)
> installed in your cluster. Everything else (failing pods, CrashLoops, restarts,
> OOMKilled, evicted, unschedulable, missing requests) works without it.

## 5. Install without Helm (raw manifests)
```bash
kubectl create namespace qentra
kubectl -n qentra create secret generic qentra-agent --from-literal=token=<YOUR_TOKEN>
helm template qentra-agent qentra/qentra-agent \
  --set token=<YOUR_TOKEN> --set qentraUrl=https://crm.qentra.it.com \
  | kubectl -n qentra apply -f -
```

## 6. Updates & versioning
Images are tagged so you can **follow updates automatically**:

| Tag | Moves? | Use it for |
|-----|--------|-----------|
| `qentra/qentra-deamonsets:0.5.0` | no (immutable) | Pin an exact build for reproducibility. |
| `qentra/qentra-deamonsets:0.5` | yes — patch channel | **Default.** Auto-get patch fixes on a rollout restart. |
| `qentra/qentra-deamonsets:0` | yes — major channel | Track everything within a major version. |
| `qentra/qentra-deamonsets:latest` | yes — newest | Always the latest build. |

The chart defaults to the **`0.5` channel** with `imagePullPolicy: Always`, so:
```bash
kubectl -n qentra rollout restart daemonset/qentra-agent   # pulls the newest 0.5.x
```
pulls the latest patch with no action from us. **Major updates** (which can need a
config change) we announce by **email** — so you're never surprised.

## 7. Upgrade / uninstall
```bash
# Docker Hub (OCI):
helm upgrade qentra-agent oci://registry-1.docker.io/qentra/qentra-agent --version <new> -n qentra --reuse-values
# or, on the classic repo:
helm repo update && helm upgrade qentra-agent qentra/qentra-agent -n qentra --reuse-values

helm uninstall qentra-agent -n qentra
```

## 7. How it works (and what it does NOT do)
- **Reads** container logs **read-only**; runs **non-root** with a **read-only root FS**, all Linux capabilities dropped.
- **Sends only error lines** (ERROR/FATAL/panic/exception/ECONN…) + small per-interval counts — **never your full log stream**, so it's cheap on CPU, network, and your Qentra storage.
- **Kubernetes health** is read-only: each agent `get`/`list`s **only its own node's** pods (`fieldSelector spec.nodeName`) + that node — never the whole cluster, so there's no redundant API load. It reads **status only** — no secrets, no Pod specs' env, no exec, **no cluster mutation**.
- Self-hosting Qentra? Point `qentraUrl` at your own Qentra API.

## 8. Build the image yourself (optional)
```bash
docker build -t qentra/qentra-deamonsets:0.5.0 .
docker push qentra/qentra-deamonsets:0.5.0
```

## 9. Troubleshooting
| Symptom | Fix |
|--------|-----|
| `ingest 401` in agent logs | Bad/empty token → recreate it (scope `logs:write`) and `helm upgrade --set token=…`. |
| No errors in Qentra | Confirm your apps actually log errors; check egress to `qentraUrl`; `kubectl -n qentra logs ds/qentra-agent`. |
| `ingest 403` | Token is missing the `logs:write` scope. |
| Non-default log path (e.g. Docker `json-file`) | `--set logDir=/var/lib/docker/containers`. |
| Kubernetes view empty | RBAC didn't apply (older chart) → `helm upgrade … --version 0.5.0`; confirm `kubectl get clusterrole qentra-agent`. |
| CPU / memory gauges show `—` | Install metrics-server: `kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml`. |

---
MIT-licensed. Built by [Qentra](https://qentra.it.com).
