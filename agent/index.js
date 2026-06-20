// Qentra log agent — a deliberately TINY DaemonSet. Pure Node stdlib (no npm
// deps): it stream-tails container logs, keeps only ERROR-ish lines + per-flush
// counts, and ships small gzipped batches to Qentra. Bounded memory, near-zero
// idle cost. Designed to barely register on a node.
//
//   QENTRA_URL     e.g. https://crm.qentra.it.com   (required)
//   QENTRA_TOKEN   ApiToken with scope logs:write    (required)
//   LOG_GLOB       default /var/log/containers       (k8s container log dir)
//   FLUSH_SECONDS  default 10
//   MAX_BATCH      default 500 (drop-oldest beyond this between flushes)
//   CLUSTER_NAME   label stamped on the source
//   HEALTH_PORT    default 8080 (GET /healthz)
//   KUBE_METRICS   default on; set 'false' to disable k8s health collection
//   NODE_NAME      this node (injected via fieldRef); used to scope pod queries
//   CLUSTER_SECONDS default 30 (how often node health is reported)
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import https from 'node:https';
import zlib from 'node:zlib';
import { URL } from 'node:url';

// Defaults to Qentra's hosted API — users connect directly, no URL to create.
// Override QENTRA_URL only when self-hosting Qentra.
const URL_BASE = (process.env.QENTRA_URL || 'https://crm.qentra.it.com').replace(/\/$/, '');
const TOKEN = process.env.QENTRA_TOKEN || '';
const LOG_DIR = process.env.LOG_GLOB || '/var/log/containers';
const FLUSH_MS = (Number(process.env.FLUSH_SECONDS) || 10) * 1000;
const MAX_BATCH = Number(process.env.MAX_BATCH) || 500;
const CLUSTER = process.env.CLUSTER_NAME || 'default';
const HEALTH_PORT = Number(process.env.HEALTH_PORT) || 8080;
const VERSION = '0.5.0';

// Kubernetes health collection (in-cluster, read-only). Each agent reports only
// ITS OWN node (fieldSelector spec.nodeName) so there's no N×-redundant listing.
const KUBE_METRICS = process.env.KUBE_METRICS !== 'false';
const NODE_NAME = process.env.NODE_NAME || '';
const CLUSTER_MS = (Number(process.env.CLUSTER_SECONDS) || 30) * 1000;
const SA_DIR = '/var/run/secrets/kubernetes.io/serviceaccount';
// Cluster-wide pod-usage cache (refreshed ~every 3 min) so each agent reads
// metrics-server sparingly while still reporting real per-pod CPU/memory usage.
const POD_USAGE_TTL_MS = 180_000;
let podUsageCache = { at: 0, map: new Map() };

if (!TOKEN) {
  console.error('[qentra-agent] QENTRA_TOKEN is required (your org logs:write token)');
  process.exit(1);
}

// Only ship lines that look like errors — keeps volume + cost tiny.
const ERROR_RE = /(\bERROR\b|\bFATAL\b|\bCRITICAL\b|\bpanic\b|\bexception\b|\btraceback\b|"level":"error"|level=error|unhandled (promise )?rejection|ECONNREFUSED|ECONNRESET|ETIMEDOUT)/i;
const LEVEL_RE = /\b(ERROR|FATAL|CRITICAL|WARN(?:ING)?|INFO|DEBUG|TRACE)\b/;
// Also capture HTTP 4xx/5xx from access logs (so 401/404/400/5xx are tracked even
// though they're usually INFO level). Pulls the status code from common formats.
const HTTP_RE = /(?:"(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\b[^"]*"\s+|HTTP\/[\d.]+"?\s+|\bstatus(?:_?code)?["':=\s]+)([45]\d\d)\b/i;
function httpStatusOf(line) { const m = HTTP_RE.exec(line); const s = m ? Number(m[1]) : 0; return s >= 400 && s < 600 ? s : 0; }

let batch = [];
const tails = new Map(); // file -> { pos }

// k8s container log filename: <pod>_<namespace>_<container>-<hash>.log
function metaFromFile(file) {
  const base = path.basename(file).replace(/\.log$/, '');
  const m = base.match(/^(.+?)_(.+?)_(.+?)-[0-9a-f]{16,}$/);
  if (m) return { pod: m[1], namespace: m[2], container: m[3], service: m[3] };
  return { pod: null, namespace: null, container: base, service: base };
}

// Extract the human message from a CRI or JSON log line.
function parseLine(raw) {
  let msg = raw;
  let ts = null;
  let level = null;
  // CRI format: "<ts> stdout F <message>"
  const cri = raw.match(/^(\S+)\s+\w+\s+[FP]\s+(.*)$/);
  if (cri) { ts = cri[1]; msg = cri[2]; }
  // JSON line (docker/json-file or app json logs)
  if (msg.startsWith('{')) {
    try {
      const j = JSON.parse(msg);
      msg = j.log ?? j.message ?? j.msg ?? msg;
      ts = j.time ?? j.ts ?? j.timestamp ?? ts;
      level = (j.level ?? j.severity ?? null);
    } catch { /* not json — keep raw */ }
  }
  if (!level) { const lm = String(msg).match(LEVEL_RE); level = lm ? lm[1] : null; }
  return { ts: ts || new Date().toISOString(), level, message: String(msg).slice(0, 4000) };
}

function pushIfError(file, line) {
  const isErr = ERROR_RE.test(line);
  const http = isErr ? 0 : httpStatusOf(line);
  if (!isErr && !http) return;
  const meta = metaFromFile(file);
  const p = parseLine(line);
  batch.push({ ...p, ...meta, ...(http ? { http } : {}) });
  if (batch.length > MAX_BATCH) batch.splice(0, batch.length - MAX_BATCH); // drop oldest
}

// Read new bytes appended to a file since the last position.
function readNew(file) {
  let st;
  try { st = fs.statSync(file); } catch { tails.delete(file); return; }
  const prev = tails.get(file) || { pos: st.size }; // start at EOF on first sight
  if (st.size < prev.pos) prev.pos = 0; // rotated/truncated
  if (st.size === prev.pos) { tails.set(file, prev); return; }
  try {
    const fd = fs.openSync(file, 'r');
    const len = st.size - prev.pos;
    const buf = Buffer.allocUnsafe(Math.min(len, 1 << 20)); // cap 1MB/read
    const n = fs.readSync(fd, buf, 0, buf.length, prev.pos);
    fs.closeSync(fd);
    const text = buf.toString('utf8', 0, n);
    const lines = text.split('\n');
    // keep the trailing partial line for next read
    prev.partial = (prev.partial || '') + lines.shift();
    if (lines.length) { pushIfError(file, prev.partial); prev.partial = lines.pop(); }
    for (const l of lines) if (l) pushIfError(file, l);
    prev.pos += n;
  } catch { /* ignore transient read errors */ }
  tails.set(file, prev);
}

function scan() {
  let files = [];
  try { files = fs.readdirSync(LOG_DIR).filter((f) => f.endsWith('.log')).map((f) => path.join(LOG_DIR, f)); }
  catch { /* dir not present (non-k8s) */ }
  for (const f of files) readNew(f);
}

let backoff = 0;
function flush() {
  if (batch.length === 0) return;
  const logs = batch; batch = [];
  const payload = JSON.stringify({ source: { name: CLUSTER, cluster: CLUSTER, agentVersion: VERSION }, logs });
  const gz = zlib.gzipSync(payload);
  const u = new URL(`${URL_BASE}/api/ingest/logs`);
  const lib = u.protocol === 'http:' ? http : https;
  const req = lib.request(u, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Encoding': 'gzip',
      'Content-Length': gz.length,
      Authorization: `Bearer ${TOKEN}`,
    },
    timeout: 15000,
  }, (res) => {
    res.resume();
    if (res.statusCode >= 200 && res.statusCode < 300) { backoff = 0; }
    else { console.error(`[qentra-agent] ingest ${res.statusCode}`); }
  });
  req.on('error', (e) => {
    console.error('[qentra-agent] ingest error:', e.message);
    // Re-queue (bounded) and back off.
    batch = logs.slice(-MAX_BATCH).concat(batch).slice(0, MAX_BATCH);
    backoff = Math.min(backoff + 1, 6);
  });
  req.on('timeout', () => req.destroy());
  req.end(gz);
}

// ── Kubernetes health (read-only, in-cluster) ───────────────────────────────
// Fire-and-forget gzipped JSON POST (used for cluster snapshots — the next cycle
// re-sends a fresh snapshot, so there's no need to re-queue on failure).
function postJson(apiPath, jsonStr) {
  const gz = zlib.gzipSync(jsonStr);
  const u = new URL(`${URL_BASE}${apiPath}`);
  const lib = u.protocol === 'http:' ? http : https;
  const req = lib.request(u, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Encoding': 'gzip',
      'Content-Length': gz.length,
      Authorization: `Bearer ${TOKEN}`,
    },
    timeout: 15000,
  }, (res) => {
    res.resume();
    if (res.statusCode < 200 || res.statusCode >= 300) console.error(`[qentra-agent] POST ${apiPath} ${res.statusCode}`);
  });
  req.on('error', (e) => console.error(`[qentra-agent] POST ${apiPath} error:`, e.message));
  req.on('timeout', () => req.destroy());
  req.end(gz);
}

function readServiceAccount() {
  try {
    return {
      token: fs.readFileSync(path.join(SA_DIR, 'token'), 'utf8').trim(),
      ca: fs.readFileSync(path.join(SA_DIR, 'ca.crt')),
    };
  } catch { return null; }
}

// GET the k8s API with the mounted ServiceAccount token (resolves the JSON body).
function kubeGet(apiPath, sa) {
  return new Promise((resolve, reject) => {
    const host = process.env.KUBERNETES_SERVICE_HOST || 'kubernetes.default.svc';
    const port = process.env.KUBERNETES_SERVICE_PORT || '443';
    const req = https.request({
      host, port, path: apiPath, method: 'GET', ca: sa.ca,
      headers: { Authorization: `Bearer ${sa.token}`, Accept: 'application/json' },
      timeout: 12000,
    }, (res) => {
      let data = '';
      res.on('data', (c) => { data += c; if (data.length > 8e6) req.destroy(); });
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
        } else reject(new Error(`k8s ${res.statusCode}`));
      });
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('k8s timeout')));
    req.end();
  });
}

// ReplicaSet owner name → approximate the Deployment (strip the trailing hash).
function workloadOf(pod) {
  const o = (pod.metadata?.ownerReferences || [])[0];
  if (!o) return pod.metadata?.name || 'unknown';
  if (o.kind === 'ReplicaSet') return o.name.replace(/-[0-9a-f]{6,10}$/, '');
  return o.name;
}

// Parse a k8s CPU quantity → millicores. "100m"→100, "1"→1000, "1500000n"→1.5.
function cpuToMilli(q) {
  if (!q) return 0;
  const s = String(q);
  if (s.endsWith('n')) return parseInt(s) / 1e6;
  if (s.endsWith('u')) return parseInt(s) / 1e3;
  if (s.endsWith('m')) return parseInt(s);
  return parseFloat(s) * 1000;
}
// Parse a k8s memory quantity → bytes. Handles Ki/Mi/Gi/Ti + k/M/G + plain.
function memToBytes(q) {
  if (!q) return 0;
  const m = String(q).match(/^(\d+(?:\.\d+)?)([A-Za-z]*)$/);
  if (!m) return 0;
  const n = parseFloat(m[1]);
  const u = { '': 1, Ki: 1024, Mi: 1024 ** 2, Gi: 1024 ** 3, Ti: 1024 ** 4, k: 1e3, M: 1e6, G: 1e9, T: 1e12 };
  return n * (u[m[2]] ?? 1);
}

function podHealth(pod) {
  const phase = pod.status?.phase || 'Unknown';
  const cs = pod.status?.containerStatuses || [];
  let restarts = 0, crashloop = false, waiting = null, ready = true, oom = false;
  for (const c of cs) {
    restarts += c.restartCount || 0;
    if (!c.ready) ready = false;
    const w = c.state?.waiting;
    if (w?.reason) {
      waiting = w.reason;
      if (/CrashLoopBackOff|ImagePullBackOff|ErrImagePull|CreateContainer(Config)?Error|RunContainerError/.test(w.reason)) crashloop = true;
    }
    if (c.lastState?.terminated?.reason === 'OOMKilled' || c.state?.terminated?.reason === 'OOMKilled') oom = true;
  }
  // Resource-request hygiene: any container missing CPU/memory requests.
  let noRequests = false, noLimits = false;
  for (const c of (pod.spec?.containers || [])) {
    const r = c.resources || {};
    if (!r.requests?.cpu || !r.requests?.memory) noRequests = true;
    if (!r.limits?.cpu || !r.limits?.memory) noLimits = true;
  }
  // Unschedulable (often "Insufficient cpu/memory").
  let unschedulable = null;
  if (phase === 'Pending') {
    const sc = (pod.status?.conditions || []).find((x) => x.type === 'PodScheduled' && x.status === 'False');
    if (sc?.reason === 'Unschedulable') unschedulable = sc.message || 'Unschedulable';
  }
  const evicted = phase === 'Failed' && pod.status?.reason === 'Evicted';
  return { phase, restarts, crashloop, ready, waiting, oom, noRequests, noLimits, unschedulable, evicted };
}

// Sum a pod's container resource requests + limits (for waste / DNA / forecasting).
function podRequests(pod) {
  let cpuReq = 0, memReq = 0, cpuLim = 0, memLim = 0;
  for (const ct of (pod.spec?.containers || [])) {
    const r = ct.resources || {};
    cpuReq += cpuToMilli(r.requests?.cpu); memReq += memToBytes(r.requests?.memory);
    cpuLim += cpuToMilli(r.limits?.cpu); memLim += memToBytes(r.limits?.memory);
  }
  return { cpuReq, memReq, cpuLim, memLim };
}

// Detect a database workload from its container images (for the DB inventory).
function dbTypeOf(pod) {
  const imgs = (pod.spec?.containers || []).map((c) => c.image || '').join(' ').toLowerCase();
  if (/cloudnative-pg|cnpg|ongres.*operator/.test(imgs)) return 'cnpg';
  if (/pgbouncer|pgcat/.test(imgs)) return 'pgbouncer';
  if (/postgres|postgresql|timescale/.test(imgs)) return 'postgres';
  if (/\bredis\b|valkey|keydb/.test(imgs)) return 'redis';
  if (/mongo/.test(imgs)) return 'mongodb';
  if (/mysql|mariadb|percona/.test(imgs)) return 'mysql';
  if (/cassandra|scylla/.test(imgs)) return 'cassandra';
  if (/elasticsearch|opensearch/.test(imgs)) return 'elasticsearch';
  if (/clickhouse/.test(imgs)) return 'clickhouse';
  return null;
}

// Kubelet stats summary via the API-server proxy (needs nodes/proxy RBAC). Gives
// per-pod + node network rx/tx + errors — the CNI-health signal. Best-effort.
async function nodeNetwork(sa) {
  try {
    const s = await kubeGet(`/api/v1/nodes/${encodeURIComponent(NODE_NAME)}/proxy/stats/summary`, sa);
    const nn = s.node?.network || {};
    const fs = s.node?.fs || {};
    const out = { rxBytes: Number(nn.rxBytes) || 0, txBytes: Number(nn.txBytes) || 0, rxErrors: Number(nn.rxErrors) || 0, txErrors: Number(nn.txErrors) || 0,
      diskUsedBytes: Number(fs.usedBytes) || 0, diskCapacityBytes: Number(fs.capacityBytes) || 0, byNamespace: {}, topPods: [] };
    const pods = [];
    for (const p of (s.pods || [])) {
      const net = p.network; if (!net) continue;
      const ns = p.podRef?.namespace || 'default';
      const rx = Number(net.rxBytes) || 0, tx = Number(net.txBytes) || 0, re = Number(net.rxErrors) || 0, te = Number(net.txErrors) || 0;
      const a = (out.byNamespace[ns] ||= { rxBytes: 0, txBytes: 0, errors: 0 });
      a.rxBytes += rx; a.txBytes += tx; a.errors += re + te;
      pods.push({ pod: p.podRef?.name, namespace: ns, rxBytes: rx, txBytes: tx, errors: re + te });
    }
    out.topPods = pods.sort((a, b) => (b.rxBytes + b.txBytes) - (a.rxBytes + a.txBytes)).slice(0, 15);
    return out;
  } catch { return null; }
}

async function collectCluster() {
  const sa = readServiceAccount();
  if (!sa || !NODE_NAME) return; // not in-cluster, or NODE_NAME not injected
  let pods, node;
  try {
    pods = await kubeGet(`/api/v1/pods?fieldSelector=spec.nodeName=${encodeURIComponent(NODE_NAME)}`, sa);
    node = await kubeGet(`/api/v1/nodes/${encodeURIComponent(NODE_NAME)}`, sa);
  } catch (e) { console.error('[qentra-agent] k8s query:', e.message); return; }

  // Best-effort per-pod usage (true requested-vs-used waste/DNA). metrics-server
  // PodMetrics doesn't honour fieldSelector spec.nodeName, so we pull the list and
  // filter to THIS node's pods by name — gated to every ~3 min via a module cache
  // so we don't hammer metrics-server (each agent refreshes at most 1×/3min).
  const podUsage = new Map(); // 'ns/name' -> { cpuMilli, memBytes }
  try {
    const now = Date.now();
    if (now - podUsageCache.at > POD_USAGE_TTL_MS) {
      const pm = await kubeGet('/apis/metrics.k8s.io/v1beta1/pods', sa);
      const map = new Map();
      for (const it of (pm.items || [])) {
        let cpu = 0, mem = 0;
        for (const ct of (it.containers || [])) { cpu += cpuToMilli(ct.usage?.cpu); mem += memToBytes(ct.usage?.memory); }
        map.set(`${it.metadata?.namespace}/${it.metadata?.name}`, { cpuMilli: cpu, memBytes: mem });
      }
      podUsageCache = { at: now, map };
    }
    // keep only this node's pods from the cached cluster-wide map
    for (const pod of (pods.items || [])) {
      const k = `${pod.metadata?.namespace}/${pod.metadata?.name}`;
      const u = podUsageCache.map.get(k);
      if (u) podUsage.set(k, u);
    }
  } catch { /* per-pod usage unavailable — waste falls back to requests vs allocatable */ }

  const c = { total: 0, running: 0, pending: 0, failed: 0, succeeded: 0, crashloop: 0, notReady: 0, restarts: 0,
    oom: 0, evicted: 0, unschedulable: 0, noRequests: 0, noLimits: 0 };
  const byNamespace = {};
  const nsRes = {}; // ns -> { pods, cpuReq, memReq, cpuLim, memLim, cpuUsed, memUsed }
  const wlRes = {}; // 'ns/workload' -> same + { namespace, workload }
  const problems = [];
  const podList = []; // compact list of ALL this node's pods (for the namespace browser)
  const dbWorkloads = {}; // detected database workloads
  for (const pod of pods.items || []) {
    const ns = pod.metadata?.namespace || 'default';
    const name = pod.metadata?.name || '';
    const h = podHealth(pod);
    c.total++; c.restarts += h.restarts;
    if (h.noRequests) c.noRequests++;
    if (h.noLimits) c.noLimits++;
    const nsAgg = (byNamespace[ns] ||= { pods: 0, failing: 0 });
    nsAgg.pods++;

    // resource accounting (requests/limits from spec + usage from metrics)
    const req = podRequests(pod);
    const use = podUsage.get(`${ns}/${name}`) || { cpuMilli: 0, memBytes: 0 };
    const wl = workloadOf(pod);
    const nr = (nsRes[ns] ||= { pods: 0, cpuReq: 0, memReq: 0, cpuLim: 0, memLim: 0, cpuUsed: 0, memUsed: 0 });
    const wr = (wlRes[`${ns}/${wl}`] ||= { namespace: ns, workload: wl, pods: 0, cpuReq: 0, memReq: 0, cpuLim: 0, memLim: 0, cpuUsed: 0, memUsed: 0 });
    for (const t of [nr, wr]) {
      t.pods++; t.cpuReq += req.cpuReq; t.memReq += req.memReq; t.cpuLim += req.cpuLim; t.memLim += req.memLim;
      t.cpuUsed += use.cpuMilli; t.memUsed += use.memBytes;
    }

    // full pod list (browser) + database detection
    const dbType = dbTypeOf(pod);
    if (podList.length < 200) {
      podList.push({ name, ns, workload: wl, phase: h.phase, ready: h.ready && h.phase === 'Running', restarts: h.restarts,
        cpuUsed: Math.round(use.cpuMilli), memUsed: use.memBytes, db: dbType || undefined });
    }
    if (dbType) {
      const dk = `${ns}/${wl}`;
      const da = (dbWorkloads[dk] ||= { namespace: ns, workload: wl, type: dbType, pods: 0, restarts: 0, cpuUsed: 0, memUsed: 0, cpuReq: 0, memReq: 0, failing: 0 });
      da.pods++; da.restarts += h.restarts; da.cpuUsed += use.cpuMilli; da.memUsed += use.memBytes; da.cpuReq += req.cpuReq; da.memReq += req.memReq;
    }
    let failing = false, reason = null;
    if (h.oom) c.oom++;
    if (h.evicted) { c.evicted++; failing = true; reason = 'Evicted (resource pressure)'; }
    else if (h.unschedulable) { c.unschedulable++; failing = true; reason = h.unschedulable; }
    else if (h.phase === 'Succeeded') c.succeeded++;
    else if (h.phase === 'Failed') { c.failed++; failing = true; reason = h.oom ? 'OOMKilled' : 'Failed'; }
    else if (h.crashloop) { c.crashloop++; failing = true; reason = h.oom ? 'OOMKilled (CrashLoop)' : h.waiting; }
    else if (h.phase === 'Pending') { c.pending++; failing = true; reason = h.waiting || 'Pending'; }
    else if (h.phase === 'Running' && !h.ready) { c.notReady++; failing = true; reason = h.oom ? 'OOMKilled (restarting)' : (h.waiting || 'NotReady'); }
    else if (h.phase === 'Running') { c.running++; if (h.oom) { failing = true; reason = 'OOMKilled (recovered)'; } }
    if (failing) {
      nsAgg.failing++;
      if (problems.length < 60) problems.push({ namespace: ns, pod: name, workload: workloadOf(pod), reason: reason || h.phase, phase: h.phase, restarts: h.restarts });
    }
  }
  const cond = (node.status?.conditions || []).find((x) => x.type === 'Ready');
  const nodeReady = cond?.status === 'True';

  // Node resource pressure: allocatable (from the node) vs usage (metrics-server,
  // optional — skipped cleanly if it isn't installed). Powers CPU/mem gauges and
  // the throttling-risk signal (sustained high CPU = CFS throttling).
  const alloc = node.status?.allocatable || {};
  const res = { cpuAllocMilli: cpuToMilli(alloc.cpu), memAllocBytes: memToBytes(alloc.memory), cpuUsedMilli: null, memUsedBytes: null };
  try {
    const nm = await kubeGet(`/apis/metrics.k8s.io/v1beta1/nodes/${encodeURIComponent(NODE_NAME)}`, sa);
    res.cpuUsedMilli = cpuToMilli(nm.usage?.cpu);
    res.memUsedBytes = memToBytes(nm.usage?.memory);
  } catch { /* metrics-server not present — gauges fall back to allocatable only */ }

  // Node metadata for the aging view (age + kernel/kubelet/OS drift).
  const ni = node.status?.nodeInfo || {};
  const nodeMeta = {
    created: node.metadata?.creationTimestamp || null,
    kernel: ni.kernelVersion || null,
    kubelet: ni.kubeletVersion || null,
    os: ni.osImage || null,
    runtime: ni.containerRuntimeVersion || null,
  };
  // Cap workloads to the heaviest (by requested cpu+mem) to bound the payload.
  const workloads = Object.values(wlRes)
    .sort((a, b) => (b.cpuReq + b.memReq / 1e6) - (a.cpuReq + a.memReq / 1e6)).slice(0, 150);

  const network = await nodeNetwork(sa);
  if (network) { res.diskUsedBytes = network.diskUsedBytes; res.diskCapacityBytes = network.diskCapacityBytes; }

  postJson('/api/ingest/cluster', JSON.stringify({
    cluster: CLUSTER, node: NODE_NAME, agentVersion: VERSION,
    nodeReady, nodeReason: nodeReady ? null : (cond?.reason || 'NotReady'),
    counts: c, byNamespace, problems, resources: res,
    nsResources: nsRes, workloads, nodeMeta, metricsAvailable: podUsage.size > 0,
    podList, databases: Object.values(dbWorkloads), network,
  }));
}

// ── Remote command channel (live resource details + pod logs) ───────────────
// The agent short-polls Qentra for SCOPED, AUDITED, read-only requests and replies
// with the result. Operator kill-switch: set QENTRA_REMOTE=false to disable entirely.
const REMOTE_ENABLED = process.env.QENTRA_REMOTE !== 'false';
const POLL_MS = (Number(process.env.POLL_SECONDS) || 4) * 1000;

function qentraGet(apiPath) {
  return new Promise((resolve) => {
    const u = new URL(`${URL_BASE}${apiPath}`);
    const lib = u.protocol === 'http:' ? http : https;
    const req = lib.request(u, { method: 'GET', headers: { Authorization: `Bearer ${TOKEN}`, Accept: 'application/json' }, timeout: 30000 }, (res) => {
      let d = ''; res.on('data', (c) => { d += c; if (d.length > 2e6) req.destroy(); });
      res.on('end', () => { try { resolve(res.statusCode < 300 ? JSON.parse(d) : null); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null)); req.on('timeout', () => req.destroy()); req.end();
  });
}

// k8s GET returning raw text (for pod logs).
function kubeGetText(apiPath, sa) {
  return new Promise((resolve, reject) => {
    const host = process.env.KUBERNETES_SERVICE_HOST || 'kubernetes.default.svc';
    const port = process.env.KUBERNETES_SERVICE_PORT || '443';
    const req = https.request({ host, port, path: apiPath, method: 'GET', ca: sa.ca, headers: { Authorization: `Bearer ${sa.token}` }, timeout: 15000 }, (res) => {
      let d = ''; res.on('data', (c) => { d += c; if (d.length > 1.5e6) req.destroy(); });
      res.on('end', () => { res.statusCode < 300 ? resolve(d) : reject(new Error('k8s ' + res.statusCode)); });
    });
    req.on('error', reject); req.on('timeout', () => req.destroy(new Error('timeout'))); req.end();
  });
}

async function listNs(sa, apiPath) { try { const r = await kubeGet(apiPath, sa); return r.items || []; } catch { return []; } }

// Live namespace detail — workloads, autoscaling, networking, config, events, pods.
async function handleResources(p) {
  const sa = readServiceAccount(); const ns = p.namespace; if (!sa || !ns) throw new Error('namespace required');
  const e = encodeURIComponent(ns);
  const [deploys, sts, ds, svcs, ings, cms, hpas, vpas, events, pods] = await Promise.all([
    listNs(sa, `/apis/apps/v1/namespaces/${e}/deployments`),
    listNs(sa, `/apis/apps/v1/namespaces/${e}/statefulsets`),
    listNs(sa, `/apis/apps/v1/namespaces/${e}/daemonsets`),
    listNs(sa, `/api/v1/namespaces/${e}/services`),
    listNs(sa, `/apis/networking.k8s.io/v1/namespaces/${e}/ingresses`),
    listNs(sa, `/api/v1/namespaces/${e}/configmaps`),
    listNs(sa, `/apis/autoscaling/v2/namespaces/${e}/horizontalpodautoscalers`),
    listNs(sa, `/apis/autoscaling.k8s.io/v1/namespaces/${e}/verticalpodautoscalers`),
    listNs(sa, `/api/v1/namespaces/${e}/events`),
    listNs(sa, `/api/v1/namespaces/${e}/pods`),
  ]);
  const wl = (o) => ({ name: o.metadata?.name, kind: o.kind || 'Deployment', replicas: o.spec?.replicas ?? null, ready: o.status?.readyReplicas ?? 0, available: o.status?.availableReplicas ?? 0, images: (o.spec?.template?.spec?.containers || []).map((c) => c.image), created: o.metadata?.creationTimestamp });
  return {
    deployments: deploys.map(wl),
    statefulsets: sts.map((o) => ({ ...wl(o), kind: 'StatefulSet' })),
    daemonsets: ds.map((o) => ({ name: o.metadata?.name, kind: 'DaemonSet', desired: o.status?.desiredNumberScheduled, ready: o.status?.numberReady, images: (o.spec?.template?.spec?.containers || []).map((c) => c.image) })),
    services: svcs.map((s) => ({ name: s.metadata?.name, type: s.spec?.type, clusterIP: s.spec?.clusterIP, ports: (s.spec?.ports || []).map((x) => `${x.port}/${x.protocol || 'TCP'}`) })),
    ingresses: ings.map((i) => ({ name: i.metadata?.name, hosts: (i.spec?.rules || []).map((r) => r.host).filter(Boolean), tls: !!(i.spec?.tls || []).length })),
    configmaps: cms.map((c) => ({ name: c.metadata?.name, keys: Object.keys(c.data || {}) })),
    hpa: hpas.map((h) => ({ name: h.metadata?.name, min: h.spec?.minReplicas, max: h.spec?.maxReplicas, current: h.status?.currentReplicas, desired: h.status?.desiredReplicas, targetCPU: (h.spec?.metrics || []).find((m) => m.resource?.name === 'cpu')?.resource?.target?.averageUtilization ?? null })),
    vpa: vpas.map((v) => ({ name: v.metadata?.name, mode: v.spec?.updatePolicy?.updateMode, recommendations: (v.status?.recommendation?.containerRecommendations || []).map((r) => ({ container: r.containerName, cpu: r.target?.cpu, memory: r.target?.memory })) })),
    events: events.sort((a, b) => new Date(b.lastTimestamp || b.metadata?.creationTimestamp) - new Date(a.lastTimestamp || a.metadata?.creationTimestamp)).slice(0, 40)
      .map((ev) => ({ type: ev.type, reason: ev.reason, object: `${ev.involvedObject?.kind}/${ev.involvedObject?.name}`, message: (ev.message || '').slice(0, 300), count: ev.count, time: ev.lastTimestamp || ev.metadata?.creationTimestamp })),
    pods: pods.map((pod) => ({ name: pod.metadata?.name, phase: pod.status?.phase, node: pod.spec?.nodeName, restarts: (pod.status?.containerStatuses || []).reduce((s, c) => s + (c.restartCount || 0), 0), ready: (pod.status?.containerStatuses || []).every((c) => c.ready), containers: (pod.spec?.containers || []).map((c) => c.name) })),
  };
}

async function handleLogs(p) {
  const sa = readServiceAccount(); if (!sa || !p.namespace || !p.pod) throw new Error('namespace + pod required');
  const tail = Math.min(Number(p.tailLines) || 500, 2000);
  const q = `tailLines=${tail}&timestamps=true${p.container ? `&container=${encodeURIComponent(p.container)}` : ''}`;
  const text = await kubeGetText(`/api/v1/namespaces/${encodeURIComponent(p.namespace)}/pods/${encodeURIComponent(p.pod)}/log?${q}`, sa);
  return { logs: text.slice(-200000) };
}

async function runCommand(cmd) {
  if (cmd.kind === 'resources') return handleResources(cmd.params || {});
  if (cmd.kind === 'logs') return handleLogs(cmd.params || {});
  throw new Error(`unsupported command: ${cmd.kind}`);
}

async function pollCommands() {
  const r = await qentraGet('/api/agent/poll');
  const cmd = r && r.command; if (!cmd) return;
  try { const result = await runCommand(cmd); postJson(`/api/agent/result/${cmd.id}`, JSON.stringify({ ok: true, result })); }
  catch (e) { postJson(`/api/agent/result/${cmd.id}`, JSON.stringify({ ok: false, error: String(e.message || e).slice(0, 500) })); }
}

// Health endpoint (1-liner, no framework).
http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(404); res.end();
}).listen(HEALTH_PORT, () => console.log(`[qentra-agent] v${VERSION} → ${URL_BASE} (cluster=${CLUSTER}, node=${NODE_NAME || 'n/a'}, kube=${KUBE_METRICS}, remote=${REMOTE_ENABLED})`));

setInterval(scan, 2000);
setInterval(() => { if (backoff === 0 || Math.random() < 1 / (backoff + 1)) flush(); }, FLUSH_MS);

// k8s health: collect once on startup (so the page fills immediately), then loop.
if (KUBE_METRICS) {
  collectCluster();
  setInterval(collectCluster, CLUSTER_MS);
}

// Remote command channel — short-poll for scoped read requests (logs/resources).
if (REMOTE_ENABLED && KUBE_METRICS) {
  setInterval(pollCommands, POLL_MS);
}
