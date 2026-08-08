// Writes the request bodies insert.js posts, once, to files.
//
// Run by k6 itself, so this directory needs exactly one tool:
//
//   mkdir -p /tmp/smolquery-bodies
//   k6 run -e ROWS=3062 -e OUT=/tmp/smolquery-bodies bench/k6/generate.js
//
// It talks to nothing. No HTTP, no application, no build tool — the column list
// comes from schema.json next to this file, as data, and the same file is what
// creates the table over the API, so the rows and the table cannot disagree
// about what the columns are.
//
// Generating is separate from loading on purpose:
//
//   * a body built inside the load loop puts JSON encoding on the critical path
//     of the one process that must not be the bottleneck;
//   * init code runs once per VU, so generating there would build the rows N
//     times and hold N copies of them;
//   * all three shapes come out of the *same* rows, so whatever the smolquery
//     arm and the ClickHouse arm differ by, it is not the data.
//
// Same SEED, same bytes — a run from today and a run from last week compare.
//
// Writes into OUT, with the row count in the name so a stale ROWS cannot go
// unnoticed:
//   columns.N.json    smolquery columnar,  {"rowCount": N, "columns": {...}}
//   rows.N.json       smolquery row-major, {"rows": [...]}
//   eachrow.N.ndjson  ClickHouse JSONEachRow, one object per line
//
// The rows are identical across all three. Timestamps are the one deliberate
// difference: each engine gets the format it parses fastest, same instants.

// open() is init-only, so the schema is read here. The rows themselves are built
// in handleSummary, which runs once in the main context rather than once per VU.
const SCHEMA = JSON.parse(open('./schema.json'));

const ROWS = Number(__ENV.ROWS || 3062);
const PROJECTS = Number(__ENV.PROJECTS || 1000);
const SEED = Number(__ENV.SEED || 42);
const OUT = __ENV.OUT || '/tmp/smolquery-bodies';

// One no-op iteration: k6 wants a scenario to run, this script only wants the
// summary hook at the end of it.
export const options = {
  scenarios: {
    generate: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: '1h' },
  },
};

export default function () {}

const SERVICES = ['checkout-api', 'cart-service', 'payments', 'search', 'inventory'];
const REGIONS = ['us-east-1', 'us-west-2', 'eu-central-1'];
const ZONES = ['a', 'b', 'c'];
const SEVERITIES = [
  [1, 'TRACE'],
  [5, 'DEBUG'],
  [9, 'INFO'],
  [13, 'WARN'],
  [17, 'ERROR'],
];
const ROUTES = ['/v1/checkout', '/v1/cart', '/v1/search', '/v1/inventory', '/v1/pay'];
const METHODS = ['GET', 'POST', 'PUT', 'DELETE'];
const BASE = Date.parse('2026-08-01T10:00:00.000Z');

export function handleSummary() {
  const names = SCHEMA.schema.map((column) => column.name);
  const stamps = SCHEMA.schema.filter((column) => column.type === 'TIMESTAMP').map((column) => column.name);
  const rows = makeRows(names, ROWS, PROJECTS, SEED);

  // The row count is in the file name so insert.js can refuse a stale ROWS
  // without parsing anything: a body regenerated at a different size and posted
  // with yesterday's shell line would scale the headline number by whatever the
  // ratio happens to be, silently.
  const files = {
    [`rows.${rows.length}.json`]: JSON.stringify({ rows: rows }),
    [`columns.${rows.length}.json`]: JSON.stringify({
      rowCount: rows.length,
      columns: toColumns(names, rows),
    }),
    [`eachrow.${rows.length}.ndjson`]: rows.map((row) => JSON.stringify(clickhouse(row, stamps))).join('\n') + '\n',
  };

  const out = { stdout: report(names, rows, files) };

  Object.keys(files).forEach((name) => {
    out[`${OUT}/${name}`] = files[name];
  });

  return out;
}

function makeRows(names, count, projects, seed) {
  const next = mulberry32(seed);
  const refs = projectRefs(projects, next);
  const rows = [];

  for (let i = 0; i < count; i++) {
    const at = new Date(BASE + i);
    const severity = SEVERITIES[i % SEVERITIES.length];
    const service = SERVICES[Math.floor(i / 5) % SERVICES.length];
    const route = ROUTES[i % ROUTES.length];
    const region = REGIONS[i % REGIONS.length];
    const trace = hex(next, 32);
    const span = hex(next, 16);
    const pod = `${service}-${(i % 997).toString(16)}-${(i % 89).toString(16)}`;

    const row = {
      project_id: projectFor(refs, next),
      timestamp: at.toISOString(),
      observed_timestamp: new Date(BASE + i + 2).toISOString(),
      severity_number: severity[0],
      severity_text: severity[1],
      body: `${severity[1]} ${service} handled ${route} 200 in ${(i % 90) / 10}ms req_id=${span} bytes=${i % 4096}`,
      trace_id: trace,
      span_id: span,
      trace_flags: 1,
      dropped_attributes_count: 0,
      service_name: service,
      service_namespace: 'storefront',
      service_version: `2.${i % 5}.${i % 3}`,
      service_instance_id: pod,
      deployment_environment: 'production',
      cloud_provider: 'aws',
      cloud_region: region,
      cloud_availability_zone: region + ZONES[i % ZONES.length],
      cloud_account_id: '9284017263',
      k8s_cluster_name: 'storefront-prod',
      k8s_namespace_name: 'storefront',
      k8s_deployment_name: service,
      k8s_pod_name: pod,
      k8s_pod_uid: hex(next, 32),
      k8s_container_name: service,
      k8s_node_name: `ip-10-${i % 4}-${i % 8}-14.ec2.internal`,
      host_name: `ip-10-${i % 4}-${i % 8}-14`,
      host_arch: 'arm64',
      os_type: 'linux',
      os_version: '6.1.0-amzn2023',
      container_id: hex(next, 64),
      container_image_tag: `sha256-${trace.slice(0, 12)}`,
      telemetry_sdk_name: 'opentelemetry',
      telemetry_sdk_language: 'erlang',
      telemetry_sdk_version: '1.5.0',
      scope_name: `${service}.plug.router`,
      scope_version: '1.20.2',
      code_namespace: 'Storefront.CheckoutApi.Router',
      code_function: 'call/2',
      code_lineno: 40 + (i % 60),
      http_request_method: METHODS[i % METHODS.length],
      http_route: route,
      http_response_status_code: i % 17 === 0 ? 500 : 200,
      http_request_body_size: i % 2048,
      http_response_body_size: i % 8192,
      url_path: route,
      url_scheme: 'https',
      network_protocol_version: '2',
      user_agent_original:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) ' +
        `Chrome/1${i % 10}.0.0.0 Safari/537.36`,
      client_address: `203.0.${i % 256}.${(i * 7) % 256}`,
      server_address: `${service}.storefront.svc.cluster.local`,
      server_port: 4000 + (i % 4),
      duration_ms: Math.round(((i % 500) / 100 + 0.5) * 100) / 100,
      error_type: null,
      exception_type: null,
      exception_message: null,
      exception_stacktrace: null,
      enduser_id: `usr_${span}`,
      session_id: `sess_${trace.slice(0, 24)}`,
      thread_name: `erl_sched_${i % 10}`,
      log_file_path: `/var/log/pods/storefront_${pod}/${service}/0.log`,
      sampled: i % 2 === 0,
    };

    // The schema is the contract. Anything it declares that the template above
    // does not set is sent as null rather than silently missing, so all three
    // shapes always carry the same column set in the same order.
    const shaped = {};

    names.forEach((name) => {
      shaped[name] = name in row ? row[name] : null;
    });

    rows.push(shaped);
  }

  return rows;
}

// Same instants, ClickHouse's own fast path: `2026-08-01 10:00:00.000` parses
// straight into DateTime64(3), where the ISO form with a Z needs
// date_time_input_format=best_effort — the slowest parser it has, on two columns
// of every row of every request. Handing one engine a format it has to adapt to
// while the other gets its native one is not a measurement of either.
function clickhouse(row, stamps) {
  const converted = {};

  Object.keys(row).forEach((name) => {
    converted[name] = row[name];
  });

  stamps.forEach((name) => {
    if (typeof converted[name] === 'string') {
      converted[name] = converted[name].replace('T', ' ').replace('Z', '');
    }
  });

  return converted;
}

function toColumns(names, rows) {
  const columns = {};

  names.forEach((name) => {
    const values = new Array(rows.length);

    for (let i = 0; i < rows.length; i++) values[i] = rows[i][name];

    columns[name] = values;
  });

  return columns;
}

function projectRefs(count, next) {
  const refs = new Array(count);

  for (let i = 0; i < count; i++) {
    let ref = '';

    while (ref.length < 20) ref += 'abcdefghijklmnopqrstuvwxyz'[Math.floor(next() * 26)];

    refs[i] = ref;
  }

  return refs;
}

// Log-uniform tenant skew: a few very large projects and a long thin tail. Real
// multi-tenant traffic is nothing like uniform, and a uniform fixture hides
// exactly what a clustering key exists for — one tenant's rows landing together.
//
// trunc((N+1)^u) - 1 over a uniform u covers every rank including both ends,
// which the obvious trunc(N^u) does not: it can never return rank 0, and rank 0
// is the biggest tenant, the one queries are usually bound to.
function projectFor(refs, next) {
  const rank = Math.trunc(Math.pow(refs.length + 1, next())) - 1;

  return refs[Math.max(0, Math.min(rank, refs.length - 1))];
}

function hex(next, length) {
  let out = '';

  while (out.length < length) {
    out += Math.floor(next() * 0x100000000)
      .toString(16)
      .padStart(8, '0');
  }

  return out.slice(0, length);
}

// mulberry32: a seeded generator, because JS has none. Math.random cannot be
// seeded, and an unseeded fixture means two runs are never comparable.
function mulberry32(seed) {
  let a = seed >>> 0;

  return function () {
    a = (a + 0x6d2b79f5) >>> 0;

    let t = a;

    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);

    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function report(names, rows, files) {
  const lines = ['', `  ${rows.length} rows x ${names.length} columns -> ${OUT}`, ''];

  Object.keys(files).forEach((name) => {
    const mib = (byteLength(files[name]) / 1048576).toFixed(2);

    lines.push(`    ${name.padEnd(18)} ${mib} MiB`);
  });

  lines.push('', `  ROWS=${rows.length} for insert.js`, '');

  return lines.join('\n') + '\n';
}

// The bodies are posted as bytes, so the sizes reported here are bytes, not
// UTF-16 code units. The rows are ASCII today; this stays right if they stop
// being.
function byteLength(text) {
  let bytes = 0;

  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);

    if (code < 0x80) {
      bytes += 1;
    } else if (code < 0x800) {
      bytes += 2;
    } else if (code >= 0xd800 && code < 0xdc00) {
      // A surrogate pair is one 4-byte character, and its low half must not be
      // counted again on the next turn of the loop.
      bytes += 4;
      i++;
    } else {
      bytes += 3;
    }
  }

  return bytes;
}
