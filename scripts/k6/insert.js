// Posts a body at a URL, as fast as it is told to, and reports rows per second.
//
// It knows nothing about what is on the other end. No schema, no config, no
// process to boot — a URL, a file, some headers. That is deliberate: a load
// generator that imports the thing it measures is competing with it for CPU and
// can no longer tell saturation from its own overhead, and it can only ever
// measure the one system it was built out of.
//
// So the same script drives smolquery and ClickHouse, and the only thing that
// differs is the URL, the headers, and which file holds the body.
//
// Everything it cannot infer, it is told. It never parses the body: ROWS is how
// many rows that file contains, because counting them would cost more per
// iteration than the request does, and would have to understand two different
// wire formats to do it.
//
//   k6 run -e URL=... -e BODY=... -e ROWS=... scripts/k6/insert.js
//
// See README.md for both back ends and what the numbers mean.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Read at init, once per VU, never per iteration: re-reading the file inside the
// loop would put disk and string allocation inside the measurement.
//
// Binary, not text. A string body is UTF-8 encoded on every single post — 6.4
// MiB of encoding per request, in the process whose whole job is to not be the
// bottleneck. An ArrayBuffer goes to the socket as it sits in memory.
const BODY = open(__ENV.BODY, 'b');

const URL = __ENV.URL;
const ROWS = Number(__ENV.ROWS || 0);
const EXPECT = Number(__ENV.EXPECT_STATUS || 200);
const DURATION = __ENV.DURATION || '30s';
const VUS = Number(__ENV.VUS || 4);
const MODE = __ENV.MODE || 'vus';
const RATE = Number(__ENV.RATE || 0);
const CONTENT_TYPE = __ENV.CONTENT_TYPE || 'application/json';

if (!URL) throw new Error('URL is required');
if (!ROWS) throw new Error('ROWS is required — how many rows the body holds');

// generate.js puts the row count in the file name, so a stale ROWS from an older
// shell line cannot silently scale the headline number. Nothing is parsed; this
// is the name, not the contents.
const named = /[._](\d+)\.[a-z]+$/.exec(__ENV.BODY);

if (named && Number(named[1]) !== ROWS) {
  throw new Error(`ROWS=${ROWS} but ${__ENV.BODY} was generated with ${named[1]} rows`);
}

const rows = new Counter('rows');
const accepted = new Counter('accepted_requests');
const refused = new Counter('refused_requests');
const insertDuration = new Trend('insert_duration', true);
const refusedDuration = new Trend('refused_duration', true);

const headers = buildHeaders();

// Two load models, and the difference decides what the run can answer.
//
//   vus  — a closed loop: VUS clients each post, wait for the reply, post again.
//          The server never sees more than VUS in flight, so the run measures
//          latency at a fixed concurrency. Throughput is a consequence of it.
//
//   rate — an open loop: requests start every 1/RATE seconds whether or not the
//          previous ones finished. This is the one that finds saturation — hold
//          the arrival rate above what the server can absorb and the queue grows
//          without the load generator politely slowing down to hide it.
//
// A closed loop cannot find a saturation point, only a knee, and the knee moves
// with VUS. Use rate when the question is capacity.

// Which percentiles the summary carries. k6 computes a default set that has
// neither p(50) nor p(99), and a Trend cannot be asked for them after the fact.
const summaryTrendStats = ['med', 'p(95)', 'p(99)', 'max'];

// The body is held per VU, so VU count is also a memory budget: at 6.4 MiB a
// piece, a few hundred VUs is gigabytes on a machine that is simultaneously
// running the server under test. Swapping mid-run degrades both sides at once
// and leaves no mark in the output, so the ceiling is explicit and modest —
// raise MAX_VUS deliberately, having checked there is RAM for it.
const preAllocatedVUs = Number(__ENV.PRE_VUS || Math.max(VUS, RATE));
const maxVUs = Number(__ENV.MAX_VUS || Math.max(preAllocatedVUs, RATE * 2));

// Iterations still running when the clock expires are given this long to finish.
// It lands in the denominator of rows/s, so it is small and reported rather than
// k6's silent 30s default: a drain at falling concurrency is not steady state,
// and at overload the default can be most of the run.
const gracefulStop = __ENV.GRACEFUL_STOP || '5s';

export const options = {
  discardResponseBodies: true,
  summaryTrendStats,
  scenarios:
    MODE === 'rate'
      ? {
          insert: {
            executor: 'constant-arrival-rate',
            rate: RATE,
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs,
            maxVUs,
            gracefulStop,
          },
        }
      : {
          insert: {
            executor: 'constant-vus',
            vus: VUS,
            duration: DURATION,
            gracefulStop,
          },
        },
};

export default function () {
  const response = http.post(URL, BODY, { headers, timeout: __ENV.TIMEOUT || '120s' });

  const ok = check(response, {
    'expected status': (r) => r.status === EXPECT,
  });

  // Only accepted requests count, for rows and for latency alike. A refusal is
  // either instant (connection refused, status 0, ~0 ms) or a timeout (120 s);
  // mixed into the same Trend, the first pulls the percentiles down and makes an
  // overloaded server look fast, the second inflates p99 with time nobody waited
  // for data. They get their own Trend so the failure is visible, not blended.
  if (ok) {
    insertDuration.add(response.timings.duration);
    rows.add(ROWS);
    accepted.add(1);
  } else {
    refusedDuration.add(response.timings.duration);
    refused.add(1);
  }
}

function buildHeaders() {
  const built = { 'Content-Type': CONTENT_TYPE };

  if (__ENV.AUTH) built['Authorization'] = __ENV.AUTH;

  // HEADERS is a JSON object, for anything else a back end needs — ClickHouse
  // settings, a tenant id, a trace header.
  if (__ENV.HEADERS) Object.assign(built, JSON.parse(__ENV.HEADERS));

  return built;
}

export function handleSummary(data) {
  const seconds = data.state.testRunDurationMs / 1000;
  const planned = parseDuration(DURATION);
  const total = count(data, 'rows');
  const ok = count(data, 'accepted_requests');
  const bad = count(data, 'refused_requests');
  const dropped = count(data, 'dropped_iterations');
  const latency = data.metrics.insert_duration && data.metrics.insert_duration.values;

  const report = [
    '',
    `  url          ${URL}`,
    `  mode         ${MODE === 'rate' ? `${RATE}/s arrival, ${DURATION}` : `${VUS} VUs closed loop, ${DURATION}`}`,
    `  body         ${(BODY.byteLength / 1048576).toFixed(2)} MiB, ${ROWS} rows`,
    `  window       ${seconds.toFixed(2)}s measured`,
    '',
    `  rows/s       ${Math.round(total / seconds)}`,
    `  requests     ${ok} accepted, ${bad} refused, ${dropped} dropped`,
    latency
      ? `  latency ms   p50 ${ms(latency, 'med')}   p95 ${ms(latency, 'p(95)')}   p99 ${ms(latency, 'p(99)')}   max ${ms(latency, 'max')}`
      : '',
    '',
    // Three ways this run is not the number it looks like. Each is stated here
    // rather than left to be noticed in the counts, because the summary is what
    // gets pasted into a results document and the console warnings are not.
    bad > 0 ? `  ${bad} request(s) refused — rows/s above is what got through, not capacity` : '',
    dropped > 0
      ? `  ${dropped} iteration(s) dropped — k6 could not keep the arrival rate, so the offered load was below ${RATE}/s; raise MAX_VUS or read this as saturation`
      : '',
    // The drain runs at falling concurrency, so a long one means the tail of the
    // run was not steady state and rows/s is understated by roughly its share.
    planned && seconds > planned * 1.05
      ? `  drain was ${(seconds - planned).toFixed(2)}s of the ${seconds.toFixed(2)}s window — rows/s is understated`
      : '',
    '',
  ];

  return {
    stdout: report.filter((line) => line !== '').join('\n') + '\n',
    [__ENV.JSON_OUT || '/dev/null']: JSON.stringify(data),
  };
}

// A stat that was not computed prints as a dash rather than crashing the
// summary and taking the whole run's output with it.
function ms(values, key) {
  return typeof values[key] === 'number' ? values[key].toFixed(1) : '-';
}

function count(data, name) {
  return data.metrics[name] ? data.metrics[name].values.count : 0;
}

function parseDuration(text) {
  const parsed = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(text);
  const units = { ms: 0.001, s: 1, m: 60, h: 3600 };

  return parsed ? Number(parsed[1]) * units[parsed[2]] : null;
}
