// HyperDX 2.41.0's metrics screens against a smolquery ClickHouse edge (T-501).
// Same harness and setup as probe.ts (check out the @hyperdx/app@2.41.0 tag),
// with the stock metric source: the metric catalog and name search, a
// metric's metadata and attributes, and every chart shape
// translateMetricChartConfig and the multi-series composer emit for gauge,
// sum and histogram metrics. It writes the statements it sent to
// CORPUS_OUT (default metrics_corpus.json); build_fixture in
// test/support/fixtures/clickstack/hyperdx_metrics.json came from one.
//
// It expects the three otel_metrics_* tables of docs/clickstack.md section 6,
// with rows from the last hour: `elixir seed_metrics.exs` writes them as
// NDJSON and `seed_metrics.sh` creates the tables and loads them.
// Last run: hyperdxio/hyperdx @ d980899 (2.41.0), 2026-10-15, 35 of 36 OK;
// the exponential histogram's quantile names an alias before defining it.
import { ClickhouseClient } from '@/clickhouse/node';
import { getMetadata } from '@/core/metadata';
import { renderChartConfig } from '@/core/renderChartConfig';
import * as fs from 'fs';

const host = process.env.SMOL_HOST ?? 'http://localhost:18123';
const client = new ClickhouseClient({ host, username: 'default', password: process.env.SMOL_PASSWORD ?? 'smolquery-dev' });
const metadata = getMetadata(client);
const connectionId = 'smolquery';
const databaseName = 'default';
const metricTables: any = {
  gauge: 'otel_metrics_gauge',
  histogram: 'otel_metrics_histogram',
  sum: 'otel_metrics_sum',
  summary: 'otel_metrics_summary',
  'exponential histogram': 'otel_metrics_exponential_histogram',
};

const end = new Date();
const start = new Date(end.getTime() - 60 * 60 * 1000);
const results: [string, string][] = [];
const failures: [string, string][] = [];
const corpus: any[] = [];

async function step(name: string, fn: () => Promise<unknown>) {
  try {
    const out = await fn();
    const text = typeof out === 'string' ? out : JSON.stringify(out);
    results.push([name, 'OK   ' + text.slice(0, 400)]);
  } catch (e: any) {
    results.push([name, 'FAIL ' + String(e?.message ?? e).slice(0, 700)]);
    failures.push([name, String(e?.message ?? e)]);
  }
}

async function raw(name: string, sql: string, params: any, format: string = 'JSON') {
  corpus.push({ step: name, sql, params, format });
  const rs = await client.query({ query: sql, query_params: params, format, connectionId } as any);
  return rs.text();
}

async function chart(name: string, config: any, format: string = 'JSON') {
  const chSql = await renderChartConfig(config, metadata, undefined);
  corpus.push({ step: name, sql: chSql.sql, params: chSql.params, format });
  const rs = await client.query({ query: chSql.sql, query_params: chSql.params, format, connectionId } as any);
  const body = await rs.text();
  const json = JSON.parse(body);
  return `meta=${json.meta.map((m: any) => `${m.name}:${m.type}`).join('|')} rows=${json.rows} first=${JSON.stringify(json.data[0])} last=${JSON.stringify(json.data[json.data.length - 1])}`;
}

function base(extra: any = {}) {
  return {
    from: { databaseName, tableName: '' },
    connection: connectionId,
    metricTables,
    timestampValueExpression: 'TimeUnix',
    dateRange: [start, end],
    where: '',
    whereLanguage: 'lucene',
    granularity: '5 minute',
    ...extra,
  };
}

function metric(aggFn: string | undefined, metricType: string, metricName: string, extra: any = {}) {
  return { aggFn, metricType, metricName, valueExpression: 'Value', aggCondition: '', aggConditionLanguage: 'lucene', ...extra };
}

async function main() {
  for (const table of Object.values(metricTables) as string[]) {
    await step(`getColumns(${table})`, async () => (await metadata.getColumns({ databaseName, tableName: table, connectionId })).map((c: any) => `${c.name}:${c.type}`).join(', '));
  }
  await step('getTableMetadata(gauge)', async () => { const t: any = await metadata.getTableMetadata({ databaseName, tableName: metricTables.gauge, connectionId }); return { engine: t.engine, sorting_key: t.sorting_key }; });

  await step('getMetricNames(gauge, "cpu")', async () => metadata.getMetricNames({ databaseName, tableName: metricTables.gauge, connectionId, dateRange: [start, end], timestampValueExpression: 'TimeUnix', namePattern: 'cpu' } as any));
  await step('getMetricNames(sum)', async () => metadata.getMetricNames({ databaseName, tableName: metricTables.sum, connectionId, dateRange: [start, end], timestampValueExpression: 'TimeUnix' } as any));

  for (const [kind, table] of Object.entries(metricTables) as [string, string][]) {
    await step(`catalog(${kind})`, async () =>
      raw(`catalog_${kind.replace(' ', '_')}`,
        `SELECT MetricName, anyLast(MetricUnit) AS MetricUnit, anyLast(MetricDescription) AS MetricDescription FROM {HYPERDX_PARAM_DB:Identifier}.{HYPERDX_PARAM_T:Identifier} WHERE {HYPERDX_PARAM_TS:Identifier} >= fromUnixTimestamp64Milli({HYPERDX_PARAM_S:Int64}) AND {HYPERDX_PARAM_TS:Identifier} <= fromUnixTimestamp64Milli({HYPERDX_PARAM_E:Int64}) GROUP BY MetricName ORDER BY MetricName ASC LIMIT {HYPERDX_PARAM_L:Int32}`,
        { HYPERDX_PARAM_DB: databaseName, HYPERDX_PARAM_T: table, HYPERDX_PARAM_TS: 'TimeUnix', HYPERDX_PARAM_S: start.getTime(), HYPERDX_PARAM_E: end.getTime(), HYPERDX_PARAM_L: 1000 }));
  }

  await step('metric metadata (unit, description)', async () =>
    raw('metric_metadata', `SELECT MetricUnit, MetricDescription FROM {HYPERDX_PARAM_DB:Identifier}.{HYPERDX_PARAM_T:Identifier} WHERE MetricName = {HYPERDX_PARAM_N:String} LIMIT 1`,
      { HYPERDX_PARAM_DB: databaseName, HYPERDX_PARAM_T: metricTables.gauge, HYPERDX_PARAM_N: 'system.cpu.utilization' }));
  await step('metric attributes (distinct maps)', async () =>
    raw('metric_attributes', `SELECT DISTINCT ScopeAttributes, ResourceAttributes, Attributes FROM {HYPERDX_PARAM_DB:Identifier}.{HYPERDX_PARAM_T:Identifier} WHERE MetricName={HYPERDX_PARAM_N:String} LIMIT {HYPERDX_PARAM_L:Int32}`,
      { HYPERDX_PARAM_DB: databaseName, HYPERDX_PARAM_T: metricTables.gauge, HYPERDX_PARAM_N: 'system.cpu.utilization', HYPERDX_PARAM_L: 1000 }));
  await step('metric attribute values', async () =>
    raw('metric_attribute_values', `SELECT DISTINCT Attributes[{HYPERDX_PARAM_K:String}] as value FROM {HYPERDX_PARAM_DB:Identifier}.{HYPERDX_PARAM_T:Identifier} WHERE MetricName = {HYPERDX_PARAM_N:String} AND Attributes[{HYPERDX_PARAM_K:String}] != '' AND Attributes[{HYPERDX_PARAM_K:String}] ILIKE {HYPERDX_PARAM_P:String} ORDER BY value LIMIT {HYPERDX_PARAM_L:Int32}`,
      { HYPERDX_PARAM_DB: databaseName, HYPERDX_PARAM_T: metricTables.gauge, HYPERDX_PARAM_N: 'system.cpu.utilization', HYPERDX_PARAM_K: 'cpu', HYPERDX_PARAM_P: '%', HYPERDX_PARAM_L: 100 }));

  await step('gauge: avg by ServiceName, 5 minute', async () => chart('gauge_avg_by_service', base({ select: [metric('avg', 'gauge', 'system.cpu.utilization')], groupBy: 'ServiceName' })));
  await step('gauge: max, number chart', async () => chart('gauge_max_number', base({ select: [metric('max', 'gauge', 'system.cpu.utilization')], granularity: undefined, dateRange: [start, end] })));
  await step('gauge: avg where cpu:0 (lucene aggCondition)', async () => chart('gauge_avg_filtered', base({ select: [metric('avg', 'gauge', 'system.cpu.utilization', { aggCondition: 'Attributes.cpu:0' })], where: 'ServiceName:api' })));
  await step('gauge: delta (isDelta)', async () => chart('gauge_delta', base({ select: [metric('avg', 'gauge', 'process.runtime.memory', { isDelta: true })], groupBy: 'ServiceName' })));
  await step('gauge: group by attribute key', async () => chart('gauge_by_attribute', base({ select: [metric('avg', 'gauge', 'system.cpu.utilization')], groupBy: "Attributes['cpu']" })));

  await step('sum: sum (rate) by ServiceName', async () => chart('sum_rate_by_service', base({ select: [metric('sum', 'sum', 'http.server.request.count')], groupBy: 'ServiceName' })));
  await step('sum: increase by ServiceName (TopGroups)', async () => chart('sum_increase_by_service', base({ select: [metric('increase', 'sum', 'http.server.request.count')], groupBy: 'ServiceName' })));
  await step('sum: increase, number chart', async () => chart('sum_increase_number', base({ select: [metric('increase', 'sum', 'http.server.request.count')], granularity: undefined })));
  await step('sum: last_value(Sum) without aggFn', async () => chart('sum_last_value', base({ select: [metric(undefined, 'sum', 'http.server.request.count')], groupBy: 'ServiceName' })));
  await step('sum: delta temporality counter', async () => chart('sum_delta_temporality', base({ select: [metric('sum', 'sum', 'queue.enqueued')] })));

  await step('histogram: p95 by ServiceName', async () => chart('histogram_p95_by_service', base({ select: [metric('quantile', 'histogram', 'http.server.duration', { level: 0.95 })], groupBy: 'ServiceName' })));
  await step('histogram: p50, number chart', async () => chart('histogram_p50_number', base({ select: [metric('quantile', 'histogram', 'http.server.duration', { level: 0.5 })], granularity: undefined })));
  await step('histogram: count, 5 minute', async () => chart('histogram_count', base({ select: [metric('count', 'histogram', 'http.server.duration')] })));
  await step('histogram: count by ServiceName', async () => chart('histogram_count_by_service', base({ select: [metric('count', 'histogram', 'http.server.duration')], groupBy: 'ServiceName' })));

  await step('multi: gauge avg + sum rate (pivot)', async () => chart('multi_gauge_sum', base({ select: [metric('avg', 'gauge', 'system.cpu.utilization', { alias: 'cpu' }), metric('sum', 'sum', 'http.server.request.count', { alias: 'reqs' })], groupBy: 'ServiceName' })));
  await step('multi: ratio of two sums', async () => chart('multi_ratio', base({ select: [metric('sum', 'sum', 'http.server.request.count', { aggCondition: 'Attributes.status:500', aggConditionLanguage: 'lucene' }), metric('sum', 'sum', 'http.server.request.count')], seriesReturnType: 'ratio' })));
  await step('multi: formula A / B * 100', async () => chart('multi_formula', base({ select: [metric('sum', 'sum', 'http.server.request.count', { aggCondition: 'Attributes.status:500' }), metric('sum', 'sum', 'http.server.request.count')], formulas: [{ expression: 'A / B * 100', alias: 'error_pct' }] })));
  await step('multi: gauge + histogram p95 (packed group)', async () => chart('multi_gauge_histogram', base({ select: [metric('avg', 'gauge', 'system.cpu.utilization'), metric('quantile', 'histogram', 'http.server.duration', { level: 0.95 })], groupBy: 'ServiceName' })));

  await step('exponential histogram: p95 (expected unsupported)', async () => chart('exp_histogram_p95', base({ select: [metric('quantile', 'exponential histogram', 'rpc.client.duration', { level: 0.95 })] })));
  await step('exponential histogram: count (expected unsupported)', async () => chart('exp_histogram_count', base({ select: [metric('count', 'exponential histogram', 'rpc.client.duration')] })));

  for (const [name, text] of results) console.log(`${text.startsWith('OK') ? '✅' : '❌'} ${name}\n   ${text}`);
  console.log(`\n${results.length - failures.length}/${results.length} answered`);
  fs.writeFileSync(process.env.CORPUS_OUT ?? 'metrics_corpus.json', JSON.stringify(corpus, null, 2));
}

main().catch(e => { console.error(e); process.exit(1); });
