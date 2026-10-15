// HyperDX 2.41.0's trace screens against a smolquery ClickHouse edge (T-501).
// Same harness and setup as probe.ts (check out the @hyperdx/app@2.41.0 tag),
// with a trace source: the Search page in trace mode, the filters sidebar,
// the trace waterfall, a latency chart, and the service map's SQL as
// useServiceMap composes it. It expects the `default.otel_traces` table of
// docs/clickstack.md section 5, with rows from the last hour.
// Last run: hyperdxio/hyperdx @ d980899 (2.41.0), 2026-10-14, every step OK.
import { ClickhouseClient } from '@/clickhouse/node';
import { getMetadata } from '@/core/metadata';
import { renderChartConfig } from '@/core/renderChartConfig';
import { buildSearchChartConfig } from '@/core/searchChartConfig';
import SqlString from 'sqlstring';

const host = process.env.SMOL_HOST ?? 'http://localhost:18123';
const client = new ClickhouseClient({ host, username: 'default', password: process.env.SMOL_PASSWORD ?? 'smolquery-dev' });
const metadata = getMetadata(client);
const connectionId = 'smolquery';
const databaseName = 'default';
const tableName = 'otel_traces';

const source: any = {
  id: 'traces', name: 'Traces', kind: 'trace', connection: connectionId,
  from: { databaseName, tableName },
  timestampValueExpression: 'Timestamp', displayedTimestampValueExpression: 'Timestamp',
  implicitColumnExpression: 'SpanName',
  serviceNameExpression: 'ServiceName',
  eventAttributesExpression: 'SpanAttributes', resourceAttributesExpression: 'ResourceAttributes',
  defaultTableSelectExpression: 'Timestamp,ServiceName,StatusCode,round(Duration/1e6),SpanName',
  traceIdExpression: 'TraceId', spanIdExpression: 'SpanId',
  durationExpression: 'Duration', durationPrecision: 9,
  parentSpanIdExpression: 'ParentSpanId', spanNameExpression: 'SpanName',
  spanKindExpression: 'SpanKind', statusCodeExpression: 'StatusCode', statusMessageExpression: 'StatusMessage',
};

const end = new Date();
const start = new Date(end.getTime() - 60 * 60 * 1000);
const results: [string, string][] = [];
const failures: [string, string][] = [];

async function step(name: string, fn: () => Promise<unknown>) {
  try {
    const out = await fn();
    const text = typeof out === 'string' ? out : JSON.stringify(out);
    results.push([name, 'OK   ' + text.slice(0, 300)]);
  } catch (e: any) {
    results.push([name, 'FAIL ' + String(e?.message ?? e).slice(0, 500)]);
    failures.push([name, String(e?.message ?? e)]);
  }
}

async function run(config: any, format: any) {
  const chSql = await renderChartConfig(config, metadata, undefined);
  const rs = await client.query({ query: chSql.sql, query_params: chSql.params, format, connectionId });
  return { sql: chSql.sql, params: chSql.params, body: await rs.text() };
}

function search(where: string, extra: any = {}) {
  return { ...buildSearchChartConfig(source, { where, whereLanguage: 'lucene', dateRange: [start, end] }), orderBy: 'Timestamp DESC', limit: { limit: 200, offset: 0 }, ...extra };
}

async function main() {
  await step('getColumns', async () => (await metadata.getColumns({ databaseName, tableName, connectionId })).map((c: any) => `${c.name}:${c.type}`).join(', '));
  await step('getTableMetadata', async () => { const t: any = await metadata.getTableMetadata({ databaseName, tableName, connectionId }); return { engine: t.engine, sorting_key: t.sorting_key }; });
  await step('getAllFields', async () => (await metadata.getAllFields({ databaseName, tableName, connectionId })).length + ' fields');
  await step('getMapKeys(SpanAttributes)', async () => metadata.getMapKeys({ databaseName, tableName, column: 'SpanAttributes', connectionId } as any));

  const searches: [string, string][] = [
    ['search: no filter', ''],
    ['search: term (SpanName)', 'checkout'],
    ['search: phrase', '"SELECT orders"'],
    ['search: field StatusCode', 'StatusCode:Error'],
    ['search: field SpanKind', 'SpanKind:Server'],
    ['search: map key', 'SpanAttributes.http.method:POST'],
    ['search: duration range', 'Duration:[100000000 TO 300000000]'],
    ['search: trace id', `TraceId:${process.env.TRACE_ID ?? 'none'}`],
  ];
  for (const [name, where] of searches) {
    await step(name, async () => {
      const { body } = await run(search(where), 'JSONCompactEachRowWithNamesAndTypes');
      const lines = body.split('\n').filter(Boolean);
      return `${lines.length - 2} rows; types=${lines[1]}; first=${lines[2] ?? '(none)'}`;
    });
  }

  await step('histogram (count by StatusCode, 5 minute)', async () => {
    const config: any = { ...search(''), select: [{ aggFn: 'count', aggCondition: '', valueExpression: '' }], groupBy: 'StatusCode', orderBy: undefined, granularity: '5 minute', displayType: 'stacked_bar', alignDateRangeToGranularity: true, limit: undefined };
    const { sql, body } = await run(config, 'JSON');
    const json = JSON.parse(body);
    return `meta=${json.meta.map((m: any) => `${m.name}:${m.type}`).join('|')} rows=${json.rows} first=${JSON.stringify(json.data[0])}`;
  });

  await step('chart: p95 Duration by ServiceName', async () => {
    const config: any = { ...search(''), select: [{ aggFn: 'quantile', level: 0.95, aggCondition: '', valueExpression: 'Duration' }], groupBy: 'ServiceName', orderBy: undefined, granularity: '5 minute', displayType: 'line', limit: undefined };
    const { body } = await run(config, 'JSON');
    const json = JSON.parse(body);
    return `meta=${json.meta.map((m: any) => `${m.name}:${m.type}`).join('|')} rows=${json.rows} first=${JSON.stringify(json.data[0])}`;
  });

  await step('chart: avg Duration where StatusCode = Error', async () => {
    const config: any = { ...search(''), select: [{ aggFn: 'avg', aggCondition: "StatusCode = 'Error'", aggConditionLanguage: 'sql', valueExpression: 'Duration' }], orderBy: undefined, granularity: '10 minute', displayType: 'line', limit: undefined };
    const { body } = await run(config, 'JSON');
    const json = JSON.parse(body);
    return `rows=${json.rows} first=${JSON.stringify(json.data[0])}`;
  });

  await step('getKeyValues(ServiceName, SpanName, StatusCode, SpanKind)', async () =>
    metadata.getKeyValues({ chartConfig: search(''), keys: ['ServiceName', 'SpanName', 'StatusCode', 'SpanKind'], limit: 20, connectionId } as any));

  let traceId = process.env.TRACE_ID;
  await step('pick a trace id', async () => {
    const { body } = await run(search('SpanKind:Server'), 'JSONCompactEachRowWithNamesAndTypes');
    const lines = body.split('\n').filter(Boolean);
    const names = JSON.parse(lines[0]);
    const row = JSON.parse(lines[2]);
    traceId = traceId ?? row[names.indexOf('TraceId')];
    return traceId;
  });

  await step('waterfall: spans of one trace', async () => {
    const alias: Record<string, string> = {
      Body: source.defaultTableSelectExpression, Timestamp: 'Timestamp', Duration: '(Duration)/1e9',
      TraceId: 'TraceId', SpanId: 'SpanId', ParentSpanId: 'ParentSpanId', StatusCode: 'StatusCode',
      ServiceName: 'ServiceName', SpanAttributes: 'SpanAttributes',
    };
    const config: any = {
      select: [
        { valueExpression: 'SpanName', alias: 'Body' },
        { valueExpression: alias.Timestamp, alias: 'Timestamp' },
        { valueExpression: alias.SpanId, alias: 'SpanId' },
        { valueExpression: alias.ServiceName, alias: 'ServiceName' },
        { valueExpression: alias.Duration, alias: 'Duration' },
        { valueExpression: alias.ParentSpanId, alias: 'ParentSpanId' },
        { valueExpression: alias.StatusCode, alias: 'StatusCode' },
        { valueExpression: alias.SpanAttributes, alias: 'SpanAttributes' },
      ],
      from: source.from, timestampValueExpression: source.timestampValueExpression,
      where: `${alias.TraceId} = ${SqlString.escape(traceId)}`,
      limit: { limit: 10000 }, connection: source.connection,
    };
    const { body } = await run(config, 'JSON');
    const json = JSON.parse(body);
    return `meta=${json.meta.map((m: any) => `${m.name}:${m.type}`).join('|')} rows=${json.rows} first=${JSON.stringify(json.data[0])}`;
  });

  await step('waterfall: with hidden-row expression (lucene)', async () => {
    const config: any = {
      select: [
        { valueExpression: 'SpanName', alias: 'Body' },
        { valueExpression: 'Timestamp', alias: 'Timestamp' },
        { valueExpression: 'SpanId', alias: 'SpanId' },
        { valueExpression: 'NOT (SpanKind:Server)', valueExpressionLanguage: 'lucene', alias: '__hdx_hidden' },
        { valueExpression: '(Duration)/1e9', alias: 'Duration' },
        { valueExpression: 'ParentSpanId', alias: 'ParentSpanId' },
      ],
      from: source.from, timestampValueExpression: source.timestampValueExpression,
      implicitColumnExpression: 'SpanName',
      where: `TraceId = ${SqlString.escape(traceId)}`, limit: { limit: 10000 }, connection: source.connection,
    };
    const { body } = await run(config, 'JSON');
    const json = JSON.parse(body);
    return `rows=${json.rows} first=${JSON.stringify(json.data[0])}`;
  });

  await step('service map', async () => {
    const base: any = (kinds: string) => ({
      from: source.from, connection: source.connection, dateRange: [start, end],
      timestampValueExpression: source.timestampValueExpression, implicitColumnExpression: source.implicitColumnExpression,
      where: '', whereLanguage: 'lucene',
      filters: [
        { type: 'sql', condition: `cityHash64(${source.traceIdExpression}) % 1 = 0` },
        { type: 'sql', condition: `${source.spanKindExpression} IN (${kinds})` },
      ],
      select: [
        { valueExpression: 'TraceId', alias: 'traceId' }, { valueExpression: 'SpanId', alias: 'spanId' },
        { valueExpression: 'ServiceName', alias: 'serviceName' }, { valueExpression: 'ParentSpanId', alias: 'parentSpanId' },
        { valueExpression: 'StatusCode', alias: 'statusCode' }, { valueExpression: 'Duration', alias: 'duration' },
      ],
    });
    const serverCTE = await renderChartConfig(base("'Server', 'Consumer', 'SPAN_KIND_SERVER', 'SPAN_KIND_CONSUMER'"), metadata, undefined);
    const clientCTE = await renderChartConfig(base("'Client', 'Producer', 'SPAN_KIND_CLIENT', 'SPAN_KIND_PRODUCER'"), metadata, undefined);
    const sql = `
    WITH
      ServerSpans AS (${serverCTE.sql}),
      ClientSpans AS (${clientCTE.sql})
    SELECT
      ServerSpans.serviceName AS serverServiceName,
      ClientSpans.serviceName AS clientServiceName,
      GROUPING(ClientSpans.serviceName) AS isNodeLevel,
      count(*) * 1 as requestCount,
      countIf(ServerSpans.statusCode = 'Error') * 1 as errorCount,
      quantiles(0.5, 0.95, 0.99)(ServerSpans.duration) as quantiles
    FROM ServerSpans
      LEFT JOIN ClientSpans
        ON ServerSpans.traceId = ClientSpans.traceId
        AND ServerSpans.parentSpanId = ClientSpans.spanId
    WHERE (ClientSpans.serviceName IS NULL OR ServerSpans.serviceName != ClientSpans.serviceName)
    GROUP BY GROUPING SETS (
      (ServerSpans.serviceName, ClientSpans.serviceName),
      (ServerSpans.serviceName)
    )
    ORDER BY serverServiceName, isNodeLevel, clientServiceName`;
    const params = { ...serverCTE.params, ...clientCTE.params };
    const rs = await client.query({ query: sql, query_params: params, format: 'JSON', connectionId, clickhouse_settings: { join_algorithm: 'auto' } } as any);
    const json = JSON.parse(await rs.text());
    return `rows=${json.rows} data=${JSON.stringify(json.data).slice(0, 400)}`;
  });

  for (const [name, out] of results) console.log(`${name.padEnd(48)} ${out}`);
  console.log(`\n${results.length - failures.length} of ${results.length} OK`);
  for (const [name, err] of failures) console.log(`\n--- ${name}\n${err}`);
}

main().catch(e => { console.error('probe crashed', e); process.exit(1); });
