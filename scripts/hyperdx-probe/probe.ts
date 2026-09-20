// Runs HyperDX's own query code against a smolquery ClickHouse edge (PL-66).
//
// HyperDX's SQL generator, metadata reader and ClickHouse client wrapper live
// in its `packages/common-utils`. This script drives them, with the real
// `@clickhouse/client`, the way the Search page does: read the source's
// columns and sorting key, list fields and map keys, run the results table
// for a set of Lucene searches, the histogram, and the filters sidebar's
// values. Everything but the browser UI and MongoDB is the real thing.
//
// Setup (Node 22; no container needed):
//
//   git clone https://github.com/hyperdxio/hyperdx && cd hyperdx/packages/common-utils
//   mkdir probe && cp -r src probe/src && cp <this file> probe/ && cd probe
//   jq '{name: "probe", private: true, dependencies: (.dependencies + {"tsx": "^4"})}' \
//     ../package.json > package.json && npm install
//   echo '{"compilerOptions":{"module":"commonjs","moduleResolution":"node","esModuleInterop":true,
//     "skipLibCheck":true,"strict":false,"baseUrl":".","paths":{"@/*":["./src/*"]}}}' > tsconfig.json
//   SMOL_HOST=http://localhost:18123 SMOL_PASSWORD=smolquery-dev ./node_modules/.bin/tsx probe.ts
//
// It expects the `default.otel_logs` table of docs/clickstack.md, with rows
// from the last hour. Last run: hyperdxio/hyperdx @ c42dda8, 2026-09-20, 17 of 17 OK.
import { ClickhouseClient } from '@/clickhouse/node';
import { getMetadata } from '@/core/metadata';
import { renderChartConfig } from '@/core/renderChartConfig';
import { buildSearchChartConfig } from '@/core/searchChartConfig';

const host = process.env.SMOL_HOST ?? 'http://localhost:18123';
const client = new ClickhouseClient({ host, username: 'default', password: process.env.SMOL_PASSWORD ?? 'smolquery-dev' });
const metadata = getMetadata(client);
const connectionId = 'smolquery';
const databaseName = 'default';
const tableName = 'otel_logs';

const source: any = {
  id: 'logs', name: 'Logs', kind: 'log', connection: connectionId,
  from: { databaseName, tableName },
  timestampValueExpression: 'Timestamp', displayedTimestampValueExpression: 'Timestamp',
  implicitColumnExpression: 'Body', bodyExpression: 'Body',
  serviceNameExpression: 'ServiceName', severityTextExpression: 'SeverityText',
  eventAttributesExpression: 'LogAttributes', resourceAttributesExpression: 'ResourceAttributes',
  traceIdExpression: 'TraceId', spanIdExpression: 'SpanId',
  defaultTableSelectExpression: 'Timestamp,ServiceName,SeverityText,Body',
};

const end = new Date();
const start = new Date(end.getTime() - 60 * 60 * 1000);
const results: [string, string][] = [];

async function step(name: string, fn: () => Promise<unknown>) {
  try {
    const out = await fn();
    const text = typeof out === 'string' ? out : JSON.stringify(out);
    results.push([name, 'OK   ' + text.slice(0, 260)]);
  } catch (e: any) {
    results.push([name, 'FAIL ' + String(e?.message ?? e).slice(0, 400)]);
  }
}

async function run(config: any, format: any) {
  const chSql = await renderChartConfig(config, metadata, undefined);
  const rs = await client.query({ query: chSql.sql, query_params: chSql.params, format, connectionId });
  return { sql: chSql.sql, body: await rs.text() };
}

async function main() {
  await step('getColumns', async () => (await metadata.getColumns({ databaseName, tableName, connectionId })).map((c: any) => `${c.name}:${c.type}`).join(', '));
  await step('getTableMetadata', async () => { const t: any = await metadata.getTableMetadata({ databaseName, tableName, connectionId }); return { engine: t.engine, sorting_key: t.sorting_key, primary_key: t.primary_key, partition_key: t.partition_key }; });
  await step('getAllFields', async () => (await metadata.getAllFields({ databaseName, tableName, connectionId })).length + ' fields');
  await step('getMapKeys(LogAttributes)', async () => metadata.getMapKeys({ databaseName, tableName, column: 'LogAttributes', connectionId } as any));

  const searches: [string, string][] = [
    ['search: no filter', ''],
    ['search: term', 'payment'],
    ['search: phrase', '"payment failed"'],
    ['search: negation', '-payment'],
    ['search: field', 'ServiceName:api'],
    ['search: exact field', 'SeverityText:"error"'],
    ['search: exists', 'TraceId:*'],
    ['search: map key', 'LogAttributes.http.status:500'],
    ['search: number', 'SeverityNumber:17'],
    ['search: range', 'SeverityNumber:[10 TO 20]'],
    ['search: underscore term', 'user_id'],
  ];

  for (const [name, where] of searches) {
    await step(name, async () => {
      const config: any = { ...buildSearchChartConfig(source, { where, whereLanguage: 'lucene', dateRange: [start, end] }), orderBy: 'Timestamp DESC', limit: { limit: 200, offset: 0 } };
      const { body } = await run(config, 'JSONCompactEachRowWithNamesAndTypes');
      const lines = body.split('\n').filter(Boolean);
      return `${lines.length - 2} rows; first=${lines[2] ?? '(none)'}`;
    });
  }

  await step('histogram', async () => {
    const base: any = buildSearchChartConfig(source, { where: '', whereLanguage: 'lucene', dateRange: [start, end] });
    const config: any = { ...base, select: [{ aggFn: 'count', aggCondition: '', valueExpression: '' }], orderBy: undefined, granularity: '5 minute', displayType: 'stacked_bar', alignDateRangeToGranularity: false, dateRangeEndInclusive: true, groupBy: source.severityTextExpression };
    const { sql, body } = await run(config, 'JSON');
    const json = JSON.parse(body);
    return `meta=${json.meta.map((m: any) => m.name).join('|')} rows=${json.rows} first=${JSON.stringify(json.data[0])} sql=${sql.slice(0, 120)}`;
  });

  await step('getKeyValues(ServiceName,SeverityText)', async () => metadata.getKeyValues({ chartConfig: buildSearchChartConfig(source, { where: '', whereLanguage: 'lucene', dateRange: [start, end] }) as any, keys: ['ServiceName', 'SeverityText'], limit: 20 } as any));

  for (const [name, out] of results) console.log(`${name.padEnd(40)} ${out}`);
}

main().catch(e => { console.error('probe crashed', e); process.exit(1); });
