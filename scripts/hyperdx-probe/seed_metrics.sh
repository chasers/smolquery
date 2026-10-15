#!/bin/bash
# Creates the five otel_metrics_* tables through the API (SMOLQUERY_API_KEY=smolquery-dev at :4000)
# and loads the NDJSON `elixir seed_metrics.exs` wrote into METRICS_DIR (default: the working directory).
S=${METRICS_DIR:-.}
auth='authorization: Bearer smolquery-dev'; json='content-type: application/json'; api=http://127.0.0.1:4000
for i in $(seq 1 80); do curl -s -o /dev/null -w '%{http_code}' $api/v1/datasets -H "$auth" 2>/dev/null | grep -q 200 && break; sleep 3; done
echo "api up after ${i}x3s"
curl -s -H "$auth" -H "$json" -d '{"id": "default"}' $api/v1/datasets >/dev/null

common='{"name": "ResourceAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "ResourceSchemaUrl", "type": "STRING"},
      {"name": "ScopeName", "type": "STRING"},
      {"name": "ScopeVersion", "type": "STRING"},
      {"name": "ScopeAttributes", "type": "MAP(STRING, STRING)"},
      {"name": "ScopeDroppedAttrCount", "type": "INT64"},
      {"name": "ScopeSchemaUrl", "type": "STRING"},
      {"name": "ServiceName", "type": "STRING"},
      {"name": "MetricName", "type": "STRING"},
      {"name": "MetricDescription", "type": "STRING"},
      {"name": "MetricUnit", "type": "STRING"},
      {"name": "Attributes", "type": "MAP(STRING, STRING)"},
      {"name": "StartTimeUnix", "type": "TIMESTAMP"},
      {"name": "TimeUnix", "type": "TIMESTAMP", "nullable": false},
      {"name": "Flags", "type": "INT64"}'

create() {
  local table=$1 extra=$2
  curl -s -H "$auth" -H "$json" -d "{\"id\": \"$table\", \"schema\": [$common, $extra]}" $api/v1/datasets/default/tables | cut -c1-60; echo
  curl -s -X PATCH -H "$auth" -H "$json" -d '{"clustering": ["ServiceName", "MetricName", "TimeUnix"]}' $api/v1/datasets/default/tables/$table | cut -c1-30; echo
  curl -s -H "$auth" -H 'content-type: application/x-ndjson' --data-binary @$S/$table.ndjson "$api/v1/datasets/default/tables/$table/insert" | cut -c1-120; echo
}

create otel_metrics_gauge '{"name": "Value", "type": "FLOAT64"}'
create otel_metrics_sum '{"name": "Value", "type": "FLOAT64"}, {"name": "AggregationTemporality", "type": "INT64"}, {"name": "IsMonotonic", "type": "BOOL"}'
create otel_metrics_histogram '{"name": "Count", "type": "INT64"}, {"name": "Sum", "type": "FLOAT64"}, {"name": "BucketCounts", "type": "ARRAY<INT64>"}, {"name": "ExplicitBounds", "type": "ARRAY<FLOAT64>"}, {"name": "Min", "type": "FLOAT64"}, {"name": "Max", "type": "FLOAT64"}, {"name": "AggregationTemporality", "type": "INT64"}'
create otel_metrics_exponential_histogram '{"name": "Count", "type": "INT64"}, {"name": "Sum", "type": "FLOAT64"}, {"name": "Scale", "type": "INT64"}, {"name": "ZeroCount", "type": "INT64"}, {"name": "PositiveOffset", "type": "INT64"}, {"name": "PositiveBucketCounts", "type": "ARRAY<INT64>"}, {"name": "NegativeOffset", "type": "INT64"}, {"name": "NegativeBucketCounts", "type": "ARRAY<INT64>"}, {"name": "Min", "type": "FLOAT64"}, {"name": "Max", "type": "FLOAT64"}, {"name": "AggregationTemporality", "type": "INT64"}'
create otel_metrics_summary '{"name": "Count", "type": "INT64"}, {"name": "Sum", "type": "FLOAT64"}'
sleep 3
curl -s --max-time 60 'http://127.0.0.1:18123/?default_format=JSONCompact' -H 'X-ClickHouse-Key: smolquery-dev' --data-binary "SELECT count(), min(TimeUnix), max(TimeUnix) FROM default.otel_metrics_histogram" | head -c 300; echo
