-- The ClickHouse side of the comparison, written down so a run is reproducible.
--
-- Column names, order, and types mirror schema.json one for one. The sort key
-- mirrors the clustering key set on the smolquery table (see README step 3):
-- comparing a sorted table against an unsorted one measures the sort, not the
-- engine.
--
-- Only the four columns the generator actually emits nulls for are Nullable.
-- ClickHouse's input_format_null_as_default would otherwise turn those nulls
-- into empty strings on arrival, and the two back ends would stop holding the
-- same data at rest even though they were sent the same bytes.
--
--   clickhouse client --queries-file scripts/k6/clickhouse.sql

CREATE DATABASE IF NOT EXISTS bench;

DROP TABLE IF EXISTS bench.otel_logs;

CREATE TABLE bench.otel_logs
(
    project_id String,
    timestamp DateTime64(3),
    observed_timestamp DateTime64(3),
    severity_number Int64,
    severity_text String,
    body String,
    trace_id String,
    span_id String,
    trace_flags Int64,
    dropped_attributes_count Int64,
    service_name String,
    service_namespace String,
    service_version String,
    service_instance_id String,
    deployment_environment String,
    cloud_provider String,
    cloud_region String,
    cloud_availability_zone String,
    cloud_account_id String,
    k8s_cluster_name String,
    k8s_namespace_name String,
    k8s_deployment_name String,
    k8s_pod_name String,
    k8s_pod_uid String,
    k8s_container_name String,
    k8s_node_name String,
    host_name String,
    host_arch String,
    os_type String,
    os_version String,
    container_id String,
    container_image_tag String,
    telemetry_sdk_name String,
    telemetry_sdk_language String,
    telemetry_sdk_version String,
    scope_name String,
    scope_version String,
    code_namespace String,
    code_function String,
    code_lineno Int64,
    http_request_method String,
    http_route String,
    http_response_status_code Int64,
    http_request_body_size Int64,
    http_response_body_size Int64,
    url_path String,
    url_scheme String,
    network_protocol_version String,
    user_agent_original String,
    client_address String,
    server_address String,
    server_port Int64,
    duration_ms Float64,
    error_type Nullable(String),
    exception_type Nullable(String),
    exception_message Nullable(String),
    exception_stacktrace Nullable(String),
    enduser_id String,
    session_id String,
    thread_name String,
    log_file_path String,
    sampled Bool
)
ENGINE = MergeTree
ORDER BY (project_id, timestamp);
