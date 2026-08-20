Code.require_file("support.exs", __DIR__)

defmodule Bench.Otel do
  @moduledoc """
  The OpenTelemetry logs fixture, and the node takeover the e2e benches share.

  `bench/otel_logs.exs` (PL-17, streaming inserts) and `bench/load.exs` (PL-18,
  batch loads) measure different endpoints against the same data and the same
  node, so the schema, the row generator, and the boot takeover live here rather
  than being duplicated into both.

  ## The fixture

  61 columns, flattened from OpenTelemetry's resource / scope / record /
  HTTP-attribute groups, spanning `TIMESTAMP`, `INT64`, `FLOAT64`, `STRING`, and
  `BOOL` — `Smolquery.Schema` has no map or list type, which is what every log
  warehouse ends up doing anyway (see T-140). A record is ~2.1 KB of JSON.

  Rows are shaped like traffic rather than uniformly: INFO-dominated severity with
  a few percent ERROR, exception columns null except on those, bounded service and
  route cardinality so a filtered query has something to select, and bodies that
  vary in length.

  ## The node

  `SmolqueryApi.Endpoint` is a module-based Phoenix endpoint, so there cannot be a
  second instance beside the application's own — an e2e bench has to *be* the node
  rather than start a private stack next to it. `boot!/1` terminates the role
  subtrees `mix run` already started against `priv/data`, repoints the catalog,
  buffer, and sealed directories at a scratch dir, and restarts all of them except
  `SmolqueryWeb.Supervisor`.

  `Application.stop/1` plus `ensure_all_started/1` cannot do this:
  `Smolquery.Telemetry.init/1` attaches its handler with `:ok = attach_many/4` and
  does not trap exits, so a second start crashes on `:already_exists`. Restarting
  the role subtrees skips that child entirely.
  """

  import Bench.Support, except: [table: 0, schema: 0]

  alias Smolquery.BufferService
  alias Smolquery.InternalSecret
  alias Smolquery.Schema

  @dataset "logs"
  @table "otel_logs"
  @api_key "otel-bench-key"
  @default_pool_size 256

  @subtrees [
    SmolqueryApi.Supervisor,
    SmolqueryWeb.Supervisor,
    Smolquery.QueryService.Supervisor,
    Smolquery.BufferService.Supervisor,
    Smolquery.StorageService.Supervisor,
    Smolquery.IngestService.Supervisor
  ]

  @columns [
    {"timestamp", "TIMESTAMP"},
    {"observed_timestamp", "TIMESTAMP"},
    {"severity_number", "INT64"},
    {"severity_text", "STRING"},
    {"body", "STRING"},
    {"trace_id", "STRING"},
    {"span_id", "STRING"},
    {"trace_flags", "INT64"},
    {"dropped_attributes_count", "INT64"},
    {"service_name", "STRING"},
    {"service_namespace", "STRING"},
    {"service_version", "STRING"},
    {"service_instance_id", "STRING"},
    {"deployment_environment", "STRING"},
    {"cloud_provider", "STRING"},
    {"cloud_region", "STRING"},
    {"cloud_availability_zone", "STRING"},
    {"cloud_account_id", "STRING"},
    {"k8s_cluster_name", "STRING"},
    {"k8s_namespace_name", "STRING"},
    {"k8s_deployment_name", "STRING"},
    {"k8s_pod_name", "STRING"},
    {"k8s_pod_uid", "STRING"},
    {"k8s_container_name", "STRING"},
    {"k8s_node_name", "STRING"},
    {"host_name", "STRING"},
    {"host_arch", "STRING"},
    {"os_type", "STRING"},
    {"os_version", "STRING"},
    {"container_id", "STRING"},
    {"container_image_tag", "STRING"},
    {"telemetry_sdk_name", "STRING"},
    {"telemetry_sdk_language", "STRING"},
    {"telemetry_sdk_version", "STRING"},
    {"scope_name", "STRING"},
    {"scope_version", "STRING"},
    {"code_namespace", "STRING"},
    {"code_function", "STRING"},
    {"code_lineno", "INT64"},
    {"http_request_method", "STRING"},
    {"http_route", "STRING"},
    {"http_response_status_code", "INT64"},
    {"http_request_body_size", "INT64"},
    {"http_response_body_size", "INT64"},
    {"url_path", "STRING"},
    {"url_scheme", "STRING"},
    {"network_protocol_version", "STRING"},
    {"user_agent_original", "STRING"},
    {"client_address", "STRING"},
    {"server_address", "STRING"},
    {"server_port", "INT64"},
    {"duration_ms", "FLOAT64"},
    {"error_type", "STRING"},
    {"exception_type", "STRING"},
    {"exception_message", "STRING"},
    {"exception_stacktrace", "STRING"},
    {"enduser_id", "STRING"},
    {"session_id", "STRING"},
    {"thread_name", "STRING"},
    {"log_file_path", "STRING"},
    {"sampled", "BOOL"}
  ]

  @services ~w(checkout-api cart-api catalog-api payments-api auth-api)
  @routes ~w(/v1/checkout /v1/cart /v1/cart/:id /v1/catalog/items /v1/auth/token)
  @methods ~w(GET POST PUT DELETE)
  @regions ~w(us-east-1 us-west-2 eu-central-1)
  @tail_service "checkout-api"
  @error_severity 17

  @doc """
  The dataset every e2e bench writes into.
  """
  def dataset, do: @dataset

  @doc """
  The first table's name; `table_at/1` names the rest.
  """
  def table, do: @table

  @doc """
  The nth table, so a bench can spread writers over several `TableBuffer`s.
  """
  def table_at(0), do: @table
  def table_at(index), do: "#{@table}_#{index + 1}"

  @doc """
  The `{name, api_type}` pairs the fixture's table is created with.
  """
  def columns, do: @columns

  @doc """
  The fixture as a `Smolquery.Schema`, for the paths that skip the API.
  """
  def table_schema do
    Schema.new!(
      for {name, api_type} <- @columns do
        {:ok, type} = Schema.type_from_api(api_type)

        {name, type}
      end
    )
  end

  @doc """
  Takes over this node's roles, pointing every directory at `dir`.

  Returns a `Req` handle carrying the API key, aimed at the restarted endpoint.
  """
  def boot!(dir) do
    File.mkdir_p!(dir)

    for id <- @subtrees, do: Supervisor.terminate_child(Smolquery.Supervisor, id)

    Application.put_env(:smolquery, :data_dir, dir)

    Application.put_env(:smolquery, Smolquery.Catalog.DuckLake,
      metadata: "sqlite:#{Path.join(dir, "catalog.sqlite")}",
      data_path: Path.join(dir, "ducklake")
    )

    merge_env(Smolquery.BufferService, dir: Path.join(dir, "buffer"))
    merge_env(Smolquery.StorageService, dir: Path.join(dir, "sealed"))
    merge_env(SmolqueryApi, api_key: @api_key)

    if interval = System.get_env("FLUSH_MS") do
      merge_env(Smolquery.BufferService, flush_interval_ms: String.to_integer(interval))
    end

    for id <- @subtrees -- [SmolqueryWeb.Supervisor] do
      {:ok, _pid} = Supervisor.restart_child(Smolquery.Supervisor, id)
    end

    Req.new(
      base_url: base_url(),
      auth: {:bearer, @api_key},
      retry: false,
      receive_timeout: 600_000
    )
  end

  @doc """
  Stops the role subtrees again and removes the scratch directory.
  """
  def teardown!(dir) do
    for id <- @subtrees, do: Supervisor.terminate_child(Smolquery.Supervisor, id)

    File.rm_rf!(dir)
    IO.puts("")
  end

  @doc """
  Creates the dataset and `count` tables over HTTP.
  """
  def create_tables!(req, count) do
    schema = for {name, type} <- @columns, do: %{"name" => name, "type" => type}

    %{status: 200} = Req.post!(req, url: "/v1/datasets", json: %{"id" => @dataset})

    for index <- 0..(count - 1) do
      %{status: 200} =
        Req.post!(req,
          url: "/v1/datasets/#{@dataset}/tables",
          json: %{"id" => table_at(index), "schema" => schema}
        )
    end

    :ok
  end

  @doc """
  Where the restarted API listens.
  """
  def base_url, do: SmolqueryApi.Endpoint.base_url()

  @doc """
  The buffer's flush interval, which every latency here is measured against.
  """
  def flush_interval_ms do
    :smolquery
    |> Application.get_env(Smolquery.BufferService, [])
    |> Keyword.fetch!(:flush_interval_ms)
  end

  @doc """
  How deep a table's hot tier is: `{unsealed, in_the_manifest}`.

  Only the unsealed entries are read by a query. `HotManifest.entries/2` also
  returns entries a sealer already retired, which live out `retire_grace_ms` so an
  in-flight scan holding an older snapshot cannot lose rows underneath it —
  counting those reads as a seal backlog that is not there.
  """
  def hot_depth(tables) do
    entries =
      Enum.flat_map(0..(tables - 1), fn index ->
        {:ok, entries} =
          BufferService.Client.hot_manifest(Smolquery.BufferService, {@dataset, table_at(index)})

        entries
      end)

    {Enum.count(entries, &is_nil(&1.retired_at)), length(entries)}
  end

  @doc """
  Prints the node's Prometheus counters, read through `GET /metrics`.
  """
  def counters do
    heading("Node counters (GET /metrics)")

    body =
      Req.get!(
        base_url() <> "/metrics",
        headers: [{InternalSecret.header(), InternalSecret.value()}]
      ).body

    for name <- ~w(smolquery_buffer_commits_total smolquery_buffer_rows_committed_total
                   smolquery_buffer_commit_bytes_total smolquery_seal_segment_bytes_total
                   smolquery_buffer_admission_refused_rows_total smolquery_seal_attempts_total
                   smolquery_ingest_rows_rejected_total smolquery_query_jobs_total) do
      IO.puts(
        "  #{label(String.replace_prefix(name, "smolquery_", ""), 40)} #{series(body, name)}"
      )
    end
  end

  @doc """
  The row templates every batch is drawn from, as a tuple for O(1) access.

  Raises unless the pool holds a row matching the filtered-tail predicate: a
  generator whose severity and service draws are correlated can silently produce
  none, and then a filtered query measures an empty result.
  """
  def pool do
    templates = for i <- 0..(pool_size() - 1), do: template(i)

    case Enum.count(templates, &tailable?/1) do
      0 ->
        raise "no #{tail_service()} row at severity >= #{error_severity()} in the pool: " <>
                "a filtered query would be measuring an empty result"

      _matches ->
        List.to_tuple(templates)
    end
  end

  @doc """
  How many distinct row templates the pool holds — `POOL`, default 256.

  This is the fixture's cardinality, and it is load-bearing for anything that
  reports *bytes*: 256 templates compress far harder than real log traffic, so a
  Parquet fixture built from them is unrealistically small. Raise it to price a
  higher-cardinality corpus.
  """
  def pool_size, do: env("POOL", @default_pool_size)

  @doc """
  `count` rows drawn from `pool`, stamped now — the shape an insert body carries.
  """
  def rows(pool, count, offset) do
    base = DateTime.utc_now()

    for i <- 0..(count - 1) do
      at = DateTime.add(base, i, :microsecond)

      pool
      |> elem(rem(offset + i, tuple_size(pool)))
      |> Map.merge(%{
        "timestamp" => DateTime.to_iso8601(at),
        "observed_timestamp" => at |> DateTime.add(2, :millisecond) |> DateTime.to_iso8601()
      })
    end
  end

  @doc """
  The service a filtered query selects on.
  """
  def tail_service, do: @tail_service

  @doc """
  The severity a filtered query selects at or above.
  """
  def error_severity, do: @error_severity

  defp merge_env(key, opts) do
    Application.put_env(
      :smolquery,
      key,
      Keyword.merge(Application.get_env(:smolquery, key, []), opts)
    )
  end

  defp series(body, name) do
    ~r/^#{name}(\{[^}]*\})? ([\d.e+]+)$/m
    |> Regex.scan(body)
    |> Enum.map_join(", ", fn match -> match |> List.last() |> round_series() end)
    |> case do
      "" -> "-"
      text -> text
    end
  end

  defp round_series(value) do
    {number, _rest} = Float.parse(value)

    number |> round() |> to_string()
  end

  defp tailable?(template) do
    template["service_name"] == @tail_service and
      template["severity_number"] >= @error_severity
  end

  @doc """
  One `POST /insert` body: `batch` rows drawn from `pool`, stamped now.

  Timestamps are fresh on every call — a bench that measures staleness cannot
  reuse a pre-encoded body. `INSERT_FORMAT=ndjson` sends newline-delimited
  rows instead of the `{"rows": [...]}` envelope, which is the columnar fast
  path (PL-21); the rows inside are identical.
  """
  def body(pool, batch, offset) do
    case insert_format() do
      :json -> JSON.encode!(%{"rows" => rows(pool, batch, offset)})
      :ndjson -> Enum.map_join(rows(pool, batch, offset), "\n", &JSON.encode!/1) <> "\n"
    end
  end

  @doc "The insert body format the bench drives with, from `INSERT_FORMAT`."
  def insert_format do
    case System.get_env("INSERT_FORMAT", "json") do
      "json" -> :json
      "ndjson" -> :ndjson
      other -> raise ArgumentError, "INSERT_FORMAT=#{other} — expected json or ndjson"
    end
  end

  @doc "The content type `body/3`'s output must be posted as."
  def insert_content_type do
    case insert_format() do
      :json -> "application/json"
      :ndjson -> "application/x-ndjson"
    end
  end

  defp template(i) do
    {severity, text} = severity(rem(i * 7, 100))
    service = Enum.at(@services, rem(div(i, 5), length(@services)))
    route = Enum.at(@routes, rem(i, length(@routes)))
    status = if severity >= @error_severity, do: 500, else: 200
    pod = "#{service}-#{rem(i, 8)}f4c9-#{rem(i, 26)}kx2"
    region = Enum.at(@regions, rem(i, length(@regions)))

    %{
      "severity_number" => severity,
      "severity_text" => text,
      "body" => body_text(text, service, route, status, i),
      "trace_id" => hex(i, 32),
      "span_id" => hex(i * 3, 16),
      "trace_flags" => 1,
      "dropped_attributes_count" => 0,
      "service_name" => service,
      "service_namespace" => "storefront",
      "service_version" => "2.#{rem(i, 12)}.#{rem(i, 5)}",
      "service_instance_id" => pod,
      "deployment_environment" => "production",
      "cloud_provider" => "aws",
      "cloud_region" => region,
      "cloud_availability_zone" => region <> Enum.at(~w(a b c), rem(i, 3)),
      "cloud_account_id" => "9284#{rem(i, 100)}17263",
      "k8s_cluster_name" => "storefront-prod",
      "k8s_namespace_name" => "storefront",
      "k8s_deployment_name" => service,
      "k8s_pod_name" => pod,
      "k8s_pod_uid" => hex(i * 11, 32),
      "k8s_container_name" => service,
      "k8s_node_name" => "ip-10-#{rem(i, 200)}-#{rem(i, 250)}-14.ec2.internal",
      "host_name" => "ip-10-#{rem(i, 200)}-#{rem(i, 250)}-14",
      "host_arch" => "arm64",
      "os_type" => "linux",
      "os_version" => "6.1.#{rem(i, 90)}-amzn2023",
      "container_id" => hex(i * 13, 64),
      "container_image_tag" => "sha256-#{hex(i * 17, 12)}",
      "telemetry_sdk_name" => "opentelemetry",
      "telemetry_sdk_language" => "erlang",
      "telemetry_sdk_version" => "1.5.0",
      "scope_name" => "#{service}.plug.router",
      "scope_version" => "1.20.2",
      "code_namespace" =>
        "Storefront.#{Macro.camelize(String.replace(service, "-", "_"))}.Router",
      "code_function" => "call/2",
      "code_lineno" => 40 + rem(i, 600),
      "http_request_method" => Enum.at(@methods, rem(i, length(@methods))),
      "http_route" => route,
      "http_response_status_code" => status,
      "http_request_body_size" => rem(i * 97, 8_192),
      "http_response_body_size" => rem(i * 131, 65_536),
      "url_path" => String.replace(route, ":id", to_string(1_000 + rem(i, 9_000))),
      "url_scheme" => "https",
      "network_protocol_version" => "2",
      "user_agent_original" =>
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " <>
          "(KHTML, like Gecko) Chrome/1#{rem(i, 30)}.0.0.0 Safari/537.36",
      "client_address" => "203.0.#{rem(i, 250)}.#{rem(i * 3, 250)}",
      "server_address" => "#{service}.storefront.svc.cluster.local",
      "server_port" => 4000 + rem(i, 3),
      "duration_ms" => Float.round(1.5 + rem(i * 37, 4_000) / 100, 2),
      "error_type" => error_field(severity, "Storefront.PaymentError"),
      "exception_type" => error_field(severity, "Storefront.PaymentError"),
      "exception_message" => error_field(severity, "gateway declined authorization #{i}"),
      "exception_stacktrace" => error_field(severity, stacktrace(i)),
      "enduser_id" => "usr_#{hex(i * 19, 16)}",
      "session_id" => "sess_#{hex(i * 23, 24)}",
      "thread_name" => "erl_sched_#{rem(i, 10)}",
      "log_file_path" => "/var/log/pods/storefront_#{pod}/#{service}/0.log",
      "sampled" => rem(i, 4) != 0
    }
  end

  defp severity(draw) when draw < 1, do: {1, "TRACE"}
  defp severity(draw) when draw < 16, do: {5, "DEBUG"}
  defp severity(draw) when draw < 86, do: {9, "INFO"}
  defp severity(draw) when draw < 96, do: {13, "WARN"}
  defp severity(_draw), do: {@error_severity, "ERROR"}

  defp body_text("ERROR", service, route, status, i) do
    "#{service} #{route} failed with #{status} after #{rem(i * 37, 4_000) / 100}ms: " <>
      "gateway declined authorization #{i}, retry scheduled"
  end

  defp body_text(text, service, route, status, i) do
    "#{text} #{service} handled #{route} #{status} in #{rem(i * 37, 4_000) / 100}ms " <>
      "req_id=#{hex(i * 29, 16)} bytes=#{rem(i * 131, 65_536)}"
  end

  defp error_field(severity, value) when severity >= @error_severity, do: value
  defp error_field(_severity, _value), do: nil

  defp stacktrace(i) do
    Enum.map_join(1..4, "\n", fn frame ->
      "    (storefront 2.1.0) lib/storefront/payments.ex:#{frame * 40 + rem(i, 30)}: " <>
        "Storefront.Payments.authorize/#{frame}"
    end)
  end

  defp hex(seed, length) do
    digest = :crypto.hash(:sha256, <<seed::64>>)

    digest |> Base.encode16(case: :lower) |> binary_part(0, length)
  end
end
