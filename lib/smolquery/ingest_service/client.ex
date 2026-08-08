defmodule Smolquery.IngestService.Client do
  @moduledoc """
  The one way into the ingest service, per the split-out rules.

  `insert/3` is the streaming-insert brain (PL-8 D2): resolve the table's
  schema (cached), validate and coerce every row, and forward what survives
  to the owning buffer service — returning only when the buffer has those
  rows durable and queryable. One request is one forward-batch (PL-8 D3);
  coalescing across requests is a measurement away, not a default.

  Nothing here fakes durability: there is no ack until
  `BufferService.Client.write_batch/3` reports the batch persisted, and a
  batch with no valid rows reports zero without touching the buffer.
  """

  alias Smolquery.BufferService
  alias Smolquery.Heap
  alias Smolquery.IngestService.Runtime
  alias Smolquery.IngestService.SchemaCache
  alias Smolquery.IngestService.Validator
  alias Smolquery.Segments.Store

  @type result :: %{inserted: non_neg_integer(), errors: [Validator.row_errors()]}

  @doc """
  Validates `rows` against the table's schema and writes the valid ones.

  `{:ok, result}` carries how many rows the buffer acked and every rejected
  row's index and reasons — partial failure is a result, not an error. An
  error is a whole-request failure: the table does not exist, the buffer is
  full (`{:error, :buffer_full}`) or too far behind
  (`{:error, {:overloaded, predicted_ms}}`, PL-9) — both the API's 429 — or
  a service is not running.

  ## Options

    * `:batch_id` — the batch's idempotency key, end to end. It must come
      from the caller whose retries it protects: an id generated here would
      change on every retry and dedup nothing. With one, a retry of a batch
      the buffer already committed — after a lost response, a transport
      timeout, or a buffer crash-before-reply — is answered with the
      original commit instead of writing the rows twice (T-41). Without
      one, writes are at-least-once, as before.

  Runs in the caller's process — a request handler, for both the insert and
  the load route — which is where the decoded body and the coerced rows are,
  and is therefore also where `Smolquery.IngestService.Runtime`'s
  `:request_fullsweep_after` and `:request_min_heap_size` land if they are
  set. They are unset by default: the process belongs to the web server, not
  to this service, and a flag set on it outlives the insert for as long as the
  connection is kept alive.
  """
  @spec insert(atom(), Store.table_ref(), [term()], keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert(name, table_ref, payload, opts \\ []) do
    with {:ok, runtime} <- runtime(name),
         {:ok, schema} <- SchemaCache.fetch(runtime, table_ref) do
      tune(runtime)

      case validate(schema, payload) do
        {%{row_count: 0}, errors} ->
          measure(0, errors)

          {:ok, %{inserted: 0, errors: errors}}

        {batch, errors} ->
          write(runtime, table_ref, batch(schema, batch, opts), errors)
      end
    end
  end

  @doc """
  Forwards a spooled NDJSON body without parsing a single row of it.

  The counterpart of `insert/4` for `flush_writer: :duckdb`: the API wrote the
  request body to `path` and counted its lines, and DuckDB reads it at flush.
  Nothing here validates, so **`insertErrors` is always empty** — a value this
  path cannot coerce fails the whole flush instead of one row's index, which is a
  weaker promise than `docs/api.md` makes for `insert/4`. Callers that need
  per-row errors must use `insert/4`.

  Refuses when this node does not own the table: the path names a file on this
  node's disk, and a `:gen_rpc` forward would hand the owner a name it cannot
  open. `insert/4` has no such restriction.
  """
  @spec insert_file(atom(), Store.table_ref(), Path.t(), non_neg_integer(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert_file(name, table_ref, path, row_count, opts \\ []) do
    with {:ok, runtime} <- runtime(name),
         {:ok, schema} <- SchemaCache.fetch(runtime, table_ref),
         :ok <- local_owner(runtime, table_ref),
         {:ok, %File.Stat{size: bytes}} <- File.stat(path) do
      batch = %{
        schema: schema,
        ndjson: path,
        row_count: row_count,
        byte_size: bytes
      }

      batch =
        case Keyword.get(opts, :batch_id) do
          nil -> batch
          batch_id -> Map.put(batch, :batch_id, batch_id)
        end

      write(runtime, table_ref, batch, [])
    end
  end

  defp local_owner(runtime, table_ref) do
    if BufferService.Client.owner(runtime.buffer_name, table_ref) == node() do
      :ok
    else
      {:error, :spooled_batch_not_local}
    end
  end

  # Both wire shapes converge here on the one column-major batch the buffer
  # takes, so nothing downstream of this function knows which one arrived.
  defp validate(schema, {:columns, columns, row_count}),
    do: Validator.validate_columns(schema, columns, row_count)

  defp validate(schema, rows) when is_list(rows), do: Validator.validate(schema, rows)

  defp tune(runtime) do
    Heap.tune(
      fullsweep_after: runtime.request_fullsweep_after,
      min_heap_size: runtime.request_min_heap_size
    )
  end

  @doc """
  Drops a table's cached schema on this node — what the API's CRUD routes
  call after changing what the catalog says.

  An ingest service that is not running has nothing cached, and that is not
  the caller's problem.
  """
  @spec invalidate(atom(), Store.table_ref()) :: :ok
  def invalidate(name, table_ref) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> SchemaCache.invalidate(runtime, table_ref)
      :error -> :ok
    end
  end

  defp write(runtime, table_ref, batch, errors) do
    with {:ok, _ack} <- BufferService.Client.write_batch(runtime.buffer_name, table_ref, batch) do
      measure(batch.row_count, errors)

      {:ok, %{inserted: batch.row_count, errors: errors}}
    end
  end

  defp measure(accepted, errors) do
    :telemetry.execute(
      [:smolquery, :ingest, :insert],
      %{accepted: accepted, rejected: length(errors)},
      %{}
    )
  end

  defp batch(schema, batch, opts) do
    batch = Map.put(batch, :schema, schema)

    case Keyword.get(opts, :batch_id) do
      nil -> batch
      batch_id -> Map.put(batch, :batch_id, batch_id)
    end
  end

  defp runtime(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> {:ok, runtime}
      :error -> {:error, :ingest_service_unavailable}
    end
  end
end
