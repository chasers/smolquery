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

  `row_count` is the caller's line count and is what the admission bounds
  measure the request as. It is *not* what comes back: `result.inserted` is the
  Parquet footer's count, read back from the segment the flush wrote, which is
  the only number in this path that says what landed. Nothing here counted a
  row, and a line count and a row count disagree over blank lines, trailing
  whitespace, and a body whose last line has no newline.

  The caller owns `path` and must delete it on every outcome. This function
  never does: it hands the accumulator a name, not the bytes, and a delete here
  would race the flush that is about to read it. It is safe to delete once this
  returns — the reply is sent after the commit the body was written by, and a
  spooled body is never grouped with another, so nothing else is still holding
  it. `sweep_spool/1` is the backstop for a caller that did not live to return.

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

      write_spooled(runtime, table_ref, batch)
    end
  end

  @doc """
  Whether this node can take a spooled body at all.

  `insert_file/5` hands the accumulator a path instead of rows, and only
  `flush_writer: :duckdb` starts the DuckDB write pool that reads it back.
  Under the default `:polars` there is no engine to call, and discovering that
  at flush time costs far more than the one request: the call exits `:noproc`
  inside the table's committer, which takes the `TableBuffer` with it and drops
  every batch it was holding unacked — including the ordinary JSON inserts of
  clients that never sent an NDJSON body.

  So the edge asks before it spools, and answers
  `{:error, {:spooled_inserts_unsupported, writer}}` — which names the
  configured writer, because the operator reading that message is the one who
  has to change it. The answer is about *this* node because the path is: the
  body lands on this node's disk and `insert_file/5` refuses a table this node
  does not own.
  """
  @spec check_file_writer(atom()) :: :ok | {:error, term()}
  def check_file_writer(name) do
    with {:ok, runtime} <- runtime(name),
         {:ok, buffer} <- buffer_runtime(runtime) do
      case buffer.flush_writer do
        :duckdb -> :ok
        writer -> {:error, {:spooled_inserts_unsupported, writer}}
      end
    end
  end

  defp buffer_runtime(runtime) do
    case BufferService.Runtime.fetch(runtime.buffer_name) do
      {:ok, buffer} -> {:ok, buffer}
      :error -> {:error, :buffer_service_unavailable}
    end
  end

  @doc """
  Where a request body spools before `insert_file/5` forwards it.

  One function so the writer of those files and the sweeper of them cannot
  drift apart on which directory they mean.
  """
  @spec spool_dir() :: Path.t()
  def spool_dir do
    Path.join(Application.get_env(:smolquery, :data_dir, System.tmp_dir!()), "tmp")
  end

  @doc """
  Deletes spooled bodies older than `age_ms`, returning what was deleted.

  The backstop for `insert_file/5`'s ownership rule, and the counterpart of
  `Smolquery.Segments.Store.sweep_staging/2` for the other temporary directory
  the write path uses. The edge deletes the body it spooled on every outcome it
  lives to see; what it cannot delete is a body whose request handler was killed
  outright, or one a node left behind by going down mid-upload. Nothing else
  names those files — they never become segments, so no manifest, catalog, or
  garbage collector will ever find them — and they sit on the same volume as the
  manifest logs and the catalog, so what they fill up is the write path itself.

  The age is the guard against sweeping a body that is merely still uploading:
  pass a duration comfortably longer than the slowest request this node serves,
  or `0` only from a boot path, where nothing can be writing yet.
  """
  @spec sweep_spool(non_neg_integer()) :: {:ok, [Path.t()]} | {:error, term()}
  def sweep_spool(age_ms) when is_integer(age_ms) and age_ms >= 0 do
    dir = spool_dir()
    cutoff = System.os_time(:second) - div(age_ms, 1000)

    case File.ls(dir) do
      {:ok, names} -> {:ok, sweep(dir, names, cutoff)}
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  # Only the two prefixes the API mints, so a sweep can never take a file some
  # other part of the system put in the data directory's `tmp/`.
  defp sweep(dir, names, cutoff) do
    names
    |> Enum.filter(&(String.starts_with?(&1, "insert-") or String.starts_with?(&1, "load-")))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.filter(&(Store.staged_at(&1) <= cutoff and File.rm(&1) == :ok))
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

  # `inserted` is the batch's own accepted row count rather than the ack's,
  # because a JSON flush groups every request in its window into one segment and
  # the ack's `row_count` is that whole group's. The validator counted these rows
  # here, one request at a time, so this number is already per-request and
  # already true.
  defp write(runtime, table_ref, batch, errors) do
    with {:ok, _ack} <- commit(runtime, table_ref, batch) do
      measure(batch.row_count, errors)

      {:ok, %{inserted: batch.row_count, errors: errors}}
    end
  end

  # The spooled path has no such count to fall back on — nothing parsed a row on
  # the way in, and the line count the edge took while streaming the body is a
  # newline tally, not a row tally. So the ack is the answer: its `row_count` is
  # the Parquet footer's, read back from the segment the flush wrote, and it is
  # this request's alone because `TableBuffer` never groups a spooled body with
  # another. A retry that dedups is answered with the original commit's count,
  # which is the same batch and so the same number.
  defp write_spooled(runtime, table_ref, batch) do
    with {:ok, ack} <- commit(runtime, table_ref, batch) do
      measure(ack.row_count, [])

      {:ok, %{inserted: ack.row_count, errors: []}}
    end
  end

  defp commit(runtime, table_ref, batch) do
    BufferService.Client.write_batch(runtime.buffer_name, table_ref, batch)
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
