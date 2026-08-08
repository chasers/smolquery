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

  alias Explorer.DataFrame
  alias Smolquery.BufferService
  alias Smolquery.IngestService.ColumnarValidator
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
  """
  @spec insert(atom(), Store.table_ref(), [term()], keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert(name, table_ref, rows, opts \\ []) when is_list(rows) do
    with {:ok, runtime} <- runtime(name),
         {:ok, schema} <- SchemaCache.fetch(runtime, table_ref) do
      case Validator.validate(schema, rows) do
        {[], errors} ->
          measure(0, errors)

          {:ok, %{inserted: 0, errors: errors}}

        {valid, errors} ->
          write(runtime, table_ref, schema, valid, errors, Keyword.get(opts, :batch_id))
      end
    end
  end

  @doc """
  Validates NDJSON `body` against the table's schema and writes the valid
  rows — `insert/4`'s contract, entered from bytes instead of decoded terms.

  The fast path never materializes rows: `ColumnarValidator` parses and
  casts the whole body in one native pass and the resulting frame rides to
  the buffer as a frame (T-139). Any batch the columnar pass cannot prove
  entirely valid falls back to decoding the lines and running the per-row
  validator, so `insertErrors` reporting is byte-for-byte what `insert/4`
  answers — a line that is not a JSON object is rejected at its index like
  any other invalid row.

  Takes the same `:batch_id` option, with the same dedup semantics.
  """
  @spec insert_ndjson(atom(), Store.table_ref(), binary(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert_ndjson(name, table_ref, body, opts \\ []) when is_binary(body) do
    with {:ok, runtime} <- runtime(name),
         {:ok, schema} <- SchemaCache.fetch(runtime, table_ref) do
      case ColumnarValidator.validate(schema, body) do
        {:ok, frame} -> write_frame(runtime, table_ref, schema, frame, byte_size(body), opts)
        :fallback -> insert_decoded_lines(runtime, table_ref, schema, body, opts)
      end
    end
  end

  defp insert_decoded_lines(runtime, table_ref, schema, body, opts) do
    case Validator.validate(schema, decode_lines(body)) do
      {[], errors} ->
        measure(0, errors)

        {:ok, %{inserted: 0, errors: errors}}

      {valid, errors} ->
        write(runtime, table_ref, schema, valid, errors, Keyword.get(opts, :batch_id))
    end
  end

  defp decode_lines(body) do
    body
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      case JSON.decode(line) do
        {:ok, row} -> row
        {:error, _reason} -> line
      end
    end)
  end

  defp write_frame(runtime, table_ref, schema, frame, byte_size, opts) do
    batch = %{schema: schema, frame: frame, byte_size: byte_size}

    batch =
      case Keyword.get(opts, :batch_id) do
        nil -> batch
        batch_id -> Map.put(batch, :batch_id, batch_id)
      end

    with {:ok, _ack} <- BufferService.Client.write_batch(runtime.buffer_name, table_ref, batch) do
      inserted = DataFrame.n_rows(frame)
      measure(inserted, [])

      {:ok, %{inserted: inserted, errors: []}}
    end
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

  defp write(runtime, table_ref, schema, valid, errors, batch_id) do
    batch = batch(schema, valid, batch_id)

    with {:ok, _ack} <- BufferService.Client.write_batch(runtime.buffer_name, table_ref, batch) do
      measure(length(valid), errors)

      {:ok, %{inserted: length(valid), errors: errors}}
    end
  end

  defp measure(accepted, errors) do
    :telemetry.execute(
      [:smolquery, :ingest, :insert],
      %{accepted: accepted, rejected: length(errors)},
      %{}
    )
  end

  defp batch(schema, rows, nil), do: %{schema: schema, rows: rows}
  defp batch(schema, rows, batch_id), do: %{schema: schema, rows: rows, batch_id: batch_id}

  defp runtime(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> {:ok, runtime}
      :error -> {:error, :ingest_service_unavailable}
    end
  end
end
