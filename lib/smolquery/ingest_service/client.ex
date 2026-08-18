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
  alias Smolquery.Partitions
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
      if runtime.ndjson_passthrough do
        forward_ndjson(runtime, table_ref, schema, body, opts)
      else
        parse_and_write(runtime, table_ref, schema, body, opts)
      end
    end
  end

  # The body goes to the owning buffer as the bytes the client sent. Nothing here
  # parses it, so this node spends no CPU per row and the frame never exists to be
  # serialized — T-182 measured those two as 32% and 1% of the ack.
  #
  # The row count is the non-blank line count, which is one pass over the bytes
  # rather than a parse, and is what the ack reports. The trade is stated in
  # `insert_ndjson/4`'s docs: nothing validates per row on this path, so
  # `insertErrors` is always empty and a value the schema cannot take fails the
  # whole flush instead of one row.
  defp forward_ndjson(runtime, table_ref, schema, body, opts) do
    started = System.monotonic_time(:microsecond)
    row_count = count_rows(body)
    count_us = System.monotonic_time(:microsecond) - started

    batch = %{
      schema: schema,
      ndjson: body,
      row_count: row_count,
      byte_size: byte_size(body)
    }

    batch_id = Keyword.get(opts, :batch_id)
    target = write_target(table_ref, schema, runtime, batch_id)

    batch =
      case batch_id do
        nil -> batch
        batch_id -> Map.put(batch, :batch_id, batch_id)
      end

    written_at = System.monotonic_time(:microsecond)
    written = BufferService.Client.write_batch(runtime.buffer_name, target, batch)
    write_us = System.monotonic_time(:microsecond) - written_at

    case written do
      {:ok, _ack} ->
        measure(row_count, [], count_us, write_us)

        {:ok, %{inserted: row_count, errors: []}}

      # The flush validated what this node did not. Rows the schema refused are
      # reported at their index in this body, the way `insert/4` reports them,
      # and the rest are durable.
      {:ok, _ack, errors} ->
        inserted = row_count - length(errors)
        measure(inserted, errors, count_us, write_us)

        {:ok, %{inserted: inserted, errors: errors}}

      {:invalid, errors} ->
        measure(0, errors, count_us, write_us)

        {:ok, %{inserted: 0, errors: errors}}

      # The owning buffer's flush writer cannot take unparsed bytes — its config
      # and this node's disagree. Parsing here costs what passthrough saves, but
      # answers the request instead of failing it over a deployment seam.
      {:error, :ndjson_unsupported} ->
        parse_and_write(runtime, table_ref, schema, body, opts)

      {:error, _reason} = error ->
        error
    end
  end

  # Non-blank lines, not newlines: the flush's `read_json` skips a blank line,
  # so counting it would ack a row that was never committed. Splitting keeps
  # this a pass over the bytes without a parse, and its indices are the ones
  # `decode_lines/1` and the salvage path report errors at.
  # Only a line carrying non-whitespace is a row. The flush's reader skips a
  # blank line rather than parse it, and since the zero-row guard a commit of
  # nothing lands nothing — so counting `" "` (or the `"\r"` a CRLF body leaves
  # on every line) would ack rows the table never gained. `String.trim/1`
  # returns a sub-binary, so this stays one pass over the bytes with no copy.
  defp count_rows(body) do
    body
    |> String.split("\n", trim: true)
    |> Enum.count(&(String.trim(&1) != ""))
  end

  defp parse_and_write(runtime, table_ref, schema, body, opts) do
    # The parse is timed separately from the write because they are the two
    # halves of the ack that live on this side of the wire, and T-181 left
    # ~400ms of it unattributed (T-182).
    started = System.monotonic_time(:microsecond)
    validated = ColumnarValidator.validate(schema, body)
    parse_us = System.monotonic_time(:microsecond) - started

    case validated do
      {:ok, frame} ->
        write_frame(runtime, table_ref, schema, frame, byte_size(body), opts, parse_us)

      :fallback ->
        insert_decoded_lines(runtime, table_ref, schema, body, opts)
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

  defp write_target(table_ref, schema, runtime, batch_id) do
    partitions = Partitions.count(schema.partitions, runtime.write_partitions)

    Partitions.write_ref(table_ref, partitions, batch_id)
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

  defp write_frame(runtime, table_ref, schema, frame, byte_size, opts, parse_us) do
    batch = %{schema: schema, frame: frame, byte_size: byte_size}
    batch_id = Keyword.get(opts, :batch_id)
    target = write_target(table_ref, schema, runtime, batch_id)

    batch =
      case batch_id do
        nil -> batch
        batch_id -> Map.put(batch, :batch_id, batch_id)
      end

    started = System.monotonic_time(:microsecond)
    written = BufferService.Client.write_batch(runtime.buffer_name, target, batch)
    write_us = System.monotonic_time(:microsecond) - started

    with {:ok, _ack} <- written do
      inserted = DataFrame.n_rows(frame)
      measure(inserted, [], parse_us, write_us)

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
    target = write_target(table_ref, schema, runtime, batch_id)

    with {:ok, _ack} <- BufferService.Client.write_batch(runtime.buffer_name, target, batch) do
      measure(length(valid), errors)

      {:ok, %{inserted: length(valid), errors: errors}}
    end
  end

  defp measure(accepted, errors, parse_us \\ 0, write_us \\ 0) do
    :telemetry.execute(
      [:smolquery, :ingest, :insert],
      %{
        accepted: accepted,
        rejected: length(errors),
        parse_us: parse_us,
        write_us: write_us
      },
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
