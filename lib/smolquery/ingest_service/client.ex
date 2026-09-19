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
  alias Smolquery.IngestService.Runtime
  alias Smolquery.IngestService.SchemaCache
  alias Smolquery.IngestService.Validator
  alias Smolquery.Partitions
  alias Smolquery.RowBinary
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
    * `:skip_invalid_rows` — `true`, the default, writes the valid rows and
      reports the refused ones. `false` writes nothing when any row is
      refused, by the validator or by the flush, and answers
      `{:ok, %{inserted: 0, errors: errors}}` (T-474). A buffer node on a
      release that cannot honor it refuses the batch instead of writing part
      of it, answered `{:error, :whole_request_unsupported}`.
  """
  @spec insert(atom(), Store.table_ref(), [term()], keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert(name, table_ref, rows, opts \\ []) when is_list(rows) do
    with {:ok, runtime} <- runtime(name) do
      batch_id = Keyword.get(opts, :batch_id)
      skip? = Keyword.get(opts, :skip_invalid_rows, true)

      with_fresh_schema(
        runtime,
        table_ref,
        &validated_write(runtime, table_ref, &1, rows, {batch_id, skip?})
      )
    end
  end

  defp validated_write(runtime, table_ref, schema, rows, {_batch_id, skip?} = write_opts) do
    case Validator.validate(schema, rows) do
      {valid, errors} when valid == [] or (errors != [] and not skip?) ->
        measure(0, errors)

        {:ok, %{inserted: 0, errors: errors}}

      {valid, errors} ->
        write(runtime, table_ref, schema, valid, errors, write_opts)
    end
  end

  # The buffer refuses a batch whose column ids the catalog no longer gives
  # those names (T-439): this node's cached schema predates a DROP and ADD of
  # a name. The cache is dropped and the write is made once more against the
  # schema the catalog holds now — the rows are validated again, because the
  # re-added column may have another type. A second refusal is the caller's.
  defp with_fresh_schema(runtime, table_ref, write) do
    with {:ok, schema} <- SchemaCache.fetch(runtime, table_ref) do
      schema |> write.() |> retried_fresh(schema, runtime, table_ref, write)
    end
  end

  defp retried_fresh({:error, {:stale_schema, _ref, _names}}, _cached, runtime, table_ref, write) do
    :ok = SchemaCache.invalidate(runtime, table_ref)

    with {:ok, fresh} <- SchemaCache.fetch(runtime, table_ref), do: write.(fresh)
  end

  defp retried_fresh(
         {:error, {:invalid_rowbinary, _}} = refused,
         cached,
         runtime,
         table_ref,
         write
       ) do
    :ok = SchemaCache.invalidate(runtime, table_ref)

    case SchemaCache.fetch(runtime, table_ref) do
      {:ok, ^cached} -> refused
      {:ok, fresh} -> write.(fresh)
      {:error, _reason} -> refused
    end
  end

  defp retried_fresh(result, _cached, _runtime, _table_ref, _write), do: result

  @doc """
  Writes the NDJSON `body` to the table — `insert/4`'s contract, entered from
  bytes instead of decoded terms.

  The body goes to the owning buffer as the bytes the client sent; nothing here
  parses it (T-180). The flush validates what this node did not: rows the
  schema refuses come back at their index in this body, the way `insert/4`
  reports them, and the rest are durable. Takes the same `:batch_id` option,
  with the same dedup semantics, and `:skip_invalid_rows` as `insert/4` takes it.
  """
  @spec insert_ndjson(atom(), Store.table_ref(), binary(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert_ndjson(name, table_ref, body, opts \\ []) when is_binary(body) do
    with {:ok, runtime} <- runtime(name) do
      with_fresh_schema(runtime, table_ref, &forward_ndjson(runtime, table_ref, &1, body, opts))
    end
  end

  @doc """
  Writes a ClickHouse RowBinary `body` to the table: `insert_ndjson/4`'s
  contract, entered from RowBinary (T-476).

  The body is decoded on this node by `Smolquery.RowBinary.decode/4` into the
  NDJSON the flush reads, and forwarded as that. `format` is the decoder's.
  A body the decoder cannot read is `{:error, {:invalid_rowbinary, message}}`
  and writes nothing. Rows the decoder refuses and rows the flush refuses are
  both reported at their index in the RowBinary body.

  The decoder reads against the cached schema, so before an unreadable body's
  answer stands the cache is dropped and the catalog asked once: a body
  that names a column added since the schema was cached decodes against the
  fresh one. A plain `RowBinary` body with no column list cannot be checked
  that way, since it names nothing; a producer that adds columns sends the
  list, or a header format.

  ## Options

    * `:columns` — the names a plain `RowBinary` body's columns carry, in
      order, as an `INSERT` statement lists them
    * `:max_ndjson_bytes` — refuses a body whose rows decode past it, with
      `{:error, {:decoded_too_large, bytes, limit}}`. The decoder stops there,
      so `bytes` is how far it got, and nothing is forwarded
    * `:batch_id` and `:skip_invalid_rows`, as `insert/4` takes them
  """
  @spec insert_rowbinary(atom(), Store.table_ref(), binary(), RowBinary.format(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def insert_rowbinary(name, table_ref, body, format, opts \\ []) when is_binary(body) do
    with {:ok, runtime} <- runtime(name) do
      with_fresh_schema(
        runtime,
        table_ref,
        &forward_rowbinary(runtime, table_ref, &1, {body, format}, opts)
      )
    end
  end

  defp forward_rowbinary(runtime, table_ref, schema, {body, format}, opts) do
    started = System.monotonic_time(:microsecond)
    decoded = RowBinary.decode(schema, body, format, decode_opts(opts))
    decode_us = System.monotonic_time(:microsecond) - started
    skip? = Keyword.get(opts, :skip_invalid_rows, true)

    case decoded do
      {:ok, %{row_count: 0, errors: errors}} ->
        nothing_written(errors, decode_us)

      {:ok, %{errors: [_ | _] = errors}} when not skip? ->
        nothing_written(errors, decode_us)

      {:ok, %{ndjson: ndjson, row_count: count, errors: errors}} ->
        runtime
        |> forward(table_ref, schema, {IO.iodata_to_binary(ndjson), count}, decode_us, opts)
        |> merge_refused(errors, count)

      {:error, _reason} = error ->
        error
    end
  end

  defp nothing_written(errors, decode_us) do
    measure(0, errors, decode_us)

    {:ok, %{inserted: 0, errors: errors}}
  end

  defp decode_opts(opts) do
    opts
    |> Keyword.take([:columns])
    |> Keyword.put(:max_bytes, Keyword.get(opts, :max_ndjson_bytes))
  end

  defp merge_refused({:ok, result}, decode_errors, row_count),
    do: {:ok, %{result | errors: remapped(decode_errors, result.errors, row_count)}}

  defp merge_refused(error, _decode_errors, _row_count), do: error

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

    forward(runtime, table_ref, schema, {body, row_count}, count_us, opts)
  end

  defp forward(runtime, table_ref, schema, {body, row_count}, count_us, opts) do
    skip? = Keyword.get(opts, :skip_invalid_rows, true)
    batch_id = Keyword.get(opts, :batch_id)
    target = write_target(table_ref, schema, runtime, batch_id)

    batch =
      %{schema: schema, byte_size: byte_size(body)}
      |> Map.merge(ndjson_payload(body, row_count, skip?))
      |> with_batch_id(batch_id)

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

      {:error, reason} ->
        {:error, unsupported(reason, skip?)}
    end
  end

  # Non-blank lines, not newlines: the flush's `read_json` skips a blank line,
  # so counting it would ack a row that was never committed. Splitting keeps
  # this a pass over the bytes without a parse, and its indices are the ones
  # the salvage path reports errors at.
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

  defp write_target(table_ref, schema, runtime, batch_id) do
    partitions = Partitions.count(schema.partitions, runtime.write_partitions)

    Partitions.write_ref(table_ref, partitions, batch_id)
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

  defp write(runtime, table_ref, schema, valid, errors, {batch_id, skip?}) do
    batch = batch(schema, valid, batch_id, skip?)
    target = write_target(table_ref, schema, runtime, batch_id)

    case BufferService.Client.write_batch(runtime.buffer_name, target, batch) do
      {:ok, _ack} ->
        measure(length(valid), errors)

        {:ok, %{inserted: length(valid), errors: errors}}

      {:ok, _ack, refused} ->
        report(valid, errors, refused, length(valid) - length(refused))

      {:invalid, refused} ->
        report(valid, errors, refused, 0)

      {:error, reason} ->
        {:error, unsupported(reason, skip?)}
    end
  end

  # The flush refused rows the validator took (`Committer.salvage`); their
  # indices are positions in `valid`, so they are mapped back onto the caller's
  # body before they join the validator's own errors.
  defp report(valid, errors, refused, inserted) do
    all = remapped(errors, refused, length(valid))

    measure(inserted, all)

    {:ok, %{inserted: inserted, errors: all}}
  end

  # `refused` are indices among the rows that were forwarded, which skip the
  # rows `errors` already refused; they are mapped back onto the caller's body.
  defp remapped(errors, refused, forwarded) do
    rejected = MapSet.new(errors, & &1.index)

    original =
      0..(forwarded + length(errors) - 1)//1
      |> Enum.reject(&MapSet.member?(rejected, &1))
      |> List.to_tuple()

    Enum.sort_by(
      errors ++ Enum.map(refused, &%{&1 | index: elem(original, &1.index)}),
      & &1.index
    )
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

  defp batch(schema, rows, batch_id, true),
    do: with_batch_id(%{schema: schema, rows: rows}, batch_id)

  defp batch(schema, rows, batch_id, false),
    do: with_batch_id(%{schema: schema, whole_request: %{rows: rows}}, batch_id)

  defp ndjson_payload(body, row_count, true), do: %{ndjson: body, row_count: row_count}

  defp ndjson_payload(body, row_count, false),
    do: %{whole_request: %{ndjson: body, row_count: row_count}}

  defp with_batch_id(batch, nil), do: batch
  defp with_batch_id(batch, batch_id), do: Map.put(batch, :batch_id, batch_id)

  defp unsupported(:invalid_batch, false), do: :whole_request_unsupported
  defp unsupported(reason, _skip?), do: reason

  defp runtime(name) do
    case Runtime.fetch(name) do
      {:ok, runtime} -> {:ok, runtime}
      :error -> {:error, :ingest_service_unavailable}
    end
  end
end
