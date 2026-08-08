defmodule SmolqueryApi.InsertController do
  @moduledoc """
  The streaming-insert route's logic, over `Smolquery.IngestService.Client`.

  A 200 means the buffer service has every accepted row durable and queryable,
  and the body says which rows were not accepted (`insertErrors`, per-index) —
  partial failure is a successful response, BigQuery-style. Whole-request
  failures map to the envelope: an unknown table is a 404, a full buffer is a
  429 with `retry-after`, a service that is not running here is a 503.

  ## The NDJSON body is a different contract, and says so

  An `application/x-ndjson` body skips the parser entirely: it streams to a file
  and DuckDB reads it at flush. Nothing validates a row, so `insertErrors` is
  always empty and a value the schema cannot take fails the whole body with a
  400 instead of one index — weaker than what this route promises for JSON, and
  the reason it is refused outright (415) on a node whose buffer is not
  configured with `flush_writer: :duckdb`. Its idempotency key is the
  `x-smolquery-insert-id` header rather than `insertId`, under the same rules:
  non-empty, at most 128 bytes, or a 400.

  `insertedRows` on that path is the Parquet footer's count from the commit's
  own ack, so it is what landed rather than what was sent.

  The spooled file has one owner — the request handler that wrote it, which
  deletes it on every outcome. `Smolquery.IngestService.Client.sweep_spool/1` is
  the backstop for a handler that did not live to do it.
  """

  use SmolqueryApi, :controller

  alias Smolquery.IngestService
  alias SmolqueryApi.Errors
  alias SmolqueryApi.Json
  alias SmolqueryApi.Runtime

  @doc """
  Inserts the body's rows into a table.

  An optional `insertId` makes the request idempotent: retrying it — after a
  timeout, a dropped connection, or a 5xx whose write may still have landed —
  with the same id cannot double-count the rows. The id identifies *this
  batch*, so a retry must carry the same rows; BigQuery-style, best-effort
  scope: the window closes for as long as the batch's segment stays in the
  hot tier, which comfortably covers any retry loop.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, %{"dataset" => dataset, "table" => table}) do
    if ndjson?(conn) do
      spooled(conn, {dataset, table})
    else
      parsed(conn, {dataset, table})
    end
  end

  # `SmolqueryApi.Parsers` lists application/x-ndjson in `:pass`, so the body
  # arrives unread and unparsed. It goes to disk as it streams and DuckDB reads it
  # at flush — the point being that no row here becomes an Elixir term.
  #
  # The trade is explicit and is not the JSON route's: nothing validates per row,
  # so `insertErrors` is always empty and a value the schema cannot take fails the
  # whole body rather than one index.
  #
  # Both gates run before a byte is written. A node not configured with
  # `flush_writer: :duckdb` has no engine to read this body back, and an empty or
  # oversized idempotency key is a broken client — neither is worth spooling
  # megabytes to discover.
  defp spooled(conn, table_ref) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    with :ok <- IngestService.Client.check_file_writer(runtime.ingest_name),
         {:ok, batch_id} <- spooled_batch_id(conn) do
      spool_and_insert(conn, runtime, table_ref, batch_id)
    else
      {:error, reason} -> insert_error(conn, reason)
    end
  end

  # The spooled body has exactly one owner, and it is this function: it is deleted
  # on every outcome, including the ones that are not returns — a client that
  # disconnects mid-upload, and a `GenServer.call` that gives up at
  # `write_timeout_ms` and exits in this process. Handing ownership to the buffer
  # is what leaked before: four of the buffer's `{:ok, _}` answers are dedup hits
  # that never took the file, and nothing swept what they left.
  #
  # Deleting here cannot race the flush. The reply arrives after the commit that
  # wrote this body, and `Smolquery.BufferService.TableBuffer` never groups a
  # spooled body with another, so no other request's flush is still holding it.
  # `Smolquery.IngestService.Client.sweep_spool/1` is the backstop for the one
  # case an `after` cannot cover: a handler killed outright, or a node that went
  # down mid-upload.
  defp spool_and_insert(conn, runtime, table_ref, batch_id) do
    path = spool_path()

    try do
      case write_body(conn, path, SmolqueryApi.Parsers.max_body_bytes()) do
        {:ok, conn, 0} ->
          Json.send_json(conn, 200, %{"insertedRows" => 0, "insertErrors" => []})

        {:ok, conn, rows} ->
          insert_spooled(conn, runtime, table_ref, path, rows, batch_id)

        {:error, conn, :too_large} ->
          Errors.send_error(
            conn,
            413,
            "REQUEST_TOO_LARGE",
            "request body exceeds #{SmolqueryApi.Parsers.max_body_bytes()} bytes; split the batch"
          )

        {:error, conn, :read_body_failed} ->
          Errors.send_error(
            conn,
            400,
            "INVALID_ARGUMENT",
            "the request body ended before it was fully read; nothing was written"
          )
      end
    after
      File.rm(path)
    end
  end

  defp insert_spooled(conn, runtime, table_ref, path, rows, batch_id) do
    case IngestService.Client.insert_file(runtime.ingest_name, table_ref, path, rows,
           batch_id: batch_id
         ) do
      {:ok, result} ->
        # `insertedRows` is the Parquet footer's count, carried back on the ack —
        # not the newline tally taken while the body streamed past. The two
        # disagree over blank lines, over trailing whitespace, and over a last
        # line with no newline, and only one of them is what the manifest holds.
        Json.send_json(conn, 200, %{
          "insertedRows" => result.inserted,
          "insertErrors" => []
        })

      # Two shapes, one cause. The `COPY` runs inside `Store.put/3`'s encoder, so
      # its failure comes back wrapped as the store's; the two reads that follow
      # it — the footer count and the stats — fail bare. Both mean the same thing
      # to the client and both name the offending line.
      {:error, {:put_failed, _key, {:ndjson_copy_failed, message}}} ->
        copy_failed(conn, path, message)

      {:error, {:ndjson_copy_failed, message}} ->
        copy_failed(conn, path, message)

      {:error, reason} ->
        insert_error(conn, reason)
    end
  end

  # A line DuckDB cannot read into the table's schema fails the statement, so no
  # row of this body was written. The caller is told that, and told which line,
  # rather than given a bare 500: it is the only party that can fix it, and this
  # route has no per-index `insertErrors` to say it with. The spool path is
  # scrubbed out first — it names this node's data directory, which is not the
  # client's business.
  #
  # No other request is affected. One spooled body is one `COPY`, so a poison line
  # cannot fail a co-grouped tenant's write, which is what it did while flushes
  # were grouped.
  defp copy_failed(conn, path, message) do
    Errors.send_error(
      conn,
      400,
      "INVALID_ARGUMENT",
      "the body could not be read into the table's schema, and no row was written: " <>
        String.replace(message, path, "the request body")
    )
  end

  # The same rule the JSON route's `insertId` gets, for the same reason and then
  # one more. An empty header value is not "no id": it is a real dedup key, so
  # every client that emits the header unconditionally and leaves it blank
  # collapses into one slot per table and every batch after the first is answered
  # 200 with rows that were never written. And the id is persisted — into the
  # manifest log and the resident ETS dedup table — so its length is a client's
  # to choose only up to a bound.
  defp spooled_batch_id(conn) do
    case get_req_header(conn, "x-smolquery-insert-id") do
      [] -> {:ok, nil}
      [id | _rest] when is_binary(id) and id != "" and byte_size(id) <= 128 -> {:ok, id}
      _invalid -> {:error, {:invalid_param, "x-smolquery-insert-id"}}
    end
  end

  defp ndjson?(conn) do
    conn
    |> get_req_header("content-type")
    |> Enum.any?(&String.starts_with?(&1, "application/x-ndjson"))
  end

  defp write_body(conn, path, max_bytes) do
    File.mkdir_p!(Path.dirname(path))

    File.open!(path, [:write, :raw, :binary], fn file ->
      copy_body(conn, file, max_bytes, 0, :none)
    end)
  end

  # Lines are counted while the bytes stream past, because counting them later
  # would mean a second pass over the file. The number is an estimate and is used
  # as one: it is what the buffer's admission bounds measure the request as, while
  # what the client is told landed comes back on the ack from the Parquet footer.
  #
  # A body whose last line has no newline still counts, hence the tail adjustment
  # in `rows/2`.
  defp copy_body(conn, file, budget, newlines, last) do
    case read_body(conn, length: 8_000_000) do
      {:ok, chunk, conn} ->
        if byte_size(chunk) > budget do
          {:error, conn, :too_large}
        else
          :ok = IO.binwrite(file, chunk)

          {:ok, conn, rows(newlines + count_newlines(chunk), last_byte(chunk, last))}
        end

      {:more, chunk, conn} ->
        case budget - byte_size(chunk) do
          exhausted when exhausted < 0 ->
            {:error, conn, :too_large}

          remaining ->
            :ok = IO.binwrite(file, chunk)

            copy_body(
              conn,
              file,
              remaining,
              newlines + count_newlines(chunk),
              last_byte(chunk, last)
            )
        end

      # A client that disconnects mid-upload, or a socket that times out. Handled
      # rather than left to fall off the `case`, because falling off raised inside
      # the function `File.open/3` runs — past the branch that answers the request
      # and past the one that removes what was written so far.
      {:error, _reason} ->
        {:error, conn, :read_body_failed}
    end
  end

  # The tail adjustment asks the last byte of the *body*, not of the last chunk:
  # `Plug.Conn.read_body/2` routinely ends a multi-chunk read with an empty
  # `{:ok, ""}`, and asking that chunk whether the body ended in a newline
  # under-counted every such body by a row.
  defp last_byte(<<>>, previous), do: previous
  defp last_byte(chunk, _previous), do: :binary.last(chunk)

  defp rows(0, :none), do: 0
  defp rows(newlines, ?\n), do: newlines
  defp rows(newlines, _unterminated), do: newlines + 1

  defp count_newlines(chunk) do
    chunk |> :binary.matches("\n") |> length()
  end

  defp spool_path do
    Path.join(IngestService.Client.spool_dir(), "insert-#{System.unique_integer([:positive])}")
  end

  defp parsed(conn, {dataset, table}) do
    with {:ok, rows} <- payload(conn.body_params),
         {:ok, batch_id} <- insert_id(conn.body_params),
         {:ok, result} <- insert_rows(conn, {dataset, table}, rows, batch_id) do
      Json.send_json(conn, 200, %{
        "insertedRows" => result.inserted,
        "insertErrors" => errors_json(result.errors)
      })
    else
      {:error, reason} -> insert_error(conn, reason)
    end
  end

  @doc """
  Maps a whole-request write failure to the envelope; shared with batch loads.
  """
  @spec insert_error(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def insert_error(conn, :buffer_full) do
    conn
    |> put_resp_header("retry-after", "1")
    |> Errors.send_error(429, "RESOURCE_EXHAUSTED", "buffer full, retry later")
  end

  def insert_error(conn, {:overloaded, predicted_ms}) do
    conn
    |> put_resp_header("retry-after", Integer.to_string(max(ceil(predicted_ms / 1000), 1)))
    |> Errors.send_error(
      429,
      "RESOURCE_EXHAUSTED",
      "write path overloaded, ~#{predicted_ms} ms behind; retry later"
    )
  end

  def insert_error(conn, {:spooled_inserts_unsupported, writer}) do
    Errors.send_error(
      conn,
      415,
      "UNSUPPORTED_MEDIA_TYPE",
      "an application/x-ndjson insert body needs " <>
        "`config :smolquery, Smolquery.BufferService, flush_writer: :duckdb`; " <>
        "this node is configured with flush_writer: #{inspect(writer)}, " <>
        "which has no writer that can read one — send application/json instead"
    )
  end

  def insert_error(conn, {:column_longer_than_row_count, name, length, count}) do
    Errors.send_error(
      conn,
      400,
      "INVALID_ARGUMENT",
      "column #{inspect(name)} has #{length} values but rowCount is #{count}; " <>
        "nothing was inserted"
    )
  end

  def insert_error(conn, reason)
      when reason in [:buffer_service_unavailable, :ingest_service_unavailable] do
    Errors.send_error(conn, 503, "UNAVAILABLE", "the write path is not available here")
  end

  def insert_error(conn, reason)
      when reason in [:not_owner, :ownership_settling, :ring_config_stale, :draining] do
    conn
    |> put_resp_header("retry-after", "1")
    |> Errors.send_error(503, "UNAVAILABLE", "table ownership is moving; retry")
  end

  def insert_error(conn, reason), do: Errors.from_reason(conn, reason)

  defp insert_rows(conn, table_ref, rows, batch_id) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)

    IngestService.Client.insert(runtime.ingest_name, table_ref, rows, batch_id: batch_id)
  end

  defp insert_id(%{"insertId" => id}) when is_binary(id) and id != "" and byte_size(id) <= 128,
    do: {:ok, id}

  defp insert_id(%{"insertId" => _invalid}), do: {:error, {:invalid_param, "insertId"}}
  defp insert_id(_body), do: {:ok, nil}

  @doc """
  The `insertErrors` JSON shape of validator rejections; shared with loads.
  """
  @spec errors_json([Smolquery.IngestService.Validator.row_errors()]) :: [map()]
  def errors_json(errors) do
    Enum.map(errors, fn %{index: index, errors: messages} ->
      %{"index" => index, "errors" => Enum.map(messages, &%{"message" => &1.message})}
    end)
  end

  defp payload(%{"rows" => rows}) when is_list(rows), do: {:ok, rows}

  # The columnar body: one entry per column instead of one object per row, which
  # for a wide table is about half the bytes because the column names are sent
  # once rather than once per row. Same data, same rows, nothing dropped.
  #
  # `rowCount` is the client's own statement of how many rows it is sending, and
  # it is required rather than inferred from the longest column. Inferring it
  # would silently accept a body whose columns disagree — the failure that shape
  # is prone to — and turn a truncated upload into a batch of nulls.
  defp payload(%{"columns" => columns, "rowCount" => count})
       when is_map(columns) and is_integer(count) and count >= 0 do
    with :ok <- lists?(columns),
         :ok <- fits?(columns, count) do
      {:ok, {:columns, columns, count}}
    end
  end

  defp payload(%{"columns" => _columns}), do: {:error, {:missing_field, "rowCount"}}
  defp payload(_body), do: {:error, {:missing_field, "rows"}}

  defp lists?(columns) do
    if Enum.all?(columns, fn {_name, values} -> is_list(values) end),
      do: :ok,
      else: {:error, {:invalid_param, "columns"}}
  end

  # A column *shorter* than `rowCount` is padded with nulls, which is what an
  # absent key means row-major and is a value the schema then checks like any
  # other. A column *longer* than it is not a row to drop, it is a framing error:
  # the client and this server disagree about how many rows the body contains, and
  # the only two readings — trust the count and discard the tail, or trust the
  # column and invent values for the others — are both silent data loss behind a
  # 200. Refusing is what makes `rowCount` worth requiring; the comment above says
  # inferring it would accept a body whose columns disagree, and truncating is how
  # that happened anyway.
  defp fits?(columns, count) do
    case Enum.find(columns, fn {_name, values} -> length(values) > count end) do
      nil -> :ok
      {name, values} -> {:error, {:column_longer_than_row_count, name, length(values), count}}
    end
  end
end
