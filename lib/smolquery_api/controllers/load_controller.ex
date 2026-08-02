defmodule SmolqueryApi.LoadController do
  @moduledoc """
  Batch loads: a file in the request body, the insert path underneath
  (PL-8 D10).

  The body — NDJSON, CSV, or Parquet, chosen by content type — spools to a
  temporary file under the data directory (bounded by `load_max_bytes`),
  parses to rows, and pushes chunks through `IngestService.Client.insert/3`:
  same validation, same durability point, same 429 backpressure as a
  streaming insert. A load is synchronous in v1, and it is not atomic — a
  whole-request failure partway through reports how many rows were already
  durable, and retrying re-inserts them.

  Rejected rows report their position in the file (row index, header
  excluded for CSV), capped at 100 reported errors with the true count
  alongside — a million-row file with a broken column does not get a
  million-entry response.

      curl -H "content-type: application/x-ndjson" --data-binary @events.ndjson \\
           .../v1/datasets/analytics/tables/events/load
  """

  use SmolqueryApi, :controller

  alias Explorer.DataFrame
  alias Smolquery.IngestService
  alias Smolquery.Schema
  alias SmolqueryApi.Errors
  alias SmolqueryApi.InsertController
  alias SmolqueryApi.Json
  alias SmolqueryApi.Runtime

  @chunk_rows 10_000
  @max_reported_errors 100

  @doc """
  Loads the body's file into a table.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, %{"dataset" => dataset, "table" => table}) do
    case format(conn) do
      {:ok, format} -> spool(conn, {dataset, table}, format)
      :error -> unsupported(conn)
    end
  end

  defp format(conn) do
    case List.first(Plug.Conn.get_req_header(conn, "content-type")) do
      "application/x-ndjson" <> _params -> {:ok, :ndjson}
      "text/csv" <> _params -> {:ok, :csv}
      "application/vnd.apache.parquet" <> _params -> {:ok, :parquet}
      _other -> :error
    end
  end

  defp unsupported(conn) do
    Errors.send_error(
      conn,
      415,
      "UNSUPPORTED_MEDIA_TYPE",
      "load bodies must be application/x-ndjson, text/csv, or application/vnd.apache.parquet"
    )
  end

  defp spool(conn, table_ref, format) do
    {:ok, runtime} = Runtime.fetch(conn.private.smolquery_api)
    path = spool_path()

    try do
      case write_body(conn, path, runtime.load_max_bytes) do
        {:ok, conn} -> parse_and_insert(conn, runtime, table_ref, format, path)
        {:error, conn, :too_large} -> too_large(conn, runtime)
      end
    after
      File.rm(path)
    end
  end

  defp too_large(conn, runtime) do
    Errors.send_error(
      conn,
      413,
      "PAYLOAD_TOO_LARGE",
      "load bodies are capped at #{runtime.load_max_bytes} bytes"
    )
  end

  defp parse_and_insert(conn, runtime, table_ref, format, path) do
    with {:ok, schema} <- table_schema(runtime, table_ref),
         {:ok, indexed_rows, parse_errors} <- parse(format, path, schema) do
      insert_chunks(conn, runtime, table_ref, indexed_rows, parse_errors)
    else
      {:error, {:parse_failed, message}} ->
        Errors.send_error(conn, 400, "INVALID_ARGUMENT", "could not parse file: #{message}")

      {:error, reason} ->
        Errors.from_reason(conn, reason)
    end
  end

  defp table_schema(runtime, table_ref) do
    case IngestService.Runtime.fetch(runtime.ingest_name) do
      {:ok, ingest} -> IngestService.SchemaCache.fetch(ingest, table_ref)
      :error -> {:error, :ingest_service_unavailable}
    end
  end

  defp insert_chunks(conn, runtime, table_ref, indexed_rows, parse_errors) do
    outcome =
      indexed_rows
      |> Stream.chunk_every(@chunk_rows)
      |> Enum.reduce_while({0, []}, fn chunk, {inserted, error_chunks} ->
        rows = Enum.map(chunk, fn {_index, row} -> row end)

        case IngestService.Client.insert(runtime.ingest_name, table_ref, rows) do
          {:ok, result} ->
            {:cont, {inserted + result.inserted, [reindex(result.errors, chunk) | error_chunks]}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case outcome do
      {:error, reason} ->
        InsertController.insert_error(conn, reason)

      {inserted, error_chunks} ->
        errors = parse_errors ++ (error_chunks |> Enum.reverse() |> List.flatten())

        Json.send_json(conn, 200, %{
          "insertedRows" => inserted,
          "errorCount" => length(errors),
          "insertErrors" =>
            errors |> Enum.take(@max_reported_errors) |> InsertController.errors_json()
        })
    end
  end

  defp reindex(errors, chunk) do
    positions = chunk |> Enum.map(fn {index, _row} -> index end) |> List.to_tuple()

    Enum.map(errors, &%{&1 | index: elem(positions, &1.index)})
  end

  defp parse(:ndjson, path, _schema) do
    {rows, errors} =
      path
      |> File.stream!()
      |> Stream.with_index()
      |> Enum.reduce({[], []}, fn {line, index}, {rows, errors} ->
        case decode_line(line) do
          :skip -> {rows, errors}
          {:ok, row} -> {[{index, row} | rows], errors}
          :error -> {rows, [line_error(index) | errors]}
        end
      end)

    {:ok, Enum.reverse(rows), Enum.reverse(errors)}
  end

  defp parse(:csv, path, schema) do
    with {:ok, dtypes} <- Schema.explorer_dtypes(schema),
         {:ok, frame} <- DataFrame.from_csv(path, dtypes: dtypes) do
      {:ok, indexed(frame), []}
    else
      {:error, %{} = error} -> {:error, {:parse_failed, Exception.message(error)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse(:parquet, path, _schema) do
    case DataFrame.from_parquet(path) do
      {:ok, frame} -> {:ok, indexed(frame), []}
      {:error, error} -> {:error, {:parse_failed, Exception.message(error)}}
    end
  end

  defp indexed(frame) do
    frame
    |> DataFrame.to_rows()
    |> Enum.with_index(fn row, index -> {index, row} end)
  end

  defp decode_line(line) do
    case String.trim(line) do
      "" ->
        :skip

      trimmed ->
        case JSON.decode(trimmed) do
          {:ok, row} when is_map(row) -> {:ok, row}
          _not_an_object -> :error
        end
    end
  end

  defp line_error(index) do
    %{index: index, errors: [%{message: "line is not a JSON object"}]}
  end

  defp write_body(conn, path, max_bytes) do
    File.mkdir_p!(Path.dirname(path))

    File.open!(path, [:write, :raw, :binary], fn file ->
      copy_body(conn, file, max_bytes)
    end)
  end

  defp copy_body(conn, file, budget) do
    case read_body(conn, length: 8_000_000) do
      {:ok, chunk, conn} ->
        if byte_size(chunk) > budget do
          {:error, conn, :too_large}
        else
          :ok = IO.binwrite(file, chunk)

          {:ok, conn}
        end

      {:more, chunk, conn} ->
        case budget - byte_size(chunk) do
          exhausted when exhausted < 0 ->
            {:error, conn, :too_large}

          remaining ->
            :ok = IO.binwrite(file, chunk)

            copy_body(conn, file, remaining)
        end
    end
  end

  defp spool_path do
    dir = Application.get_env(:smolquery, :data_dir, System.tmp_dir!())

    Path.join([dir, "tmp", "load-#{System.unique_integer([:positive])}"])
  end
end
