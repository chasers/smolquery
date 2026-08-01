defmodule Smolquery.Engine do
  @moduledoc """
  DuckDB read engine — a supervised `Adbc.Database` → `Adbc.Connection` subtree.

  The engine is a disposable, stateless reader: nothing here is storage of
  record. Segments live as Parquet on disk or object storage, and DuckDB is
  pointed at them per query. An engine can be thrown away and rebuilt at any
  time without data loss, which is why the default database is in-memory.

  The subtree is `rest_for_one`: the database starts first, connections after
  it, so a database crash rebuilds the connections that depend on it, while a
  connection crash leaves the database alone.

  This is shared infrastructure rather than part of a service — `QueryService`
  reads through it today and the sealer may later merge through it, so it sits
  outside the service layers that `.reach.exs` keeps apart.

  ## Configuration

      config :smolquery, Smolquery.Engine,
        memory_limit: "2GB",
        threads: System.schedulers_online(),
        extensions: [:httpfs, :json],
        max_result_rows: 100_000

  Extension names and settings are interpolated into SQL (DuckDB takes no
  parameters in `INSTALL`/`LOAD`/`SET`), so they are configuration-only inputs —
  never pass user-supplied values.

  ## Two result contracts, chosen by size

  `query/3` returns a `Smolquery.Engine.Result` — ordered columns and rows of plain
  Elixir terms. It is the right shape for the queries the system asks itself, all
  of which return tens of rows, and it refuses a result over `:max_result_rows`
  rather than spending gigabytes of heap converting one.

  `frame/3` returns an `Explorer.DataFrame`, with the Arrow stream going straight
  to Polars in Rust. That is the read path for user queries, whose size nobody
  controls.

  ## Usage

      {:ok, _pid} = Smolquery.Engine.start_link(name: MyEngine)
      {:ok, result} = Smolquery.Engine.query(MyEngine, "SELECT $1 + 1 AS n", [41])
      #=> %Smolquery.Engine.Result{columns: ["n"], rows: [[42]], num_rows: 1}

      {:ok, frame} = Smolquery.Engine.frame(MyEngine, "SELECT * FROM read_parquet($1)", [path])
      #=> #Explorer.DataFrame<...>

  """

  use Supervisor

  alias Smolquery.Engine.Connection
  alias Smolquery.Engine.Result

  @default_max_result_rows 100_000

  @type option ::
          {:name, atom()}
          | {:path, String.t()}
          | {:extensions, [atom() | String.t()]}
          | {:statements, [String.t()]}
          | {:memory_limit, String.t()}
          | {:threads, pos_integer()}
          | {:max_result_rows, pos_integer() | :infinity}

  @doc """
  Starts an engine subtree.

  ## Options

    * `:name` — base name for the subtree; the database, connection, and
      supervisor are registered beneath it. Defaults to `#{inspect(__MODULE__)}`.
    * `:path` — DuckDB database file. Defaults to in-memory.
    * `:statements` — SQL run once per connection after extensions and
      settings, for session state a caller cannot afford to lose on a restart
      (a catalog `ATTACH`, say).
    * `:max_result_rows` — most rows `query/3` will convert to Elixir terms
      before returning `Smolquery.Engine.ResultTooLarge`. Defaults to
      `#{@default_max_result_rows}`; `:infinity` disables the ceiling for a
      caller that means it.
    * `:extensions`, `:memory_limit`, `:threads` — override the application
      configuration for this instance.

  """
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, {name, opts}, name: supervisor_name(name))
  end

  @impl true
  def init({name, opts}) do
    config = Keyword.merge(Application.get_env(:smolquery, __MODULE__, []), opts)

    children = [
      {Adbc.Database, database_opts(name, config)},
      {Connection,
       database: database_name(name),
       name: connection_name(name),
       extensions: Keyword.get(config, :extensions, []),
       settings: settings(config),
       statements: Keyword.get(config, :statements, []),
       max_rows: Keyword.get(config, :max_result_rows, @default_max_result_rows)}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  Runs `sql` on the named engine with positional `params` bound to `$1..$n`.

  The engine name is always explicit: a leading default would make
  `query(sql, params)` and `query(name, sql)` indistinguishable.
  """
  @spec query(atom(), String.t(), [term()]) :: {:ok, Result.t()} | {:error, Exception.t()}
  def query(name, sql, params \\ []) do
    Connection.query(connection_name(name), sql, params)
  end

  @doc """
  Same as `query/3` but raises on error.
  """
  @spec query!(atom(), String.t(), [term()]) :: Result.t()
  def query!(name, sql, params \\ []) do
    case query(name, sql, params) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
    end
  end

  @doc """
  Runs `statements` in order inside one transaction: all commit, or the first
  failure rolls every prior statement back and is returned.

  See `Smolquery.Engine.Connection.transaction/3` for why the transaction runs
  inside the connection process and why statements carry no parameters.
  """
  @spec transaction(atom(), [String.t()]) :: :ok | {:error, Exception.t()}
  def transaction(name, statements) do
    Connection.transaction(connection_name(name), statements)
  end

  @doc """
  Runs `sql` and returns an `Explorer.DataFrame` instead of a `Result`.

  This is the read path for results too large to be worth Elixir terms. DuckDB's
  Arrow stream is handed to Polars in Rust, so nothing lands on the BEAM heap:
  five million rows take 307 ms and no measurable heap, against 11.5 s and 4.8 GiB
  through `query/3` (`bench/adbc.exs`). `:max_result_rows` does not apply, because
  the cost it guards against is not paid here.

  A frame serializes to Parquet, Arrow IPC, CSV, or NDJSON from Rust
  (`Explorer.DataFrame.dump_parquet/2` and friends), which keeps the whole path
  from DuckDB to response bytes clear of Elixir terms.

      {:ok, frame} = Smolquery.Engine.frame(MyEngine, "SELECT * FROM lake.a.events")
      {:ok, parquet} = Explorer.DataFrame.dump_parquet(frame)

  """
  @spec frame(atom(), String.t(), [term()]) ::
          {:ok, Explorer.DataFrame.t()} | {:error, Exception.t()}
  def frame(name, sql, params \\ []) do
    Connection.frame(connection_name(name), sql, params)
  end

  @doc """
  Same as `frame/3` but raises on error.
  """
  @spec frame!(atom(), String.t(), [term()]) :: Explorer.DataFrame.t()
  def frame!(name, sql, params \\ []) do
    case frame(name, sql, params) do
      {:ok, frame} -> frame
      {:error, reason} -> raise reason
    end
  end

  @doc """
  The DuckDB version reported by the engine.
  """
  @spec version(atom()) :: String.t()
  def version(name \\ __MODULE__) do
    name |> query!("SELECT version()") |> Result.one!()
  end

  @doc """
  The registered name of an engine's connection process.
  """
  @spec connection_name(atom()) :: atom()
  def connection_name(name), do: Module.concat(name, "Connection")

  @doc """
  The registered name of an engine's database process.
  """
  @spec database_name(atom()) :: atom()
  def database_name(name), do: Module.concat(name, "Database")

  @doc """
  The registered name of an engine's supervisor process.
  """
  @spec supervisor_name(atom()) :: atom()
  def supervisor_name(name), do: Module.concat(name, "Supervisor")

  defp database_opts(name, config) do
    base = [driver: :duckdb, process_options: [name: database_name(name)]]

    case Keyword.get(config, :path) do
      nil -> base
      path -> Keyword.put(base, :path, path)
    end
  end

  defp settings(config) do
    config
    |> Keyword.take([:memory_limit, :threads])
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end
end
