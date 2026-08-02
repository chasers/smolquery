defmodule Smolquery.Cluster.ConfigStore.Postgres do
  @moduledoc """
  Ring configuration in the same Postgres the deployment already depends on.

  Clustering only exists where `CATALOG_DATABASE_URL` does
  (`Smolquery.Cluster`), so fencing rides the connection the system already
  requires — no new failure domain, and the store's serializability is the
  database's own row lock: `advance/4` is a single `UPDATE ... WHERE epoch =
  $expected`, so of any number of concurrent proposals exactly one wins and
  the rest see `:conflict`.

  `age_ms` is computed inside the query from the database's clock
  (`now() - changed_at`), never from a node's, so two nodes reasoning about
  the same configuration age agree regardless of their wall clocks.

  Member lists are stored as comma-joined node names. Decoding calls
  `String.to_atom/1`: the input is this system's own fleet configuration,
  written exclusively by these nodes, bounded by fleet size — not user input.
  A freshly booted node must be able to decode peers it has not connected to
  yet, which rules out `String.to_existing_atom/1`.
  """

  @behaviour Smolquery.Cluster.ConfigStore

  @table "smolquery_ring_config"

  @impl Smolquery.Cluster.ConfigStore
  def start_link(opts) do
    Postgrex.start_link(opts)
  end

  @impl Smolquery.Cluster.ConfigStore
  def setup(conn) do
    ddl = """
    CREATE TABLE IF NOT EXISTS #{@table} (
      scope text PRIMARY KEY,
      epoch bigint NOT NULL,
      members text NOT NULL,
      prev_members text,
      changed_at timestamptz NOT NULL DEFAULT now()
    )
    """

    case Postgrex.query(conn, ddl, []) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Smolquery.Cluster.ConfigStore
  def fetch(conn, scope) do
    sql = """
    SELECT epoch, members, prev_members,
           (extract(epoch FROM (now() - changed_at)) * 1000)::bigint
      FROM #{@table} WHERE scope = $1
    """

    case Postgrex.query(conn, sql, [scope]) do
      {:ok, %Postgrex.Result{rows: [row]}} -> {:ok, decode(row)}
      {:ok, %Postgrex.Result{rows: []}} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Smolquery.Cluster.ConfigStore
  def ensure(conn, scope, members) do
    sql = """
    INSERT INTO #{@table} (scope, epoch, members)
    VALUES ($1, 0, $2)
    ON CONFLICT (scope) DO NOTHING
    """

    case Postgrex.query(conn, sql, [scope, encode(members)]) do
      {:ok, _result} ->
        case fetch(conn, scope) do
          {:ok, config} -> {:ok, config}
          :not_found -> {:error, :not_found}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Smolquery.Cluster.ConfigStore
  def advance(conn, scope, expected_epoch, members) do
    sql = """
    UPDATE #{@table}
       SET epoch = epoch + 1, prev_members = members, members = $3,
           changed_at = now()
     WHERE scope = $1 AND epoch = $2
    RETURNING epoch, members, prev_members, 0::bigint
    """

    case Postgrex.query(conn, sql, [scope, expected_epoch, encode(members)]) do
      {:ok, %Postgrex.Result{rows: [row]}} -> {:ok, decode(row)}
      {:ok, %Postgrex.Result{rows: []}} -> {:error, :conflict}
      {:error, reason} -> {:error, reason}
    end
  end

  defp encode(members) do
    members |> Enum.sort() |> Enum.map_join(",", &Atom.to_string/1)
  end

  defp decode([epoch, members, prev_members, age_ms]) do
    %{
      epoch: epoch,
      members: decode_members(members),
      prev_members: prev_members && decode_members(prev_members),
      age_ms: max(age_ms, 0)
    }
  end

  defp decode_members(text) do
    text |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)
  end
end
