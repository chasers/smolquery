defmodule SmolqueryPg.Errors do
  @moduledoc """
  The SQLSTATE and message a query-path failure answers on the wire (PL-58).

  Postgres clients act on the five-character code: `psql` prints it with
  `\\set VERBOSITY verbose`, a driver raises a typed exception for it, and
  `postgres_fdw` retries or aborts on it. The message is for humans and
  never carries internals — a reason the clauses below do not know answers
  `XX000` with its inspected form, which is what the HTTP envelope does with
  a 500.

  DuckDB's own errors arrive as text prefixed with their class (`Parser
  Error`, `Binder Error`, `Catalog Error`). The prefix selects the code.

  An `ALTER TABLE` (PL-61 L3) fails with `Smolquery.Ddl`'s reasons, which
  take the codes Postgres gives the same refusals: `42703` for an unknown
  column, `42701` for a duplicate, `42704` for an unknown type, `42602` for
  a bad identifier, `42601` for a statement the parser does not accept,
  `42P17` for a `MATERIALIZED` expression the gates refuse, `55000` for a
  column that cannot be added or dropped as asked, and
  `0A000` for a clause it recognises and refuses or what this release does
  not do. The message is
  `Smolquery.Ddl.message/1`, the wording the HTTP envelope shares.
  """

  alias Smolquery.Ddl

  @type wire_error :: {code :: String.t(), message :: String.t()}

  @doc """
  The SQLSTATE and message for a failed job's `error`, or for a refusal from
  `Smolquery.QueryService.Client`.
  """
  @spec from_reason(term()) :: wire_error()
  def from_reason({:invalid_query, message}) when is_binary(message), do: {"42601", message}
  def from_reason({:invalid_query, _ast}), do: {"42601", "syntax error"}

  def from_reason(:multiple_statements),
    do: {"42601", "a query is one SELECT statement; send several statements one at a time"}

  def from_reason({:unknown_table, {dataset, table}}),
    do: {"42P01", ~s|relation "#{dataset}.#{table}" does not exist|}

  def from_reason({:unknown_table, name}), do: {"42P01", ~s|relation "#{name}" does not exist|}

  def from_reason({:unknown_connection, name}),
    do: {"42P01", ~s|connection "#{name}" does not exist|}

  def from_reason({:catalog_qualified_reference, reference}),
    do: {"42P01", "#{reference}: a table reference is dataset.table"}

  def from_reason({:unsupported_at_clause, table}),
    do: {"0A000", "#{table}: an AT clause is not supported; every query reads one snapshot"}

  def from_reason({:unsupported_table_function, name}),
    do: {"42501", "table function #{name} is not allowed"}

  def from_reason({:result_too_large, max}),
    do: {"54000", "result exceeded result_max_rows (#{max}); add a LIMIT or aggregate the query"}

  def from_reason({:hot_tier_unavailable, _ref, _reason}),
    do: {"57P03", "a buffer node the query needs did not answer"}

  def from_reason({:hot_tier_unavailable, _reason}),
    do: {"57P03", "a buffer node the query needs did not answer"}

  def from_reason({:pinned_hot_expired, age_ms, max_age_ms}),
    do:
      {"72000",
       "the transaction block is #{age_ms} ms old, past the server's #{max_age_ms} ms " <>
         "pinned-read lifetime; start a new one"}

  def from_reason({:pinned_hot_retired, {dataset, table}, _ids}),
    do:
      {"72000",
       "a hot segment the transaction pinned for #{dataset}.#{table} has been retired; " <>
         "the transaction block is too old, start a new one"}

  def from_reason({:unknown_column, _name} = reason), do: {"42703", Ddl.message(reason)}
  def from_reason({:duplicate_columns, _names} = reason), do: {"42701", Ddl.message(reason)}
  def from_reason({:unsupported_type, _type} = reason), do: {"42704", Ddl.message(reason)}
  def from_reason({:invalid_identifier, _name} = reason), do: {"42602", Ddl.message(reason)}

  def from_reason({:unsupported_ddl, _clause} = reason), do: {"0A000", Ddl.message(reason)}

  def from_reason({tag, _detail} = reason) when tag in [:invalid_ddl, :unqualified_table],
    do: {"42601", Ddl.message(reason)}

  def from_reason({tag, _detail} = reason)
      when tag in [
             :column_must_be_nullable,
             :clustering_column,
             :retention_column,
             :partition_ref
           ],
      do: {"55000", Ddl.message(reason)}

  def from_reason(:last_column), do: {"55000", Ddl.message(:last_column)}

  def from_reason({:materialized_source, _name, _dependent} = reason),
    do: {"55000", Ddl.message(reason)}

  def from_reason({:invalid_materialized, _detail} = reason), do: {"42P17", Ddl.message(reason)}

  def from_reason(reason)
      when reason in [
             :alter_table_unsupported,
             :ddl_not_explainable,
             :ddl_takes_no_params
           ],
      do: {"0A000", Ddl.message(reason)}

  def from_reason(:timeout), do: {"57014", "canceling statement due to statement timeout"}
  def from_reason(:cancelled), do: {"57014", "canceling statement due to user request"}
  def from_reason(:too_many_jobs), do: {"53000", "too many queries in flight; retry later"}

  def from_reason(:query_service_unavailable),
    do: {"57P03", "the query service is not available on this node"}

  def from_reason({:engine_failed, reason}), do: from_reason(reason)
  def from_reason({:statement_failed, _statement, reason}), do: from_reason(reason)
  def from_reason({:query_crashed, reason}), do: {"XX000", "query crashed: #{inspect(reason)}"}
  def from_reason({:engine_exit, reason}), do: {"XX000", "engine exited: #{inspect(reason)}"}

  def from_reason(message) when is_binary(message), do: {duckdb_code(message), message}
  def from_reason(%{message: message}) when is_binary(message), do: from_reason(message)
  def from_reason(reason), do: {"XX000", inspect(reason)}

  defp duckdb_code("Parser Error" <> _rest), do: "42601"
  defp duckdb_code("Binder Error" <> _rest), do: "42000"
  defp duckdb_code("Catalog Error" <> _rest), do: "42P01"
  defp duckdb_code("Conversion Error" <> _rest), do: "22000"
  defp duckdb_code("Out of Range Error" <> _rest), do: "22003"
  defp duckdb_code("Out of Memory Error" <> _rest), do: "53200"
  defp duckdb_code("Permission Error" <> _rest), do: "42501"
  defp duckdb_code(_message), do: "XX000"
end
