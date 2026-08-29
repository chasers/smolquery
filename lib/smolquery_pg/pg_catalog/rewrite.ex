defmodule SmolqueryPg.PgCatalog.Rewrite do
  @moduledoc """
  Bridges the Postgres dialect of the catalog corpus onto DuckDB (PL-58).

  The catalog queries clients send are a known corpus — `psql`'s backslash
  commands, a driver's type bootstrap, `postgres_fdw`'s import — written in
  Postgres dialect. DuckDB's own parser does most of the work: after a
  small textual pre-pass, `json_serialize_sql` yields the statement's AST
  (which classification reads) and `json_deserialize_sql` yields a
  canonical DuckDB form (which runs). Regex lives only where the parser
  cannot go: on constructs DuckDB refuses to parse at all.

  ## `pre/2` — before the parse

  Each of these is a parse error or a wrong binding in DuckDB:

  - `OPERATOR(pg_catalog.~)` does not parse; it becomes the bare operator.
  - `pg_catalog.` qualifications (including `::pg_catalog.text` casts) do
    not parse; they drop. The emulated tables live unqualified in `main`.
  - A name literal cast to `regclass` or `regtype` (`'pg_class'::regclass`,
    DBeaver's join shape) becomes the catalog lookup it means: a subquery
    for the `oid` whose `relname` or `typname` is the literal's last dotted
    component. The other `reg*` and `oid` cast targets do not exist; they
    become `BIGINT`. `name` and `"char"` become `VARCHAR`.
  - `COLLATE <name>` names collations DuckDB does not have; it drops.
  - `pg_partition_ancestors(x) WITH ORDINALITY` does not parse; it becomes
    an empty two-column subquery.
  - `current_setting('x')` and the session constants (`current_database()`,
    `current_user`, ...) parse, but would bind to DuckDB's builtins and
    answer DuckDB's values; they become literals from the session.

  ## `post/1` — after the parse

  DuckDB parses `a ~ 'p'` as `regexp_full_match`, but Postgres's `~`
  matches anywhere: the canonical text's `regexp_full_match(` becomes
  `regexp_matches(`. `information_schema.x` becomes the `is_x` view —
  after classification, which must still see the real schema name, and
  textually because DuckDB reserves the `information_schema` name.
  """

  alias SmolqueryPg.Sql

  @doc """
  The textual pre-pass: `sql` with the constructs DuckDB cannot parse (or
  would bind wrongly) rewritten, `settings` supplying the session values.
  Strings, quoted identifiers, and comments are never touched
  (`SmolqueryPg.Sql`).
  """
  @spec pre(String.t(), %{String.t() => String.t()}) :: String.t()
  def pre(sql, settings) do
    sql
    |> Sql.tokens()
    |> rewrite_reg_literals([])
    |> rewrite_setting_calls(settings, [])
    |> Enum.map(fn
      {:code, code} -> pre_code(code)
      {_kind, text} -> text
    end)
    |> IO.iodata_to_binary()
  end

  defp pre_code(code) do
    code
    |> strip_operator_calls()
    |> strip_qualifications()
    |> rewrite_session_constants()
    |> strip_collate()
    |> rewrite_casts()
    |> rewrite_partition_ancestors()
  end

  defp rewrite_setting_calls(
         [{:code, code}, {:string, string}, {:code, tail} | rest],
         settings,
         acc
       ) do
    with [_all, head] <- Regex.run(~r/^(.*)\bcurrent_setting\s*\(\s*$/is, code),
         [_all, tail_rest] <- Regex.run(~r/^\s*(?:,\s*(?:true|false)\s*)?\)(.*)$/is, tail) do
      value = setting_literal(settings, String.trim(string, "'"))

      rewrite_setting_calls([{:code, tail_rest} | rest], settings, [{:code, head <> value} | acc])
    else
      nil ->
        rewrite_setting_calls([{:string, string}, {:code, tail} | rest], settings, [
          {:code, code} | acc
        ])
    end
  end

  defp rewrite_setting_calls([token | rest], settings, acc),
    do: rewrite_setting_calls(rest, settings, [token | acc])

  defp rewrite_setting_calls([], _settings, acc), do: Enum.reverse(acc)

  defp setting_literal(settings, name),
    do: "'" <> String.replace(setting_value(settings, name), "'", "''") <> "'"

  @reg_lookups %{
    "regclass" => {"pg_class", "relname"},
    "regtype" => {"pg_type", "typname"}
  }

  defp rewrite_reg_literals([{:string, string}, {:code, code} | rest], acc) do
    case Regex.run(~r/^::\s*(regclass|regtype)\b(.*)$/is, code) do
      [_all, target, tail] ->
        {table, column} = Map.fetch!(@reg_lookups, String.downcase(target))
        name = string |> String.trim("'") |> String.split(".") |> List.last()
        lookup = "(SELECT oid FROM #{table} WHERE #{column} = '#{name}')"

        rewrite_reg_literals([{:code, tail} | rest], [{:code, lookup} | acc])

      nil ->
        rewrite_reg_literals([{:code, code} | rest], [{:string, string} | acc])
    end
  end

  defp rewrite_reg_literals([token | rest], acc), do: rewrite_reg_literals(rest, [token | acc])
  defp rewrite_reg_literals([], acc), do: Enum.reverse(acc)

  @doc """
  The post-parse pass over the canonical SQL `json_deserialize_sql`
  answered.
  """
  @spec post(String.t()) :: String.t()
  def post(canonical) do
    canonical
    |> Sql.tokens()
    |> post_tokens([])
    |> IO.iodata_to_binary()
  end

  @is_prefix "information_schema."
  @is_prefix_size byte_size(@is_prefix)

  defp post_tokens([{:code, code}, {:quoted, quoted} | rest], acc) do
    case split_information_schema(code) do
      {:ok, prefix} ->
        post_tokens(rest, [[post_code(prefix), "is_", String.trim(quoted, "\"")] | acc])

      :none ->
        post_tokens([{:quoted, quoted} | rest], [post_code(code) | acc])
    end
  end

  defp post_tokens([{:code, code} | rest], acc), do: post_tokens(rest, [post_code(code) | acc])
  defp post_tokens([{_kind, text} | rest], acc), do: post_tokens(rest, [text | acc])
  defp post_tokens([], acc), do: Enum.reverse(acc)

  defp split_information_schema(code) when byte_size(code) < @is_prefix_size, do: :none

  defp split_information_schema(code) do
    split = byte_size(code) - @is_prefix_size
    <<prefix::binary-size(^split), suffix::binary>> = code

    if String.downcase(suffix) == @is_prefix, do: {:ok, prefix}, else: :none
  end

  defp post_code(code) do
    code
    |> String.replace("regexp_full_match(", "regexp_matches(")
    |> rewrite_information_schema()
  end

  defp strip_operator_calls(code),
    do: Regex.replace(~r/OPERATOR\s*\(\s*pg_catalog\.(\S+?)\s*\)/i, code, " \\g{1} ")

  defp strip_qualifications(code), do: Regex.replace(~r/\bpg_catalog\./i, code, "")

  @server_settings %{
    "max_index_keys" => "32",
    "max_identifier_length" => "63",
    "server_version_num" => "140010",
    "block_size" => "8192",
    "segment_size" => "131072",
    "wal_block_size" => "8192",
    "data_checksums" => "off",
    "lc_collate" => "C",
    "lc_ctype" => "C",
    "bytea_output" => "hex",
    "default_transaction_read_only" => "off",
    "in_hot_standby" => "off"
  }

  defp setting_value(settings, name) do
    Map.get_lazy(settings, name, fn -> insensitive_setting(settings, name) end) ||
      server_setting(name)
  end

  defp insensitive_setting(settings, name) do
    lower = String.downcase(name)

    Enum.find_value(settings, nil, fn {key, value} ->
      if String.downcase(key) == lower, do: value
    end)
  end

  defp server_setting(name), do: Map.get(@server_settings, String.downcase(name), "")

  @session_constants [
    {~r/\bcurrent_database\s*\(\s*\)/i, "'smolquery'"},
    {~r/\bcurrent_schema\s*\(\s*\)/i, "'public'"},
    {~r/\bcurrent_user\b/i, "'smolquery'"},
    {~r/\bsession_user\b/i, "'smolquery'"},
    {~r/\bcurrent_role\b/i, "'smolquery'"}
  ]

  defp rewrite_session_constants(code) do
    Enum.reduce(@session_constants, code, fn {pattern, replacement}, code ->
      Regex.replace(pattern, code, replacement)
    end)
  end

  defp strip_collate(code),
    do: Regex.replace(~r/\bCOLLATE\s+(?:default\b|"[^"]+"|[\w.]+)/i, code, "")

  @bigint_casts ~w(regclass regtype regnamespace regproc regprocedure regoper regoperator regrole regconfig regdictionary oid xid cid tid)
  @varchar_casts ~w(name bpchar)

  defp rewrite_casts(code) do
    code = Regex.replace(~r/::\s*(#{Enum.join(@bigint_casts, "|")})\b/i, code, "::BIGINT")
    code = Regex.replace(~r/::\s*(#{Enum.join(@varchar_casts, "|")})\b/i, code, "::VARCHAR")

    Regex.replace(~r/::\s*"char"/i, code, "::VARCHAR")
  end

  defp rewrite_partition_ancestors(code) do
    Regex.replace(
      ~r/pg_partition_ancestors\s*\([^)]*\)\s*WITH\s+ORDINALITY/i,
      code,
      "(SELECT CAST(NULL AS BIGINT), CAST(NULL AS BIGINT) WHERE FALSE)"
    )
  end

  defp rewrite_information_schema(code) do
    Regex.replace(~r/\binformation_schema\."?(\w+)"?/i, code, "is_\\g{1}")
  end
end
