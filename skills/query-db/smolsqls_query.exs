#!/usr/bin/env elixir
#
# Query ANY smolsqls database over the HTTP query API — the Elixir tool the
# skills use instead of curl. Self-contained via Mix.install (no project
# compile needed); works from the repo or a globally-symlinked skill.
#
#   elixir skills/query-db/smolsqls_query.exs [opts] "SQL [...]"
#   elixir skills/query-db/smolsqls_query.exs [opts] --file path/to/schema.sql
#
# Options:
#   --db NAME         credential set to use (default: pm). Any name works.
#   --url URL         base URL override (non-secret)
#   --id ID           database id override (non-secret)
#   --env FILE        dotenv file to load first (default: per --db, below)
#   --args JSON       positional args bound to ? placeholders, e.g. --args '["x",1]'
#   --args-file FILE  read the JSON args array from FILE (use for large values,
#                     e.g. a plan's markdown body); mutually exclusive with --args
#   --file FILE       apply each ';'-separated statement in FILE (schema/migration)
#   --json            print the raw JSON response instead of a table
#
# Credentials come from the environment (never hardcode/commit a token). For
# `--db NAME`, the tool reads SMOLSQLS_<NAME>_URL / _DB_ID / _DB_TOKEN (NAME
# upper-cased, non-alphanumerics -> '_') and auto-loads a dotenv file:
#   pm    -> SMOLSQLS_PM_*     , .claude/smolquery-pm.env
#   alpha -> SMOLSQLS_ALPHA_*  , .claude/alpha-db.env
#   <x>   -> SMOLSQLS_<X>_*    , .claude/<x>.env
# The token must come from the environment (never passed on argv). URL defaults
# to https://alpha.smolsqls.com when unset.

Mix.install([{:req, "~> 0.6"}])

defmodule SmolsqlsQuery do
  @default_url "https://alpha.smolsqls.com"

  def main(argv) do
    {opts, rest, invalid} =
      OptionParser.parse(argv,
        strict: [
          db: :string,
          url: :string,
          id: :string,
          env: :string,
          args: :string,
          args_file: :string,
          file: :string,
          json: :boolean
        ]
      )

    unless invalid == [], do: die("unknown option(s): #{inspect(invalid)}")

    db = opts[:db] || "pm"
    prefix = prefix(db)
    load_env(opts[:env] || default_env(db))

    url = opts[:url] || System.get_env("#{prefix}_URL") || @default_url
    id = opts[:id] || System.get_env("#{prefix}_DB_ID") || die("#{prefix}_DB_ID is not set (or pass --id)")
    token = System.get_env("#{prefix}_DB_TOKEN") || die("#{prefix}_DB_TOKEN is not set")

    req =
      Req.new(
        base_url: url,
        headers: [{"authorization", "Bearer " <> token}],
        url: "/v1/databases/#{id}/query"
      )

    cond do
      opts[:file] -> apply_file(req, opts[:file])
      rest == [] -> die("no SQL given (pass a statement, or --file)")
      true -> run(req, Enum.join(rest, " "), args(opts), opts[:json])
    end
  end

  defp args(opts) do
    cond do
      opts[:args] && opts[:args_file] -> die("pass only one of --args / --args-file")
      opts[:args_file] -> opts[:args_file] |> File.read!() |> Jason.decode!()
      opts[:args] -> Jason.decode!(opts[:args])
      true -> []
    end
  end

  defp run(req, sql, args, json?) do
    case query(req, sql, args) do
      {:ok, data} -> if json?, do: IO.puts(Jason.encode!(%{data: data})), else: print_table(data)
      {:error, msg} -> die(msg)
    end
  end

  defp query(req, sql, args) do
    case Req.post(req, json: %{sql: sql, args: args}) do
      {:ok, %{status: s, body: %{"data" => data}}} when s in 200..299 ->
        {:ok, data}

      {:ok, %{body: %{"error" => %{"code" => c, "message" => m}}}} ->
        {:error, "#{c}: #{m}"}

      {:ok, %{status: s, body: body}} ->
        {:error, "HTTP #{s}: #{inspect(body)}"}

      {:error, e} ->
        {:error, "request failed: #{Exception.message(e)}"}
    end
  end

  defp apply_file(req, path) do
    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.reject(&(&1 =~ ~r/^\s*--/))
    |> Enum.join("\n")
    |> split_statements()
    |> Enum.reduce(0, fn stmt, n ->
      case query(req, stmt, []) do
        {:ok, _} -> n + 1
        {:error, msg} -> die("statement #{n + 1} failed (#{String.slice(stmt, 0, 60)}...): #{msg}")
      end
    end)
    |> then(&IO.puts("applied #{&1} statement(s)"))
  end

  # Splits on ';' but stays inside CREATE TRIGGER bodies (opened by BEGIN, and
  # by CASE...END within them), so a trigger's internal ';'s don't split early.
  defp split_statements(text) do
    ~r/;|\bBEGIN\b|\bCASE\b|\bEND\b/i
    |> Regex.split(text, include_captures: true)
    |> Enum.reduce({[], "", 0}, fn tok, {stmts, cur, depth} ->
      cond do
        String.upcase(tok) in ["BEGIN", "CASE"] -> {stmts, cur <> tok, depth + 1}
        String.upcase(tok) == "END" -> {stmts, cur <> tok, max(depth - 1, 0)}
        tok == ";" and depth == 0 -> {[cur <> ";" | stmts], "", depth}
        true -> {stmts, cur <> tok, depth}
      end
    end)
    |> then(fn {stmts, cur, _depth} -> if String.trim(cur) == "", do: stmts, else: [cur | stmts] end)
    |> Enum.reverse()
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp print_table(%{"columns" => [], "num_changes" => n}), do: IO.puts("ok (num_changes: #{n})")

  defp print_table(%{"columns" => cols, "rows" => rows}) do
    widths =
      Enum.map(Enum.with_index(cols), fn {col, i} ->
        rows
        |> Enum.map(&String.length(cell(Enum.at(&1, i))))
        |> Enum.max(fn -> 0 end)
        |> max(String.length(col))
      end)

    pad = fn vals -> vals |> Enum.zip(widths) |> Enum.map_join("  ", fn {v, w} -> String.pad_trailing(cell(v), w) end) end

    IO.puts(pad.(cols))
    IO.puts(Enum.map_join(widths, "  ", &String.duplicate("-", &1)))
    Enum.each(rows, fn row -> IO.puts(pad.(row)) end)
    IO.puts("\n(#{length(rows)} row#{if length(rows) == 1, do: "", else: "s"})")
  end

  defp cell(nil), do: ""
  defp cell(v) when is_binary(v), do: v
  defp cell(v), do: inspect(v)

  defp prefix(db), do: "SMOLSQLS_" <> String.replace(String.upcase(db), ~r/[^A-Z0-9]+/, "_")

  defp default_env("pm"), do: ".claude/smolquery-pm.env"
  defp default_env("alpha"), do: ".claude/alpha-db.env"
  defp default_env(db), do: ".claude/#{db}.env"

  defp load_env(path) do
    if File.exists?(path) do
      path
      |> File.stream!()
      |> Enum.each(fn line ->
        case Regex.run(~r/^\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)$/, String.trim_trailing(line)) do
          [_, k, v] -> System.put_env(k, String.trim(v, "\""))
          _ -> :ok
        end
      end)
    end

    :ok
  end

  defp die(msg) do
    IO.puts(:stderr, "error: #{msg}")
    System.halt(1)
  end
end

SmolsqlsQuery.main(System.argv())
