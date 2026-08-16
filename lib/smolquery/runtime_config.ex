defmodule Smolquery.RuntimeConfig do
  @moduledoc """
  Strict parsers for scalar values supplied by release environment variables.

  Every parser raises `ArgumentError` with the variable name and the accepted
  value shape when a release value cannot be used safely.
  """

  @doc """
  Parses a positive base-10 integer.
  """
  @spec positive_integer!(String.t(), String.t()) :: pos_integer()
  def positive_integer!(name, value), do: integer_at_least!(name, value, 1)

  @doc """
  Parses a base-10 integer greater than or equal to `minimum`.
  """
  @spec integer_at_least!(String.t(), String.t(), integer()) :: integer()
  def integer_at_least!(name, value, minimum) do
    parse_integer!(name, value, minimum, :infinity)
  end

  @doc """
  Parses a non-negative base-10 integer.
  """
  @spec non_negative_integer!(String.t(), String.t()) :: non_neg_integer()
  def non_negative_integer!(name, value) do
    parse_integer!(name, value, 0, :infinity)
  end

  @doc """
  Parses an integer in the inclusive range `minimum..maximum`.
  """
  @spec integer_in_range!(String.t(), String.t(), integer(), integer()) :: integer()
  def integer_in_range!(name, value, minimum, maximum) do
    parse_integer!(name, value, minimum, maximum)
  end

  @doc """
  Parses an integer port in the range 1 through 65,535.
  """
  @spec port!(String.t(), String.t()) :: 1..65_535
  def port!(name, value), do: integer_in_range!(name, value, 1, 65_535)

  @doc """
  Parses an IPv4 or IPv6 address.
  """
  @spec ip!(String.t(), String.t()) :: :inet.ip_address()
  def ip!(name, value) do
    case value |> String.to_charlist() |> :inet.parse_address() do
      {:ok, ip} -> ip
      {:error, _reason} -> invalid!(name, value, "an IPv4 or IPv6 address")
    end
  end

  @doc """
  Parses a finite positive integer or the literal `infinity`.
  """
  @spec positive_integer_or_infinity!(String.t(), String.t()) :: pos_integer() | :infinity
  def positive_integer_or_infinity!(_name, "infinity"), do: :infinity
  def positive_integer_or_infinity!(name, value), do: positive_integer!(name, value)

  @doc "Parses a non-empty comma-separated list of trimmed strings."
  @spec csv!(String.t(), String.t()) :: [String.t()]
  def csv!(name, value) do
    values = value |> String.split(",") |> Enum.map(&String.trim/1)

    if values != [] and Enum.all?(values, &(&1 != "")),
      do: values,
      else: invalid!(name, value, "a non-empty comma-separated list")
  end

  @doc "Parses a bounded non-negative integer."
  @spec bounded_non_negative_integer!(String.t(), String.t(), pos_integer()) :: non_neg_integer()
  def bounded_non_negative_integer!(name, value, maximum) do
    integer = non_negative_integer!(name, value)
    if integer <= maximum, do: integer, else: invalid!(name, value, "an integer in 0..#{maximum}")
  end

  @doc "Parses a JSON claim-value-to-capabilities mapping without atomizing input."
  @spec capability_mapping!(String.t(), String.t()) :: map()
  def capability_mapping!(name, value) do
    case JSON.decode(value) do
      {:ok, mapping} when is_map(mapping) ->
        Enum.reduce(mapping, %{}, &parse_claim_mapping(name, value, &1, &2))

      _ ->
        invalid!(name, value, "a JSON object of claim names to value maps")
    end
  end

  defp parse_claim_mapping(name, original, {claim, values}, acc) do
    if is_binary(claim) and claim != "" and is_map(values) do
      Map.put(acc, claim, parse_claim_values(name, original, values))
    else
      invalid!(name, original, "a JSON object of claim names to value maps")
    end
  end

  defp parse_claim_values(name, original, values) do
    parsed = Enum.reduce(values, %{}, &parse_claim_value(name, original, &1, &2))

    if map_size(parsed) == 0,
      do: invalid!(name, original, "non-empty claim value maps"),
      else: parsed
  end

  defp parse_claim_value(name, original, {claim_value, capabilities}, acc) do
    if is_binary(claim_value) and claim_value != "" and is_list(capabilities) and
         capabilities != [] do
      Map.put(acc, claim_value, Enum.map(capabilities, &capability!(name, original, &1)))
    else
      invalid!(name, original, "a JSON object of non-empty claim values and capability arrays")
    end
  end

  @doc """
  Parses a release boolean as `true`, `1`, `false`, or `0`.
  """
  @spec boolean!(String.t(), String.t()) :: boolean()
  def boolean!(_name, value) when value in ["true", "1"], do: true
  def boolean!(_name, value) when value in ["false", "0"], do: false
  def boolean!(name, value), do: invalid!(name, value, "true, 1, false, or 0")

  @doc """
  Maps a string through a finite set of accepted values without creating atoms.
  """
  @spec enum!(String.t(), String.t(), [{String.t(), term()}]) :: term()
  def enum!(name, value, choices) do
    case List.keyfind(choices, value, 0) do
      {^value, parsed} -> parsed
      nil -> invalid!(name, value, "one of #{Enum.map_join(choices, ", ", &elem(&1, 0))}")
    end
  end

  @doc """
  Parses a bounded comma-separated list of Erlang node names.
  """
  @spec node_names!(String.t(), String.t()) :: [String.t()]
  def node_names!(name, value) do
    nodes = value |> String.split(",", trim: true) |> Enum.map(&String.trim/1)

    if Enum.count_until(nodes, 257) == 257 do
      invalid!(name, value, "at most 256 comma-separated Erlang node names")
    end

    Enum.map(nodes, &validate_node_name!(name, value, &1))
  end

  defp validate_node_name!(name, original, node_name) do
    case String.split(node_name, "@", parts: 3) do
      [local, host]
      when local != "" and host != "" and byte_size(node_name) <= 255 ->
        node_name

      _invalid ->
        invalid!(
          name,
          original,
          "comma-separated name@host values of at most 255 bytes each"
        )
    end
  end

  defp parse_integer!(name, value, minimum, :infinity) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= minimum -> integer
      _ -> invalid!(name, value, "an integer in at least #{minimum}")
    end
  end

  defp parse_integer!(name, value, minimum, maximum) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= minimum and integer <= maximum -> integer
      _ -> invalid!(name, value, "an integer in #{minimum}..#{maximum}")
    end
  end

  defp parse_integer!(name, value, _minimum, _maximum),
    do: invalid!(name, value, "a decimal integer")

  defp capability!(name, value, capability) do
    case capability do
      "web_access" -> :web_access
      "query" -> :query
      "ingest" -> :ingest
      "catalog_manage" -> :catalog_manage
      "platform_operate" -> :platform_operate
      _ -> invalid!(name, value, "known Smolquery capabilities")
    end
  end

  defp invalid!(name, value, expected) do
    raise ArgumentError,
          "#{name} has invalid value #{inspect(value)}; expected #{expected}"
  end
end
