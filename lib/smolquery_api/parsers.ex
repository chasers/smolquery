defmodule SmolqueryApi.Parsers do
  @moduledoc """
  `Plug.Parsers` behind the envelope.

  Same parser set the Plug.Router had — JSON only, with the three load body
  types passed through raw for `SmolqueryApi.LoadController` to stream — but
  a body that fails to parse answers 400/415 in `SmolqueryApi.Errors`'
  envelope instead of raising into Phoenix's error rendering. Sits after
  `SmolqueryApi.Auth` in the pipeline, so unauthenticated bodies are never
  read at all.

  ## How big a body may be

  `:max_body_bytes` is the cap, and it is deliberately a number this project
  chose: leaving it to `Plug.Parsers` meant inheriting a default of 8 000 000
  bytes that nothing here had picked, and which nothing here documented.

  It is not the write path's only size bound and it does not coordinate with the
  others, which is worth stating because the numbers invite the assumption that
  they do:

    * this one counts **encoded bytes on the wire**
    * `Smolquery.BufferService`'s `:flush_max_bytes` counts the accumulated
      **Elixir term**, which for JSON-shaped rows runs above the wire size
    * `:max_buffered_bytes` counts the same term and refuses past it, so a body
      admitted here can still be answered 429 one layer down

  Raising this alone therefore moves the refusal rather than removing it. The
  bound that actually protects the node is `:max_buffered_bytes`, because it
  measures the heap; this one bounds what a single request may ask that layer
  to consider.
  """

  @behaviour Plug

  alias SmolqueryApi.Errors

  @default_max_body_bytes 8_000_000

  @impl Plug
  def init(_opts), do: []

  @doc """
  The largest request body this endpoint reads, in bytes.
  """
  @spec max_body_bytes() :: pos_integer()
  def max_body_bytes do
    :smolquery
    |> Application.get_env(SmolqueryApi, [])
    |> Keyword.get(:max_body_bytes, @default_max_body_bytes)
  end

  # `Plug.Parsers.init/1` bakes `:length` into the parsers it returns, and under
  # the `:compile` plug init mode a release would bake it when the router is
  # compiled — so a limit set in `config/runtime.exs`, which is where every other
  # operational setting here lives, would be silently ignored. Resolving it per
  # call fixes that; caching keyed on the value itself keeps the per-request cost
  # to one `Application.get_env/3` and one `:persistent_term.get/2`, and re-inits
  # only if an operator actually changes the limit.
  defp parsers do
    limit = max_body_bytes()

    case :persistent_term.get({__MODULE__, :parsers}, nil) do
      {^limit, parsers} ->
        parsers

      _stale ->
        parsers =
          Plug.Parsers.init(
            parsers: [:json],
            json_decoder: JSON,
            length: limit,
            pass: ["application/x-ndjson", "text/csv", "application/vnd.apache.parquet"]
          )

        :persistent_term.put({__MODULE__, :parsers}, {limit, parsers})

        parsers
    end
  end

  @impl Plug
  def call(conn, _opts) do
    Plug.Parsers.call(conn, parsers())
  rescue
    Plug.Parsers.ParseError ->
      conn
      |> Errors.send_error(400, "INVALID_ARGUMENT", "request body is not valid JSON")
      |> Plug.Conn.halt()

    Plug.Parsers.UnsupportedMediaTypeError ->
      conn
      |> Errors.send_error(415, "UNSUPPORTED_MEDIA_TYPE", "request body must be application/json")
      |> Plug.Conn.halt()

    # Unrescued, this one answers 413 in the status line and the generic 500
    # envelope in the body — the status a client reads programmatically says the
    # server broke. The limit goes in the message because the only useful thing
    # a client can do about it is split the batch to fit.
    Plug.Parsers.RequestTooLargeError ->
      conn
      |> Errors.send_error(
        413,
        "REQUEST_TOO_LARGE",
        "request body exceeds #{max_body_bytes()} bytes; split the batch"
      )
      |> Plug.Conn.halt()
  end
end
