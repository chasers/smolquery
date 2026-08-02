defmodule SmolqueryApi.Parsers do
  @moduledoc """
  `Plug.Parsers` behind the envelope.

  Same parser set the Plug.Router had — JSON only, with the three load body
  types passed through raw for `SmolqueryApi.LoadController` to stream — but
  a body that fails to parse answers 400/415 in `SmolqueryApi.Errors`'
  envelope instead of raising into Phoenix's error rendering. Sits after
  `SmolqueryApi.Auth` in the pipeline, so unauthenticated bodies are never
  read at all.
  """

  @behaviour Plug

  alias SmolqueryApi.Errors

  @impl Plug
  def init(_opts) do
    Plug.Parsers.init(
      parsers: [:json],
      json_decoder: JSON,
      pass: ["application/x-ndjson", "text/csv", "application/vnd.apache.parquet"]
    )
  end

  @impl Plug
  def call(conn, opts) do
    Plug.Parsers.call(conn, opts)
  rescue
    Plug.Parsers.ParseError ->
      conn
      |> Errors.send_error(400, "INVALID_ARGUMENT", "request body is not valid JSON")
      |> Plug.Conn.halt()

    Plug.Parsers.UnsupportedMediaTypeError ->
      conn
      |> Errors.send_error(415, "UNSUPPORTED_MEDIA_TYPE", "request body must be application/json")
      |> Plug.Conn.halt()
  end
end
