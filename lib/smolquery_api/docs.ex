defmodule SmolqueryApi.Docs do
  @moduledoc """
  The HTTP API described as data, served at `GET /v1/docs.json`.

  One structured map holds what `docs/api.md` tells a human: the routes,
  the auth rule, the media types, the error envelope, and the schema types.
  Agents read this endpoint instead of the markdown. The map lives here as
  data so the JSON needs no parser and no build step; when a route changes,
  this module changes in the same commit, exactly like `docs/api.md`.

  The endpoint is deliberately unauthenticated, like `/healthz`: it holds no
  tenant data, and an agent needs the docs before it can hold a key. This is
  a stated exception to the router's "404 never reveals which routes are
  real" posture — the operator publishes the surface on purpose.
  """

  @repository "https://github.com/chasers/smolquery"

  @spec spec() :: map()
  def spec do
    %{
      "name" => "smolquery HTTP API",
      "version" => version(),
      "api_version" => "v1",
      "repository" => @repository,
      "documentation" => "#{@repository}/blob/main/docs/api.md",
      "auth" => %{
        "scheme" => "bearer",
        "header" => "authorization: Bearer <SMOLQUERY_API_KEY>",
        "note" =>
          "Every /v1 route requires the static bearer key. " <>
            "/healthz and /v1/docs.json are open. /metrics takes the internal secret."
      },
      "errors" => %{
        "envelope" => %{"error" => %{"code" => "string", "message" => "string"}},
        "note" => "Every failure answers this envelope, body-parse failures included."
      },
      "schema_types" => [
        "INT64",
        "FLOAT64",
        "STRING",
        "BOOL",
        "TIMESTAMP",
        "DATE",
        "NUMERIC(p,s)"
      ],
      "routes" => routes()
    }
  end

  defp version do
    case Application.spec(:smolquery, :vsn) do
      nil -> "unknown"
      vsn -> List.to_string(vsn)
    end
  end

  defp routes do
    [
      %{
        "method" => "GET",
        "path" => "/healthz",
        "auth" => "none",
        "summary" => "Liveness probe."
      },
      %{
        "method" => "GET",
        "path" => "/v1/docs.json",
        "auth" => "none",
        "summary" => "This document."
      },
      %{
        "method" => "GET",
        "path" => "/metrics",
        "auth" => "internal secret (x-smolquery-internal header)",
        "summary" => "Prometheus text metrics, for operators."
      },
      %{
        "method" => "GET",
        "path" => "/v1/datasets",
        "auth" => "bearer",
        "summary" => "List datasets."
      },
      %{
        "method" => "POST",
        "path" => "/v1/datasets",
        "auth" => "bearer",
        "summary" => "Create a dataset. Idempotent.",
        "request" => %{"id" => "string"}
      },
      %{
        "method" => "GET",
        "path" => "/v1/datasets/:dataset/tables",
        "auth" => "bearer",
        "summary" => "List a dataset's tables."
      },
      %{
        "method" => "POST",
        "path" => "/v1/datasets/:dataset/tables",
        "auth" => "bearer",
        "summary" =>
          "Create a table. Re-creating with the same schema answers 200. " <>
            "A different schema answers 409, never a silent no-op.",
        "request" => %{
          "id" => "string",
          "schema" => [%{"name" => "string", "type" => "schema type", "nullable" => "boolean"}]
        }
      },
      %{
        "method" => "GET",
        "path" => "/v1/datasets/:dataset/tables/:table",
        "auth" => "bearer",
        "summary" =>
          "A table's schema, retention policy, clustering key, and partition " <>
            "count. A null partition count means the deployment default applies."
      },
      %{
        "method" => "PATCH",
        "path" => "/v1/datasets/:dataset/tables/:table",
        "auth" => "bearer",
        "summary" =>
          "Update retention, clustering, and/or partitions as one atomic " <>
            "change. An error response means none of them changed.",
        "request" => %{
          "retention" => %{"column" => "string", "ttlMs" => "positive integer, or null to clear"},
          "clustering" => ["column names; [] clears"],
          "partitions" => "positive integer, at most 64; raise-only — a lower value answers 422"
        }
      },
      %{
        "method" => "POST",
        "path" => "/v1/datasets/:dataset/tables/:table/insert",
        "auth" => "bearer",
        "summary" =>
          "Streaming insert. Body is application/x-ndjson only, one JSON " <>
            "object per line. 200 means every accepted row is durable and " <>
            "queryable; rejected rows come back per index in insertErrors. " <>
            "429 with retry-after means the write path is behind.",
        "query_params" => %{
          "insertId" =>
            "optional idempotency key; a retry with the same id and rows " <>
              "cannot double-count, except across a partition-count raise"
        }
      },
      %{
        "method" => "POST",
        "path" => "/v1/datasets/:dataset/tables/:table/load",
        "auth" => "bearer",
        "summary" =>
          "Batch load. Body is the file: application/x-ndjson, text/csv, or " <>
            "application/vnd.apache.parquet. Synchronous, not atomic, no " <>
            "insertId; a retry re-inserts. 413 past the load size cap."
      },
      %{
        "method" => "POST",
        "path" => "/v1/queries",
        "auth" => "bearer",
        "summary" =>
          "Synchronous query: the finished job plus its first page of rows. " <>
            "504 when the query outlives timeoutMs.",
        "request" => %{
          "query" => "SQL, single SELECT",
          "maxResults" => "page size, default 1000",
          "timeoutMs" => "cancel-and-504 deadline",
          "explain" => "\"plan\" or \"analyze\" answers the plan text instead of rows",
          "trace" => "boolean; returns phase spans on the job"
        }
      },
      %{
        "method" => "POST",
        "path" => "/v1/jobs",
        "auth" => "bearer",
        "summary" => "The same query as an async job. Returns the job pending."
      },
      %{
        "method" => "GET",
        "path" => "/v1/jobs/:id",
        "auth" => "bearer",
        "summary" => "Job status and stats. Answers from durable history after the result TTL."
      },
      %{
        "method" => "GET",
        "path" => "/v1/jobs/:id/results",
        "auth" => "bearer",
        "summary" =>
          "Page a finished job's rows with max_results and page_token. " <>
            "410 once results expire; 404 for an unknown job."
      },
      %{
        "method" => "DELETE",
        "path" => "/v1/jobs/:id",
        "auth" => "bearer",
        "summary" => "Cancel. Cancelling a finished job still answers 200."
      }
    ]
  end
end
