defmodule Smolquery.QueryService.Job do
  @moduledoc """
  One query's identity and lifecycle, as callers see it.

  A job is metadata — the result frame lives with the runner and travels
  separately, so polling a job's state never copies a result. States move one
  way: `:pending` → `:running` → one of `:done`, `:error`, `:cancelled`.
  `row_count` and `duration_ms` are filled when the job finishes; `snapshot`
  is the catalog snapshot the plan pinned, recorded so a caller can correlate
  what a query saw with what the catalog held.
  """

  alias Smolquery.Catalog
  alias Smolquery.QueryService.Statistics
  alias Smolquery.Segments.Id

  @enforce_keys [:id, :sql, :state, :submitted_at]
  defstruct [
    :id,
    :sql,
    :state,
    :submitted_at,
    :finished_at,
    :snapshot,
    :row_count,
    :duration_ms,
    :statistics,
    :explain,
    :error
  ]

  @type state :: :pending | :running | :done | :error | :cancelled

  @type t :: %__MODULE__{
          id: String.t(),
          sql: String.t(),
          state: state(),
          submitted_at: integer(),
          finished_at: integer() | nil,
          snapshot: Catalog.snapshot() | nil,
          row_count: non_neg_integer() | nil,
          duration_ms: non_neg_integer() | nil,
          statistics: Statistics.t() | nil,
          explain: String.t() | nil,
          error: term()
        }

  @doc """
  A pending job for `sql`, with a fresh ULID identity.
  """
  @spec new(String.t()) :: t()
  def new(sql) do
    %__MODULE__{
      id: Id.generate(),
      sql: sql,
      state: :pending,
      submitted_at: System.system_time(:millisecond)
    }
  end

  @doc """
  The job, running.
  """
  @spec running(t()) :: t()
  def running(%__MODULE__{state: :pending} = job), do: %{job | state: :running}

  @doc """
  The job, finished with a result: done, stamped, sized, and costed.

  `statistics` is what the plan read to serve it
  (`Smolquery.QueryService.Statistics`); `nil` for a job restored from
  history, which does not persist it.
  """
  @spec done(t(), Catalog.snapshot(), non_neg_integer(), non_neg_integer(), Statistics.t() | nil) ::
          t()
  def done(%__MODULE__{} = job, snapshot, row_count, duration_ms, statistics) do
    %{
      job
      | state: :done,
        snapshot: snapshot,
        row_count: row_count,
        duration_ms: duration_ms,
        statistics: statistics,
        finished_at: System.system_time(:millisecond)
    }
  end

  @doc """
  The job, finished with a query plan instead of rows.

  An explain job runs the whole pipeline — plan, views, lockdown — but asks
  the engine for its plan (`explain: :plan`) or a profiled execution
  (`explain: :analyze`) rather than a result frame, so it finishes with
  `explain` text, no `row_count`, and no result to page. Like `statistics`,
  history does not persist it.
  """
  @spec explained(t(), Catalog.snapshot(), non_neg_integer(), Statistics.t() | nil, String.t()) ::
          t()
  def explained(%__MODULE__{} = job, snapshot, duration_ms, statistics, explain) do
    %{
      job
      | state: :done,
        snapshot: snapshot,
        duration_ms: duration_ms,
        statistics: statistics,
        explain: explain,
        finished_at: System.system_time(:millisecond)
    }
  end

  @doc """
  The job, failed with `reason`.
  """
  @spec failed(t(), term()) :: t()
  def failed(%__MODULE__{} = job, reason) do
    %{job | state: :error, error: reason, finished_at: System.system_time(:millisecond)}
  end

  @doc """
  The job, cancelled — by a caller, or by its own timeout (`reason` says which).
  """
  @spec cancelled(t(), term()) :: t()
  def cancelled(%__MODULE__{} = job, reason) do
    %{job | state: :cancelled, error: reason, finished_at: System.system_time(:millisecond)}
  end

  @doc """
  Whether the job has reached a state it will never leave.
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{state: state}), do: state in [:done, :error, :cancelled]
end
