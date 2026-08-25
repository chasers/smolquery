defmodule Smolquery.StorageService.Handoff do
  @moduledoc """
  What one seal attempt actually does, separated from deciding when to run it.

  `Smolquery.StorageService.Sealer` owns scheduling — coalescing per table,
  bounding the pool, surviving a crashed attempt. This is the other half: pull the
  claim's micro-segments, merge them, put the sealed segment at the key the claim
  already names, commit it to the catalog, retire the inputs. Splitting them keeps the GenServer free of the
  handoff's failure modes, and makes the handoff a plain function that can be
  called and asserted on without a process.

  It is a behaviour, and configuration, for the same reason
  `Smolquery.BufferService.SealConsumer` is: the merge implementation is a live
  question (DuckDB `COPY` against Explorer concat), and swapping it must not mean
  editing the scheduler.

      config :smolquery, Smolquery.StorageService,
        handoff: {Smolquery.StorageService.Handoff.Log, []}

  ## An attempt may fail, and failing is not exceptional

  Every step of the handoff is idempotent under the claim's fixed input set, so a
  failed or crashed attempt is reconciled by the next one rather than rolled back. An
  implementation should therefore return `{:error, reason}` for anything it can
  describe, and let anything it cannot crash the task. Both outcomes cost a
  re-signal interval; neither loses or duplicates a row.
  """

  alias Smolquery.BufferService.SealConsumer
  alias Smolquery.Segments.Store
  alias Smolquery.StorageService.Runtime

  @doc """
  Seals `claim` for `table_ref`, end to end.
  """
  @callback seal(config :: term(), Runtime.t(), Store.table_ref(), SealConsumer.claim()) ::
              :ok | {:error, term()}

  @doc """
  Reconciles a released claim's tombstone for `table_ref` (T-386): drops any
  segment registered under the claim's keys, then confirms back to the buffer
  so the tombstone clears.
  """
  @callback reconcile_released(
              config :: term(),
              Runtime.t(),
              Store.table_ref(),
              SealConsumer.claim()
            ) :: :ok | {:error, term()}

  @doc """
  Dispatches one attempt to a configured implementation.
  """
  @spec seal({module(), term()}, Runtime.t(), Store.table_ref(), SealConsumer.claim()) ::
          :ok | {:error, term()}
  def seal({impl, config}, %Runtime{} = runtime, table_ref, claim),
    do: impl.seal(config, runtime, table_ref, claim)

  @doc """
  Dispatches one reconciliation to a configured implementation.
  """
  @spec reconcile_released(
          {module(), term()},
          Runtime.t(),
          Store.table_ref(),
          SealConsumer.claim()
        ) ::
          :ok | {:error, term()}
  def reconcile_released({impl, config}, %Runtime{} = runtime, table_ref, claim),
    do: impl.reconcile_released(config, runtime, table_ref, claim)
end
