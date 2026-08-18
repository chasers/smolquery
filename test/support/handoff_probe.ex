defmodule Smolquery.Test.HandoffProbe do
  @moduledoc """
  A `Smolquery.StorageService.Handoff` a test drives by hand.

  Reports each attempt to a test process and then blocks until that process
  releases it, which is what makes the sealer's scheduling observable: coalescing
  and the concurrency bound only mean anything while an attempt is still running.

      handoff = {HandoffProbe, {self(), :ok}}
      assert_receive {:sealing, table_ref, claim, attempt}
      HandoffProbe.release(attempt)

  The configured term is `{test_pid, result}` — the result every released attempt
  returns, or `:crash` to make it raise. `release/2` overrides it for one attempt,
  for a test that needs a table to fail and then succeed.
  """

  @behaviour Smolquery.StorageService.Handoff

  alias Smolquery.StorageService.Handoff

  @impl Handoff
  def seal({test, result}, runtime, table_ref, claim) do
    send(test, {:sealing, table_ref, claim, self()})
    send(test, {:sealing_engine, table_ref, runtime.merge_engine})

    receive do
      :release -> finish(result)
      {:release, override} -> finish(override)
    end
  end

  @doc """
  Lets a reported attempt finish, returning the configured result.
  """
  @spec release(pid()) :: :ok
  def release(attempt) do
    send(attempt, :release)

    :ok
  end

  @doc """
  Lets a reported attempt finish with `result`, ignoring the configured one.
  """
  @spec release(pid(), term()) :: :ok
  def release(attempt, result) do
    send(attempt, {:release, result})

    :ok
  end

  defp finish(:crash), do: raise("handoff probe asked to crash")
  defp finish(result), do: result
end
