defmodule Smolquery.Test.Eventually do
  @moduledoc """
  Waits for a condition a supervisor is in the middle of bringing about.

  Restarts are asynchronous, so a test that kills a process and immediately
  asserts on the replacement is asserting on a race. Polling a predicate is the
  honest way to say "once the tree has settled".
  """

  @doc """
  Polls `predicate` until it holds, or the attempts run out.
  """
  @spec until((-> boolean()), pos_integer(), pos_integer()) :: boolean()
  def until(predicate, attempts \\ 100, interval \\ 10) do
    cond do
      predicate.() ->
        true

      attempts <= 1 ->
        false

      true ->
        Process.sleep(interval)
        until(predicate, attempts - 1, interval)
    end
  end
end
