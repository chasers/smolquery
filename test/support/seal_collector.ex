defmodule Smolquery.Test.SealCollector do
  @moduledoc """
  A `Smolquery.BufferService.SealConsumer` that forwards signals to a test process.

  Seal signalling is level-triggered, so a test needs to see repeats as well as the
  first signal. Sending each one as a message makes both observable, and makes the
  absence of a signal assertable with `refute_receive`.
  """

  @behaviour Smolquery.BufferService.SealConsumer

  alias Smolquery.BufferService.SealConsumer

  @impl SealConsumer
  def seal_ready(test, table_ref, ids) do
    send(test, {:seal_ready, table_ref, ids})

    :ok
  end
end
