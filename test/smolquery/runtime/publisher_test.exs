defmodule Smolquery.Runtime.PublisherTest do
  use ExUnit.Case, async: true

  alias Smolquery.Runtime.Publisher

  defmodule TestRuntime do
    def put(runtime), do: :persistent_term.put({__MODULE__, runtime.name}, runtime)

    def fetch(name) do
      case :persistent_term.get({__MODULE__, name}, nil) do
        nil -> :error
        runtime -> {:ok, runtime}
      end
    end

    def delete(name), do: :persistent_term.erase({__MODULE__, name})
  end

  test "publishes for the child lifetime and withdraws on shutdown" do
    name = :runtime_publisher_test
    runtime = %{name: name}

    assert {:ok, pid} = Publisher.start_link({TestRuntime, runtime})
    assert TestRuntime.fetch(name) == {:ok, runtime}

    GenServer.stop(pid)
    assert TestRuntime.fetch(name) == :error
  end
end
