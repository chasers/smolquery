defmodule Smolquery.Runtime do
  @moduledoc """
  Publishes a service's resolved configuration where any caller can read it free.

  Each service turns application config into handles once at boot — a store, a
  manifest, an ownership ring — and every service then faces the same problem:
  callers on the hot path must not rebuild those handles, and callers on another
  node must be able to ask whether the service is running here at all.
  `:persistent_term` answers both, at the cost of a read with no copying.

  This holds only the mechanism. What a runtime *is* belongs to each service
  (`Smolquery.BufferService.Runtime`, `Smolquery.StorageService.Runtime`), which
  wraps these three functions in its own typed API — so callers name a service,
  not a term store.

  `scope` namespaces the key, so two services publishing under the same instance
  name never collide.

  ## Usage

  `use Smolquery.Runtime` gives a service's runtime struct the typed trio — it
  needs a `name` field and a `t/0` type, and gets `put/1`, `fetch/1`, and
  `delete/1` scoped to itself:

      defmodule Smolquery.MyService.Runtime do
        defstruct [:name, ...]
        @type t :: %__MODULE__{...}

        use Smolquery.Runtime
      end

  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      @doc """
      Publishes this runtime where callers can read it.
      """
      @spec put(t()) :: :ok
      def put(%__MODULE__{} = runtime),
        do: Smolquery.Runtime.put(__MODULE__, runtime.name, runtime)

      @doc """
      The published runtime for an instance.

      `:error` means this service is not running on this node — a caller is asking
      a node that does not hold its role.
      """
      @spec fetch(atom()) :: {:ok, t()} | :error
      def fetch(name), do: Smolquery.Runtime.fetch(__MODULE__, name)

      @doc """
      Withdraws a published runtime.
      """
      @spec delete(atom()) :: boolean()
      def delete(name), do: Smolquery.Runtime.delete(__MODULE__, name)
    end
  end

  @doc """
  The value of a required configuration key.

  A service boot-gates a secret through this in its `new/1`. A missing or
  empty value raises an error that names the environment variable, the
  application-config path, and the role that requires the value. `context`
  takes `:service`, `:missing`, `:env_var`, `:scope`, and `:role`.
  """
  @spec fetch_required!(keyword(), atom(), keyword()) :: String.t()
  def fetch_required!(config, key, context) do
    case Keyword.get(config, key) do
      value when is_binary(value) and value != "" ->
        value

      _absent ->
        raise ArgumentError,
              "#{Keyword.fetch!(context, :service)} refuses to boot without " <>
                "#{Keyword.fetch!(context, :missing)}: set #{Keyword.fetch!(context, :env_var)} " <>
                "(or config :smolquery, #{inspect(Keyword.fetch!(context, :scope))}, #{key}: ...) " <>
                "on every node running the #{inspect(Keyword.fetch!(context, :role))} role"
    end
  end

  @doc """
  Publishes `runtime` for `scope` and `name`.
  """
  @spec put(module(), atom(), term()) :: :ok
  def put(scope, name, runtime), do: :persistent_term.put({scope, name}, runtime)

  @doc """
  The published runtime, or `:error` if the service is not running here.
  """
  @spec fetch(module(), atom()) :: {:ok, term()} | :error
  def fetch(scope, name) do
    {:ok, :persistent_term.get({scope, name})}
  rescue
    ArgumentError -> :error
  end

  @doc """
  Withdraws a published runtime.
  """
  @spec delete(module(), atom()) :: boolean()
  def delete(scope, name), do: :persistent_term.erase({scope, name})
end
