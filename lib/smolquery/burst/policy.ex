defmodule Smolquery.Burst.Policy do
  @moduledoc """
  When a scattered job bursts, and how many runners it takes (PL-77 D7).

  A job asks with `burst: false | :auto | n`. `false` is peers only. `:auto`
  bursts when the sealed scan is at least `min_scan_bytes`, with one runner
  per `target_bytes` of it, so a 64 GB scan over a 1 GiB target asks for 64
  runners and a dashboard query over a few hundred MB asks for none. An
  integer asks for exactly that many. Every answer is capped at
  `max_per_job` and at the pool's `max`: a runner past the pool's max would
  only queue for a checkout behind the first wave.

  The policy is pure. It answers from the request, the scan and the
  configuration, and when it refuses it says why, so the job can carry the
  reason as `burstRefused`. `:off` is not a refusal worth reporting: the job
  did not ask. Nor is `:auto` on a node without a pool, since `:auto` is
  every job's default; an integer on such a node is `:no_pool`, because the
  caller asked for something the deployment cannot give.
  """

  @type request :: false | :auto | non_neg_integer()

  @type config :: %{
          required(:enabled) => boolean(),
          required(:max) => pos_integer(),
          required(:max_per_job) => pos_integer(),
          required(:min_scan_bytes) => non_neg_integer(),
          required(:target_bytes) => pos_integer(),
          optional(atom()) => term()
        }

  @type refusal :: :off | :no_pool | {:under_threshold, non_neg_integer(), non_neg_integer()}

  @doc """
  How many runners a job that asked `request` over `scan_bytes` of sealed
  data gets under `config`, or why none.
  """
  @spec runners(request(), non_neg_integer(), config()) ::
          {:ok, pos_integer()} | {:none, refusal()}
  def runners(false, _scan_bytes, _config), do: {:none, :off}
  def runners(0, _scan_bytes, _config), do: {:none, :off}
  def runners(:auto, _scan_bytes, %{enabled: false}), do: {:none, :off}
  def runners(_count, _scan_bytes, %{enabled: false}), do: {:none, :no_pool}

  def runners(:auto, scan_bytes, %{min_scan_bytes: min}) when scan_bytes < min,
    do: {:none, {:under_threshold, scan_bytes, min}}

  def runners(:auto, scan_bytes, %{target_bytes: target} = config),
    do: {:ok, cap(ceil_div(scan_bytes, target), config)}

  def runners(count, _scan_bytes, config) when is_integer(count) and count > 0,
    do: {:ok, cap(count, config)}

  @doc """
  Whether `value` is a request a job may make.
  """
  @spec request?(term()) :: boolean()
  def request?(false), do: true
  def request?(:auto), do: true
  def request?(count) when is_integer(count) and count >= 0, do: true
  def request?(_other), do: false

  defp cap(count, %{max_per_job: per_job, max: max}),
    do: count |> min(per_job) |> min(max) |> max(1)

  defp ceil_div(bytes, target), do: div(bytes + target - 1, target)
end
