defmodule SmolqueryVictoriaMetrics.VmagentIntegrationTest do
  @moduledoc """
  A real vmagent v1.152.0 scraping itself and remote-writing to the edge
  over a real listener, then read back through the Prometheus API as
  Grafana reads it (PL-70, T-567).

  The binary is downloaded from the VictoriaMetrics release for this
  machine's architecture, checked against the release's checksums, and
  kept in `~/.cache/smolquery/vmagent/v1.152.0/`. A download or checksum
  failure fails the test: it is not skipped.
  """

  use ExUnit.Case, async: false

  alias Smolquery.Test.Eventually
  alias Smolquery.Test.VictoriaMetricsStack
  alias SmolqueryVictoriaMetrics.Runtime

  @moduletag :integration
  @moduletag :tmp_dir
  @moduletag timeout: 300_000

  @version "v1.152.0"
  @release "https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/#{@version}"
  @binary "vmagent-prod"

  setup_all do
    %{vmagent: vmagent_binary!()}
  end

  setup context do
    stack = VictoriaMetricsStack.start(context)
    name = :"vm_vmagent_edge_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {SmolqueryVictoriaMetrics.Supervisor,
       name: name,
       password: VictoriaMetricsStack.password(),
       port: 0,
       ingest_name: stack.runtime.ingest_name,
       query_name: stack.runtime.query_name,
       catalog: stack.catalog},
      id: name
    )

    on_exit(fn -> Runtime.delete(name) end)

    {:ok, {_ip, port}} = SmolqueryVictoriaMetrics.Supervisor.bound(name)

    %{edge: "http://127.0.0.1:#{port}"}
  end

  test "vmagent's own protocol lands and reads back as Grafana asks", context do
    agent = start_vmagent!(context, [])

    wait_until_up!(context.edge, agent)

    assert %{"version" => "2.24.0"} = data!(context.edge, "/api/v1/status/buildinfo", %{})

    assert %{"resultType" => "scalar", "result" => [_time, "2"]} =
             data!(context.edge, "/api/v1/query", %{"query" => "1+1"})

    assert [%{"metric" => version}] = vector(context.edge, "vm_app_version")
    assert version["short_version"] == @version
    assert version["job"] == "vmagent"

    labels = data!(context.edge, "/api/v1/labels", %{"start" => minute_ago()})
    assert "job" in labels
    assert "instance" in labels
    assert "__name__" in labels

    names = data!(context.edge, "/api/v1/label/__name__/values", %{"start" => minute_ago()})
    assert Enum.count_until(names, 101) > 100
    assert "vm_promscrape_scrapes_total" in names

    assert Eventually.until(fn -> scrapes(context.edge) >= 3 end, 30, 1_000),
           "fewer than three scrapes of vmagent landed within 30 seconds"

    assert [%{"value" => [_time, rate]}] =
             vector(context.edge, ~s|rate(vm_promscrape_scrapes_total{job="vmagent"}[10s])|)

    assert {rate, ""} = Float.parse(rate)
    assert rate > 0

    now = System.system_time(:second)

    matrix =
      data!(context.edge, "/api/v1/query_range", %{
        "query" => ~s|up{job="vmagent"}|,
        "start" => now - 60,
        "end" => now,
        "step" => "1s"
      })

    assert %{"resultType" => "matrix", "result" => [%{"values" => [_ | _] = values}]} = matrix
    assert Enum.all?(values, fn [_time, value] -> value == "1" end)

    assert_only_successes(agent)
  end

  test "Prometheus remote write 1.0 from vmagent is taken without a retry", context do
    agent = start_vmagent!(context, ["-remoteWrite.forcePromProto=true"])

    wait_until_up!(context.edge, agent)

    first = successes(agent)

    assert Eventually.until(fn -> successes(agent) > first end, 30, 500),
           "vmagent's 2XX count did not move past #{first} within 15 seconds"

    assert_only_successes(agent)

    assert [%{"metric" => %{"short_version" => @version}}] =
             vector(context.edge, "vm_app_version")
  end

  defp start_vmagent!(context, extra) do
    listen = free_port()
    dir = Path.join(context.tmp_dir, "vmagent_#{listen}")
    File.mkdir_p!(dir)
    config = Path.join(dir, "scrape.yml")

    File.write!(config, """
    global:
      scrape_interval: 1s
    scrape_configs:
      - job_name: vmagent
        static_configs:
          - targets: ["127.0.0.1:#{listen}"]
    """)

    args =
      [
        "-httpListenAddr=127.0.0.1:#{listen}",
        "-promscrape.config=#{config}",
        "-remoteWrite.url=#{context.edge}/api/v1/write",
        "-remoteWrite.bearerToken=#{VictoriaMetricsStack.password()}",
        "-remoteWrite.tmpDataPath=#{Path.join(dir, "queue")}",
        "-remoteWrite.flushInterval=1s",
        "-loggerLevel=WARN"
      ] ++ extra

    port =
      Port.open({:spawn_executable, context.vmagent}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: args
      ])

    {:os_pid, os_pid} = Port.info(port, :os_pid)
    on_exit(fn -> System.cmd("kill", ["-9", Integer.to_string(os_pid)]) end)

    %{port: port, metrics: "http://127.0.0.1:#{listen}/metrics"}
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp wait_until_up!(edge, agent) do
    up? = fn -> match?([%{"value" => [_time, "1"]}], vector(edge, ~s|up{job="vmagent"}|)) end

    assert Eventually.until(up?, 60, 500),
           "up{job=\"vmagent\"} did not answer 1 within 30 seconds; vmagent said:\n" <>
             output(agent.port)
  end

  defp scrapes(edge) do
    case vector(edge, ~s|count_over_time(up{job="vmagent"}[1m])|) do
      [%{"value" => [_time, count]}] -> String.to_integer(count)
      _none -> 0
    end
  end

  defp vector(edge, query) do
    %{"resultType" => "vector", "result" => result} =
      data!(edge, "/api/v1/query", %{"query" => query})

    result
  end

  defp data!(edge, path, params) do
    response =
      Req.get!(edge <> path,
        params: params,
        auth: {:bearer, VictoriaMetricsStack.password()},
        retry: false
      )

    assert %{status: 200, body: %{"status" => "success", "data" => data}} = response
    data
  end

  defp minute_ago, do: System.system_time(:second) - 60

  defp successes(agent) do
    agent
    |> counters("vmagent_remotewrite_requests_total")
    |> Enum.sum_by(fn {labels, count} ->
      if labels =~ ~s(status_code="2XX"), do: count, else: 0
    end)
  end

  defp assert_only_successes(agent) do
    requests = counters(agent, "vmagent_remotewrite_requests_total")
    retries = counters(agent, "vmagent_remotewrite_retries_count_total")

    assert successes(agent) > 0, "vmagent counted no 2XX from the edge: #{inspect(requests)}"

    assert Enum.reject(requests, fn {labels, count} -> labels =~ "2XX" or count == 0 end) == [],
           "vmagent counted refusals from the edge: #{inspect(requests)}"

    assert Enum.all?(retries, fn {_labels, count} -> count == 0 end),
           "vmagent retried writes to the edge: #{inspect(retries)}"
  end

  defp counters(agent, name) do
    %{status: 200, body: body} = Req.get!(agent.metrics, retry: false, decode_body: false)

    for line <- String.split(body, "\n"),
        [_line, labels, count] <- [Regex.run(~r/^#{name}(\{.*\}) (\d+)$/, line)] do
      {labels, String.to_integer(count)}
    end
  end

  defp output(port) do
    receive do
      {^port, {:data, data}} -> data <> output(port)
    after
      0 -> ""
    end
  end

  defp vmagent_binary! do
    dir = Path.join([System.user_home!(), ".cache", "smolquery", "vmagent", @version])
    File.mkdir_p!(dir)
    archive = "vmutils-linux-#{arch()}-#{@version}.tar.gz"
    sums = checksums!(dir, archive)
    binary = Path.join(dir, @binary)

    unless File.exists?(binary) and sha256(File.read!(binary)) == sums[@binary] do
      install!(archive, sums, binary)
    end

    binary
  end

  defp arch do
    case :erlang.system_info(:system_architecture) |> List.to_string() |> String.split("-") do
      ["x86_64" | _rest] -> "amd64"
      ["aarch64" | _rest] -> "arm64"
      other -> flunk("no vmagent #{@version} build for #{Enum.join(other, "-")}")
    end
  end

  defp checksums!(dir, archive) do
    file = String.replace_suffix(archive, ".tar.gz", "_checksums.txt")
    path = Path.join(dir, file)

    text =
      case File.read(path) do
        {:ok, text} ->
          text

        {:error, _enoent} ->
          text = download!(file)
          File.write!(path, text)
          text
      end

    sums =
      for line <- String.split(text, "\n", trim: true),
          [sum, name] = String.split(line, ~r/\s+/, parts: 2),
          into: %{},
          do: {name, sum}

    assert Map.has_key?(sums, archive) and Map.has_key?(sums, @binary),
           "#{file} does not list #{archive} and #{@binary}: #{inspect(sums)}"

    sums
  end

  defp install!(archive, sums, binary) do
    tarball = download!(archive)

    assert sha256(tarball) == sums[archive],
           "#{archive} does not match the release's checksum #{sums[archive]}"

    assert {:ok, [{_name, contents}]} =
             :erl_tar.extract({:binary, tarball}, [
               :compressed,
               :memory,
               files: [String.to_charlist(@binary)]
             ])

    assert sha256(contents) == sums[@binary],
           "#{@binary} in #{archive} does not match the release's checksum"

    partial = binary <> ".partial"
    File.write!(partial, contents)
    File.chmod!(partial, 0o755)
    File.rename!(partial, binary)
  end

  defp download!(file) do
    url = "#{@release}/#{file}"

    case Req.get(url, decode_body: false, receive_timeout: 120_000, max_retries: 3) do
      {:ok, %{status: 200, body: body}} ->
        body

      {:ok, %{status: status}} ->
        flunk("cannot download vmagent #{@version}: #{url} answered #{status}")

      {:error, reason} ->
        flunk("cannot download vmagent #{@version} from #{url}: #{Exception.message(reason)}")
    end
  end

  defp sha256(bytes), do: :sha256 |> :crypto.hash(bytes) |> Base.encode16(case: :lower)
end
