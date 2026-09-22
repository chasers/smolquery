defmodule SmolqueryVictoriaMetrics.Eval.LabelFunctionsTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.LabelFunctions
  alias SmolqueryVictoriaMetrics.Eval.Series

  @grid [0, 1000]

  defp series(labels, values \\ [1.0, 1.0]),
    do: %Series{labels: labels, values: Enum.zip(@grid, values)}

  defp text(s), do: [Series.string(@grid, s)]
  defp texts(list), do: Enum.map(list, &text/1)

  defp labels(name, args) do
    {:ok, list} = LabelFunctions.apply(name, args)
    Enum.map(list, & &1.labels)
  end

  @m %{"__name__" => "m", "job" => "api", "instance" => "host:9090"}

  test "functions/0" do
    assert "label_replace" in LabelFunctions.functions()
  end

  test "label_set, label_del and label_keep" do
    assert labels("label_set", [[series(@m)] | texts(["env", "prod", "job", ""])]) == [
             %{"__name__" => "m", "instance" => "host:9090", "env" => "prod"}
           ]

    assert labels("label_set", [[series(@m)] | texts(["__name__", "n"])])
           |> hd()
           |> Map.get("__name__") == "n"

    assert labels("label_del", [[series(@m)] | texts(["job", "__name__"])]) == [
             %{"instance" => "host:9090"}
           ]

    assert labels("label_keep", [[series(@m)] | texts(["job"])]) == [%{"job" => "api"}]
  end

  test "label_copy and label_move skip a missing source" do
    assert labels("label_copy", [[series(@m)] | texts(["job", "j", "nope", "job"])]) == [
             Map.put(@m, "j", "api")
           ]

    assert labels("label_move", [[series(@m)] | texts(["job", "j"])]) == [
             @m |> Map.delete("job") |> Map.put("j", "api")
           ]
  end

  test "label_join" do
    assert labels("label_join", [[series(@m)] | texts(["x", "-", "job", "missing", "instance"])]) ==
             [Map.put(@m, "x", "api--host:9090")]
  end

  test "label_replace anchors its expression and expands $1, ${name} and $$" do
    assert labels("label_replace", [[series(@m)] | texts(["host", "$1", "instance", "(.*):.*"])]) ==
             [Map.put(@m, "host", "host")]

    assert labels("label_replace", [
             [series(@m)] | texts(["x", "${p}-$$", "instance", "(?P<p>[a-z]+):9090"])
           ]) ==
             [Map.put(@m, "x", "host-$")]

    assert labels("label_replace", [[series(@m)] | texts(["x", "y", "instance", "host"])]) == [@m]

    assert labels("label_replace", [[series(@m)] | texts(["x", "$1x", "instance", "(host):9090"])]) ==
             [@m]
  end

  test "label_transform replaces every match" do
    assert labels("label_transform", [[series(@m)] | texts(["instance", "[0-9]", "N"])]) ==
             [Map.put(@m, "instance", "host:NNNN")]
  end

  test "label_map, label_uppercase and label_lowercase" do
    assert labels("label_map", [[series(@m)] | texts(["job", "api", "web", "x", "y"])]) == [
             Map.put(@m, "job", "web")
           ]

    assert labels("label_uppercase", [[series(@m)] | texts(["job"])]) == [
             Map.put(@m, "job", "API")
           ]

    assert labels("label_lowercase", [[series(%{"a" => "B"})] | texts(["a"])]) == [%{"a" => "b"}]
  end

  test "label_match, label_mismatch and labels_equal filter series" do
    other = %{"job" => "db", "instance" => "db"}
    input = [series(@m), series(other)]
    assert labels("label_match", [input | texts(["job", "a.*"])]) == [@m]
    assert labels("label_mismatch", [input | texts(["job", "a.*"])]) == [other]
    assert labels("labels_equal", [input | texts(["job", "instance"])]) == [other]
  end

  test "label_value reads a label as the value where there is one" do
    {:ok, [one]} =
      LabelFunctions.apply("label_value", [
        [series(%{"__name__" => "m", "v" => "1.5"}, [1.0, nil])],
        text("v")
      ])

    assert one.labels == %{"v" => "1.5"}
    assert Series.values(one) == [1.5, nil]
  end

  test "label_graphite_group picks parts of a dotted name" do
    input = [series(%{"__name__" => "a.b.c.d"})]

    args = [
      input,
      [Series.constant(@grid, 1.0)],
      [Series.constant(@grid, 3.0)],
      [Series.constant(@grid, 9.0)]
    ]

    assert labels("label_graphite_group", args) == [%{"__name__" => "b.d."}]
  end

  test "compile/2 and replace_all/4" do
    assert {:ok, regex} = LabelFunctions.compile("a+", false)
    assert LabelFunctions.replace_all(regex, "baab", "[$0]", :all) == "b[aa]b"
    assert {:error, {:invalid_argument, message}} = LabelFunctions.compile("invalid(regexp", true)
    assert message =~ "cannot compile regex"
  end

  test "an argument that is not a string" do
    assert {:error, {:invalid_argument, "arg #2 must be a string"}} =
             LabelFunctions.apply("label_set", [
               [series(@m)],
               text("a"),
               [Series.constant(@grid, 3.0)]
             ])
  end
end
