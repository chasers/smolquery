defmodule SmolqueryTest do
  use ExUnit.Case, async: true

  doctest Smolquery

  test "version/0 reports the application version" do
    assert Smolquery.version() == to_string(Application.spec(:smolquery, :vsn))
  end

  test "build/0 carries the version and the commit the build named, or nil without one (T-465)" do
    Application.delete_env(:smolquery, :git_sha)
    assert Smolquery.build() == %{version: Smolquery.version(), sha: nil}

    Application.put_env(:smolquery, :git_sha, "0123456789abcdef0123456789abcdef01234567")
    on_exit(fn -> Application.delete_env(:smolquery, :git_sha) end)

    assert Smolquery.git_sha() == "0123456789abcdef0123456789abcdef01234567"
    assert Smolquery.build().sha == "0123456789abcdef0123456789abcdef01234567"
  end
end
