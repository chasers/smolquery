defmodule Smolquery.RolesTest do
  use ExUnit.Case, async: false

  alias Smolquery.Roles

  doctest Roles

  describe "all/0" do
    test "lists every subtree in pipeline order, front door first" do
      assert Roles.all() == [:api, :ingest, :buffer, :storage, :query, :web]
    end
  end

  describe "parse/1" do
    test "\"all\" expands to every role" do
      assert Roles.parse("all") == {:ok, Roles.all()}
    end

    test "parses a comma-separated subset" do
      assert Roles.parse("ingest,query") == {:ok, [:ingest, :query]}
    end

    test "returns roles in pipeline order regardless of input order" do
      assert Roles.parse("query,ingest") == {:ok, [:ingest, :query]}
    end

    test "tolerates whitespace and trailing separators" do
      assert Roles.parse(" buffer , storage ,") == {:ok, [:buffer, :storage]}
    end

    test "deduplicates repeated names" do
      assert Roles.parse("query,query") == {:ok, [:query]}
    end

    test "an empty value yields no roles" do
      assert Roles.parse("") == {:ok, []}
    end

    test "reports unknown names rather than ignoring them" do
      assert Roles.parse("ingest,nope") == {:error, ["nope"]}
    end

    test "collects every unknown name" do
      assert Roles.parse("nope,alsonope") == {:error, ["nope", "alsonope"]}
    end
  end

  describe "parse!/1" do
    test "returns roles for a valid value" do
      assert Roles.parse!("storage") == [:storage]
    end

    test "raises with the offending names and the valid set" do
      assert_raise ArgumentError, ~r/unknown SMOLQUERY_ROLES value\(s\): typo/, fn ->
        Roles.parse!("typo")
      end
    end
  end

  describe "enabled/0" do
    test "reflects the application environment" do
      assert Roles.enabled() == Application.fetch_env!(:smolquery, :roles)
    end

    test "defaults to every role when unconfigured" do
      original = Application.fetch_env!(:smolquery, :roles)
      Application.delete_env(:smolquery, :roles)
      on_exit(fn -> Application.put_env(:smolquery, :roles, original) end)

      assert Roles.enabled() == Roles.all()
    end
  end

  describe "enabled?/1" do
    test "is true only for configured roles" do
      original = Application.fetch_env!(:smolquery, :roles)
      Application.put_env(:smolquery, :roles, [:query])
      on_exit(fn -> Application.put_env(:smolquery, :roles, original) end)

      assert Roles.enabled?(:query)
      refute Roles.enabled?(:buffer)
    end
  end
end
