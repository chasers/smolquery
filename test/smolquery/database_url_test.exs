defmodule Smolquery.DatabaseUrlTest do
  use ExUnit.Case, async: true

  alias Smolquery.DatabaseUrl

  describe "parse!/1" do
    test "splits a full URL into decoded components" do
      assert DatabaseUrl.parse!("postgres://app:p%40ss%2Fword@db.internal:6432/lake") == %{
               hostname: "db.internal",
               port: 6432,
               username: "app",
               password: "p@ss/word",
               database: "lake"
             }
    end

    test "defaults port, user, and database like the postgres:// scheme" do
      assert DatabaseUrl.parse!("postgres://db.internal") == %{
               hostname: "db.internal",
               port: 5432,
               username: "postgres",
               password: "",
               database: "smolquery"
             }
    end

    test "a plus in the password is a literal plus, not a space" do
      assert %{password: "a+b"} = DatabaseUrl.parse!("postgres://app:a+b@host/db")
    end

    test "raises on a URL without a host" do
      assert_raise ArgumentError, ~r/has no host/, fn ->
        DatabaseUrl.parse!("not-a-url")
      end
    end

    test "raises on query parameters rather than silently dropping them" do
      assert_raise ArgumentError, ~r/query parameters/, fn ->
        DatabaseUrl.parse!("postgres://app@host/db?sslmode=require")
      end
    end
  end

  describe "libpq_metadata/1" do
    test "quotes every value" do
      metadata =
        "postgres://app:pw@db.internal/lake"
        |> DatabaseUrl.parse!()
        |> DatabaseUrl.libpq_metadata()

      assert metadata ==
               "postgres:dbname='lake' host='db.internal' port='5432' " <>
                 "user='app' password='pw'"
    end

    test "escapes quotes and backslashes per libpq's rules" do
      metadata =
        "postgres://app:pa%27ss%5C@host/db"
        |> DatabaseUrl.parse!()
        |> DatabaseUrl.libpq_metadata()

      assert metadata =~ ~S(password='pa\'ss\\')
    end

    test "a password with spaces stays one value" do
      metadata =
        "postgres://app:one%20two@host/db"
        |> DatabaseUrl.parse!()
        |> DatabaseUrl.libpq_metadata()

      assert metadata =~ "password='one two'"
    end
  end
end
