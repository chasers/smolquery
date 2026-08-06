defmodule Smolquery.Catalog.DuckLakeRetryTest do
  @moduledoc """
  What `Smolquery.Catalog.DuckLake` will and will not retry a commit for.

  Unlike its siblings this suite is not `:integration` — the predicate is pure,
  and the strings it keys on are the point: they come from DuckDB, DuckLake and
  the metadata database, so an upgrade that renames one must fail here rather
  than in a storage node's retry loop.

  The conflict, lock-contention and replica-identity messages were captured
  from real failures, trimmed only where the SQL body was long. The deadlock
  and serialization ones are Postgres's own wording, wrapped the way the
  captured replica-identity failure showed a metadata error arrives — whole,
  inside `Failed to execute query`. Those two share that wrapper with the
  `refute` below on purpose: what separates them is the Postgres condition
  inside it, which is exactly the distinction this predicate has to draw.

  The `refute` is the regression. DuckDB wraps every commit-time failure in
  `"Failed to commit"`, so matching that prefix classified a Postgres metadata
  database's flat refusal — `ducklake_table_stats` is published but has no
  primary key, so it has no replica identity to update by — as a lost race,
  burned five backoffs on it, and reported `:commit_conflict`. The cause only
  reads in the message the metadata database itself gave.
  """

  use ExUnit.Case, async: true

  alias Smolquery.Catalog.DuckLake

  @wrapper "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\n"

  describe "retryable?/1" do
    test "retries DuckLake's own commit conflict" do
      assert DuckLake.retryable?(%Adbc.Error{
               message:
                 @wrapper <>
                   ~s|Transaction conflict - attempting to delete from file with index "1" | <>
                   "- but another transaction has deleted from it"
             })

      assert DuckLake.retryable?(%Adbc.Error{
               message:
                 @wrapper <>
                   ~s|Transaction conflict - attempting to create table "dup" in schema | <>
                   ~s|"analytics" - but this table has been created by another transaction already|
             })
    end

    test "retries a Postgres metadata deadlock" do
      assert DuckLake.retryable?(%Adbc.Error{
               message:
                 @wrapper <>
                   "Failed to flush changes into DuckLake: Failed to execute query " <>
                   ~s|"UPDATE \"public\".ducklake_table_stats ...": | <>
                   "ERROR:  deadlock detected\n" <>
                   "DETAIL:  Process 1 waits for ShareLock on transaction 2."
             })
    end

    test "retries a Postgres serialization failure" do
      assert DuckLake.retryable?(%Adbc.Error{
               message:
                 @wrapper <>
                   "Failed to flush changes into DuckLake: Failed to execute query " <>
                   ~s|"UPDATE \"public\".ducklake_table_stats ...": | <>
                   "ERROR:  could not serialize access due to concurrent update"
             })
    end

    test "retries SQLite metadata lock contention" do
      assert DuckLake.retryable?(%Adbc.Error{
               message: @wrapper <> "Failed to flush changes into DuckLake: database is locked"
             })
    end

    test "leaves a metadata database's own refusal alone, however it is wrapped" do
      refute DuckLake.retryable?(%Adbc.Error{
               message:
                 @wrapper <>
                   "Failed to flush changes into DuckLake: Failed to execute query " <>
                   ~s|"UPDATE \"public\".ducklake_table_stats SET record_count=8 | <>
                   ~s|WHERE table_id=2;": ERROR:  cannot update table "ducklake_table_stats" | <>
                   "because it does not have a replica identity and publishes updates\n" <>
                   "HINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE."
             })
    end

    test "leaves ordinary query errors alone" do
      refute DuckLake.retryable?(%Adbc.Error{
               message: "Catalog Error: Table with name events does not exist"
             })
    end

    test "answers false for anything that is not an exception" do
      refute DuckLake.retryable?(:closed)
      refute DuckLake.retryable?(nil)
    end
  end
end
