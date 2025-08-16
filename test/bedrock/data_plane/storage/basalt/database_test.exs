defmodule Bedrock.DataPlane.Storage.Basalt.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Storage.Basalt.Database
  alias Bedrock.DataPlane.Version

  def random_name, do: "basalt_database_#{Faker.random_between(0, 10_000)}" |> String.to_atom()

  describe "Basalt.Database.open/2" do
    @tag :tmp_dir
    test "can open a database successfully", %{tmp_dir: tmp_dir} do
      file_name = Path.join(tmp_dir, "a")
      assert {:ok, db} = Database.open(random_name(), file_name)
      assert db
      assert File.exists?(file_name)
    end
  end

  describe "Basalt.Database.close/1" do
    @tag :tmp_dir
    test "can close a newly-created database successfully", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "b"))

      assert :ok = Database.close(db)
    end
  end

  describe "Basalt.Database.last_durable_version/1" do
    @tag :tmp_dir
    test "returns 0 on a newly created database", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "c"))
      version_0 = Version.from_integer(0)
      assert version_0 == Database.last_durable_version(db)
    end
  end

  describe "Basalt.Database" do
    @tag :tmp_dir
    test "can durably store transactions correctly", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "d"))

      # Write a series of transactions to the DB, each overwriting the previous
      # transaction.
      version_0 = Version.from_integer(0)
      version_1 = Version.from_integer(1)
      version_2 = Version.from_integer(2)
      version_3 = Version.from_integer(3)

      assert ^version_0 =
               Database.apply_transactions(db, [
                 BedrockTransactionTestSupport.new_log_transaction(version_0, %{})
               ])

      assert ^version_1 =
               Database.apply_transactions(db, [
                 BedrockTransactionTestSupport.new_log_transaction(version_1, %{"foo" => "bar"})
               ])

      assert ^version_2 =
               Database.apply_transactions(db, [
                 BedrockTransactionTestSupport.new_log_transaction(version_2, %{
                   "foo" => "baz",
                   "boo" => "bif"
                 })
               ])

      assert ^version_3 =
               Database.apply_transactions(db, [
                 BedrockTransactionTestSupport.new_log_transaction(version_3, %{
                   "foo" => "biz",
                   "bam" => "bom"
                 })
               ])

      assert ^version_0 = Database.last_durable_version(db)
      assert ^version_3 = Database.last_committed_version(db)
      assert 0 = Database.info(db, :n_keys)

      # Ensure durability of the first transaction and check that the last
      # durable version and value is correct.
      assert :ok = Database.ensure_durability_to_version(db, version_1)
      assert ^version_1 = Database.last_durable_version(db)
      assert 1 = Database.info(db, :n_keys)
      assert {:ok, "bar"} = Database.fetch(db, "foo", version_1)
      assert {:ok, "baz"} = Database.fetch(db, "foo", version_2)
      assert {:ok, "biz"} = Database.fetch(db, "foo", version_3)
      assert {:ok, "bif"} = Database.fetch(db, "boo", version_2)
      assert {:ok, "bom"} = Database.fetch(db, "bam", version_3)

      # Ensure durability of the second transaction and check that the last
      # durable version and value is correct and that versions older than
      # this have been properly pruned.
      assert :ok = Database.ensure_durability_to_version(db, version_2)
      assert ^version_2 = Database.last_durable_version(db)
      assert 2 = Database.info(db, :n_keys)
      assert {:error, :version_too_old} = Database.fetch(db, "foo", version_1)
      assert {:ok, "baz"} = Database.fetch(db, "foo", version_2)
      assert {:ok, "biz"} = Database.fetch(db, "foo", version_3)
      assert {:ok, "bif"} = Database.fetch(db, "boo", version_2)
      assert {:ok, "bom"} = Database.fetch(db, "bam", version_3)

      # Ensure durability of the third transaction and check that the last
      # durable version and value is correct and that versions older than
      # this have been properly pruned.
      assert :ok = Database.ensure_durability_to_version(db, version_3)
      assert ^version_3 = Database.last_durable_version(db)
      assert 3 = Database.info(db, :n_keys)
      assert {:error, :version_too_old} = Database.fetch(db, "foo", version_1)
      assert {:error, :version_too_old} = Database.fetch(db, "foo", version_2)
      assert {:ok, "biz"} = Database.fetch(db, "foo", version_3)
      assert {:error, :version_too_old} = Database.fetch(db, "boo", version_2)
      assert {:ok, "bif"} = Database.fetch(db, "boo", version_3)
      assert {:ok, "bom"} = Database.fetch(db, "bam", version_3)
    end
  end
end
