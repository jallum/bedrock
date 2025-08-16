defmodule Bedrock.DataPlane.Storage.Basalt.KeyspaceTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Storage.Basalt.Keyspace
  alias Bedrock.DataPlane.Version

  def new_random_keyspace, do: Keyspace.new(:"keyspace_#{Faker.random_between(0, 10_000)}")

  describe "Keyspace.new/1" do
    test "it returns a new ETS table" do
      assert is_reference(Keyspace.new(:foo))
    end
  end

  describe "Keyspace.apply_transaction/2" do
    test "it adds the keys to the space" do
      keyspace = new_random_keyspace()

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
                   "a" => "a",
                   "c" => "c",
                   "d" => "d",
                   "e" => "e"
                 })
               )

      version_0 = Version.from_integer(0)

      assert [
               {:last_version, ^version_0},
               {"a", true},
               {"c", true},
               {"d", true},
               {"e", true}
             ] = :ets.tab2list(keyspace)
    end

    test "it adds the keys to the space that already has keys" do
      keyspace = new_random_keyspace()

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
                   "a" => "a",
                   "c" => "c",
                   "d" => "d",
                   "e" => "e"
                 })
               )

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "f" => "f",
                   "g" => "g",
                   "h" => "h",
                   "i" => "i"
                 })
               )

      version_1 = Version.from_integer(1)

      assert [
               {:last_version, ^version_1},
               {"a", true},
               {"c", true},
               {"d", true},
               {"e", true},
               {"f", true},
               {"g", true},
               {"h", true},
               {"i", true}
             ] = :ets.tab2list(keyspace)
    end

    test "it removes keys properly" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "c" => nil,
                   "d" => nil
                 })
               )

      version_1 = Version.from_integer(1)

      assert [
               {:last_version, ^version_1},
               {"a", true},
               {"c", false},
               {"d", false},
               {"e", true}
             ] = keyspace |> :ets.tab2list()
    end
  end

  describe "Keyspace.prune/2" do
    test "it suceeds and changes nothing when there are no keys to prune" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      assert {:ok, 0} = Keyspace.prune(keyspace)

      version_0 = Version.from_integer(0)

      assert [
               {:last_version, ^version_0},
               {"a", true},
               {"c", true},
               {"d", true},
               {"e", true}
             ] = :ets.tab2list(keyspace)
    end

    test "it succeeds and removes the keys when there are keys to prune" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "a" => nil,
            "d" => nil
          })
        )

      assert {:ok, 2} = Keyspace.prune(keyspace)

      version_1 = Version.from_integer(1)

      assert [
               {:last_version, ^version_1},
               {"c", true},
               {"e", true}
             ] = :ets.tab2list(keyspace)
    end
  end

  describe "Keyspace.key_exists?/2" do
    test "it returns true when the key exists" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      assert true = Keyspace.key_exists?(keyspace, "a")
    end

    test "it returns false when the key does not exist" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      refute false = Keyspace.key_exists?(keyspace, "q")
    end
  end
end
