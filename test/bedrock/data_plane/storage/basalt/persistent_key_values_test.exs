defmodule Bedrock.DataPlane.StorageSystem.Engine.Basalt.PersistentKeyValuesTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Storage.Basalt.PersistentKeyValues
  alias Bedrock.DataPlane.Version

  defp random_file_name do
    random_chars =
      :crypto.strong_rand_bytes(16)
      |> Base.encode16()
      |> String.downcase()

    "#{random_chars}.dets"
  end

  defp with_empty_pkv(context) do
    file_name = random_file_name()
    {:ok, pkv} = PersistentKeyValues.open(file_name |> String.to_atom(), file_name)

    :ok =
      PersistentKeyValues.apply_transaction(
        pkv,
        BedrockTransactionTestSupport.new_log_transaction(Version.zero(), %{})
      )

    on_exit(fn ->
      File.rm!(file_name)
    end)

    {:ok, context |> Map.put(:pkv, pkv)}
  end

  describe "Basalt.PersistentKeyValues.open/2" do
    test "opens a persistent key-value store" do
      file_name = random_file_name()

      assert {:ok, pkv} = PersistentKeyValues.open(:test, file_name)

      assert pkv

      assert File.exists?(file_name)
      File.rm!(file_name)
    end
  end

  describe "Basalt.PersistentKeyValues.last_version/1" do
    setup :with_empty_pkv

    test "returns 0 on a newly created key-value store", %{pkv: pkv} do
      zero_version = Version.zero()
      assert ^zero_version = PersistentKeyValues.last_version(pkv)
    end

    test "returns the correct version after storing one transaction", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      zero_version = Version.zero()
      assert ^zero_version = PersistentKeyValues.last_version(pkv)
    end

    test "returns the correct version after storing two transactions", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "foo" => "baz"
          })
        )

      version_one = Version.from_integer(1)
      assert ^version_one = PersistentKeyValues.last_version(pkv)
    end
  end

  describe "Basalt.PersistentKeyValues.apply_transaction/2" do
    setup :with_empty_pkv

    test "stores the given key-values correctly", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      assert {:ok, "bar"} = PersistentKeyValues.fetch(pkv, "foo")
    end

    test "correctly overwrites a previous value for a key", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "foo" => "baz"
          })
        )

      assert {:ok, "baz"} = PersistentKeyValues.fetch(pkv, "foo")
    end

    test "does not allow older transactions to be written after newer ones", %{pkv: pkv} do
      assert :ok =
               PersistentKeyValues.apply_transaction(
                 pkv,
                 BedrockTransactionTestSupport.new_log_transaction(Version.zero(), %{
                   "foo" => "baz"
                 })
               )

      assert :ok =
               PersistentKeyValues.apply_transaction(
                 pkv,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "foo" => "baz"
                 })
               )

      assert :ok =
               PersistentKeyValues.apply_transaction(
                 pkv,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(2), %{
                   "foo" => "baz"
                 })
               )

      assert {:error, :version_too_old} ==
               PersistentKeyValues.apply_transaction(
                 pkv,
                 BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "foo" => "bar"
                 })
               )

      assert {:ok, "baz"} == PersistentKeyValues.fetch(pkv, "foo")
    end
  end

  describe "Basalt.PersistentKeyValues.stream_keys/1" do
    setup :with_empty_pkv

    test "returns the correct set of keys", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "foo" => "bar",
            "a" => "1"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(2), %{
            "foo" => "baz",
            "l" => "3"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(3), %{
            "foo" => "biz",
            "j" => "2"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(4), %{
            "foo" => "buz"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(5), %{
            <<0xFF, 0xFF>> => "system_key"
          })
        )

      assert ["a", "foo", "j", "l", <<0xFF, 0xFF>>] ==
               PersistentKeyValues.stream_keys(pkv)
               |> Enum.to_list()
               |> Enum.sort()
    end
  end
end
