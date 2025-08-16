defmodule Bedrock.Cluster.Gateway.TransactionBuilder.TxBinaryIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.BedrockTransaction

  describe "binary transaction integration" do
    test "commit returns binary transaction" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        |> Tx.clear("key3")

      # commit_binary should return binary
      binary_result = Tx.commit_binary(tx)
      assert is_binary(binary_result)

      assert {:ok, _validated} = BedrockTransaction.validate(binary_result)

      assert {:ok, decoded} = BedrockTransaction.decode(binary_result)

      # Verify mutations are in exact order
      assert decoded.mutations == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"},
               {:clear, "key3"}
             ]

      assert decoded.write_conflicts == [
               {"key1", "key1\0"},
               {"key2", "key2\0"},
               {"key3", "key3\0"}
             ]

      # Read conflicts and version should be empty/nil for this transaction
      assert %{
               read_conflicts: {nil, []}
             } = decoded
    end

    test "transaction with reads generates read conflicts" do
      # Mock fetch function that returns values
      fetch_fn = fn
        "existing_key", state -> {{:ok, "existing_value"}, state}
        _, state -> {{:error, :not_found}, state}
      end

      tx = Tx.new()

      # Add some reads
      {tx, {:ok, _value}, _state} = Tx.get(tx, "existing_key", fetch_fn, :state)
      {tx, {:error, :not_found}, _state} = Tx.get(tx, "missing_key", fetch_fn, :state)

      # Add a write
      tx = Tx.set(tx, "new_key", "new_value")

      # Commit to binary with read_version (required for read_conflicts to be preserved)
      read_version = Bedrock.DataPlane.Version.from_integer(12_345)
      binary_result = Tx.commit_binary(tx, read_version)
      assert {:ok, decoded} = BedrockTransaction.decode(binary_result)

      # Verify decoded structure
      assert %{
               mutations: [{:set, "new_key", "new_value"}],
               read_conflicts: {^read_version, read_conflicts}
             } = decoded

      # Should have read conflicts from the get operations
      assert read_conflicts == [
               {"existing_key", "existing_key\0"},
               {"missing_key", "missing_key\0"}
             ]

      assert decoded.write_conflicts == [{"new_key", "new_key\0"}]
    end

    test "transaction with range operations" do
      tx =
        Tx.new()
        |> Tx.clear_range("start_key", "end_key")
        |> Tx.set("inside_range", "value")

      binary_result = Tx.commit_binary(tx)
      assert {:ok, decoded} = BedrockTransaction.decode(binary_result)

      assert decoded.mutations == [
               {:clear_range, "start_key", "end_key"},
               {:set, "inside_range", "value"}
             ]

      assert decoded.write_conflicts == [
               {"inside_range", "inside_range\0"},
               {"start_key", "end_key"}
             ]
    end

    test "empty transaction produces valid binary" do
      tx = Tx.new()
      binary_result = Tx.commit_binary(tx)

      assert is_binary(binary_result)
      assert {:ok, decoded} = BedrockTransaction.decode(binary_result)

      # Empty transaction should have empty structure
      assert decoded == %{
               mutations: [],
               read_conflicts: {nil, []},
               write_conflicts: []
             }
    end

    test "transaction with range reads" do
      # Mock range read function
      range_read_fn = fn state, start_key, end_key, _opts ->
        case {start_key, end_key} do
          {"a", "z"} -> {[{"key1", "value1"}, {"key2", "value2"}], state}
          _ -> {[], state}
        end
      end

      tx = Tx.new()

      # Perform range read
      {tx, results, _state} = Tx.get_range(tx, "a", "z", range_read_fn, :state)
      assert length(results) == 2

      # Add a mutation
      tx = Tx.set(tx, "new_key", "new_value")

      # Commit with read_version (required for read_conflicts to be preserved)
      read_version = Bedrock.DataPlane.Version.from_integer(54_321)
      binary_result = Tx.commit_binary(tx, read_version)
      assert {:ok, decoded} = BedrockTransaction.decode(binary_result)

      assert decoded.mutations == [{:set, "new_key", "new_value"}]

      # Should have read conflicts from the range read
      {actual_read_version, actual_read_conflicts} = decoded.read_conflicts
      assert actual_read_version == read_version
      assert actual_read_conflicts == [{"a", "z"}]

      assert decoded.write_conflicts == [
               {"new_key", "new_key\0"}
             ]
    end

    test "binary transaction maintains size optimization" do
      # Create transactions with different key/value sizes
      small_tx = Tx.new() |> Tx.set("k", "v")
      medium_tx = Tx.new() |> Tx.set("k", String.duplicate("x", 300))
      large_tx = Tx.new() |> Tx.set(String.duplicate("k", 300), String.duplicate("v", 70_000))

      small_binary = Tx.commit_binary(small_tx)
      medium_binary = Tx.commit_binary(medium_tx)
      large_binary = Tx.commit_binary(large_tx)

      # All should decode correctly
      assert {:ok, _} = BedrockTransaction.decode(small_binary)
      assert {:ok, _} = BedrockTransaction.decode(medium_binary)
      assert {:ok, _} = BedrockTransaction.decode(large_binary)

      # Size optimization should result in smaller binaries for smaller data
      assert byte_size(small_binary) < byte_size(medium_binary)
      assert byte_size(medium_binary) < byte_size(large_binary)
    end

    test "transaction builder integrates with BedrockTransaction section operations" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")

      binary_result = Tx.commit_binary(tx)

      assert {:ok, mutations_section} = BedrockTransaction.extract_section(binary_result, 0x01)
      assert is_binary(mutations_section)
      assert byte_size(mutations_section) > 0

      assert {:ok, stream} = BedrockTransaction.stream_mutations(binary_result)
      mutations = stream |> Enum.to_list()
      assert length(mutations) == 2

      version = Bedrock.DataPlane.Version.from_integer(12_345)
      assert {:ok, stamped} = BedrockTransaction.add_transaction_id(binary_result, version)
      assert {:ok, ^version} = BedrockTransaction.extract_transaction_id(stamped)

      # Original transaction data should be preserved
      assert {:ok, decoded} = BedrockTransaction.decode(stamped)

      assert decoded.mutations == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"}
             ]
    end
  end
end
