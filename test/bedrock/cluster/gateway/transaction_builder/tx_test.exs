defmodule Bedrock.Cluster.Gateway.TransactionBuilder.TxTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  # Helper function to get the transaction map from Tx.commit/1
  defp decode_commit(tx) do
    # Tx.commit/1 now returns the map directly
    Tx.commit(tx)
  end

  describe "new/0" do
    test "creates empty transaction" do
      tx = Tx.new()

      assert %Tx{
               mutations: [],
               writes: %{},
               reads: %{},
               range_writes: [],
               range_reads: []
             } = tx

      assert %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end
  end

  describe "set/3" do
    test "sets key-value pair" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")

      assert %Tx{
               mutations: [{:set, "key1", "value1"}],
               writes: %{"key1" => "value1"},
               reads: %{},
               range_writes: [],
               range_reads: []
             } = tx

      assert %{
               mutations: [{:set, "key1", "value1"}],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "sets multiple key-value pairs" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        |> Tx.set("key3", "value3")

      assert %Tx{
               mutations: [
                 {:set, "key3", "value3"},
                 {:set, "key2", "value2"},
                 {:set, "key1", "value1"}
               ],
               writes: %{
                 "key1" => "value1",
                 "key2" => "value2",
                 "key3" => "value3"
               },
               reads: %{},
               range_writes: [],
               range_reads: []
             } = tx

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:set, "key2", "value2"},
                 {:set, "key3", "value3"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = decode_commit(tx)

      assert write_conflicts == [
               {"key1", "key1\0"},
               {"key2", "key2\0"},
               {"key3", "key3\0"}
             ]
    end

    test "overwrites existing key" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key1", "updated_value")

      # Assert on whole struct state (optimization removes previous sets to same key)
      assert %Tx{
               mutations: [
                 {:set, "key1", "updated_value"}
               ],
               writes: %{"key1" => "updated_value"},
               reads: %{},
               range_writes: [],
               range_reads: []
             } = tx

      # Optimization removes previous operations, only final set remains
      assert %{
               mutations: [
                 {:set, "key1", "updated_value"}
               ],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "handles empty string key and value" do
      tx =
        Tx.new()
        |> Tx.set("", "")

      assert %{
               mutations: [{:set, "", ""}],
               write_conflicts: [{"", "\0"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "handles unicode keys and values" do
      tx =
        Tx.new()
        |> Tx.set("键名", "值")

      assert %{
               mutations: [{:set, "键名", "值"}],
               write_conflicts: [{"键名", "键名\0"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "handles binary data with null bytes" do
      binary_key = "\x00\x01\xFF\x02"
      binary_value = "\xFF\x00\x01\x02"

      tx =
        Tx.new()
        |> Tx.set(binary_key, binary_value)

      expected_end_key = binary_key <> "\0"

      assert %{
               mutations: [{:set, ^binary_key, ^binary_value}],
               write_conflicts: [{^binary_key, ^expected_end_key}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end
  end

  describe "clear/2" do
    test "clears single key" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.clear("key2")

      assert %Tx{
               mutations: [
                 {:clear, "key2"},
                 {:set, "key1", "value1"}
               ],
               writes: %{
                 "key1" => "value1",
                 "key2" => :clear
               },
               reads: %{},
               range_writes: [],
               range_reads: []
             } = tx

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:clear, "key2"}
               ],
               write_conflicts: [
                 {"key1", "key1\0"},
                 {"key2", "key2\0"}
               ],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "clear overwrites existing key" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.clear("key1")

      assert %Tx{
               mutations: [
                 {:clear, "key1"}
               ],
               writes: %{"key1" => :clear},
               reads: %{},
               range_writes: [],
               range_reads: []
             } = tx

      assert %{
               mutations: [
                 {:clear, "key1"}
               ],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end
  end

  describe "clear_range/3" do
    test "clears range of keys" do
      tx =
        Tx.new()
        |> Tx.clear_range("a", "z")

      assert %Tx{
               mutations: [{:clear_range, "a", "z"}],
               writes: %{},
               reads: %{},
               range_writes: [{"a", "z"}],
               range_reads: []
             } = tx

      assert %{
               mutations: [{:clear_range, "a", "z"}],
               write_conflicts: [{"a", "z"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "clears range removes individual ops in range" do
      tx =
        Tx.new()
        |> Tx.set("apple", "fruit")
        |> Tx.set("zebra", "animal")
        |> Tx.set("banana", "fruit")
        |> Tx.clear_range("a", "m")

      # Should have operations in chronological order
      # Apple and banana should be removed by clear_range, only zebra and clear_range remain
      assert %{
               mutations: [
                 {:set, "zebra", "animal"},
                 {:clear_range, "a", "m"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = decode_commit(tx)

      assert write_conflicts == [
               {"a", "m"},
               {"zebra", "zebra\0"}
             ]
    end
  end

  describe "get/4" do
    test "gets value from writes cache" do
      tx =
        Tx.new()
        |> Tx.set("cached_key", "cached_value")

      fetch_fn = fn _key, state ->
        flunk("Should not call fetch_fn when value is in writes cache")
        {{:ok, "should_not_reach"}, state}
      end

      {new_tx, result, state} = Tx.get(tx, "cached_key", fetch_fn, :test_state)

      assert result == {:ok, "cached_value"}
      assert state == :test_state

      assert %Tx{
               mutations: [{:set, "cached_key", "cached_value"}],
               writes: %{"cached_key" => "cached_value"},
               reads: %{},
               range_writes: [],
               range_reads: []
             } = new_tx

      assert new_tx == tx
    end

    test "gets value from reads cache when not in writes" do
      tx =
        Tx.new()
        |> then(&%{&1 | reads: %{"cached_key" => "cached_value"}})

      fetch_fn = fn _key, state ->
        flunk("Should not call fetch_fn when value is in reads cache")
        {{:ok, "should_not_reach"}, state}
      end

      {new_tx, result, state} = Tx.get(tx, "cached_key", fetch_fn, :test_state)

      assert result == {:ok, "cached_value"}
      # No change when reading from reads cache
      assert new_tx == tx
      assert state == :test_state
    end

    test "fetches from storage when not in cache" do
      tx = Tx.new()

      fetch_fn = fn key, state ->
        assert key == "missing_key"
        assert state == :test_state
        {{:ok, "fetched_value"}, :new_state}
      end

      {new_tx, result, new_state} = Tx.get(tx, "missing_key", fetch_fn, :test_state)

      assert result == {:ok, "fetched_value"}
      assert new_state == :new_state
      assert new_tx.reads == %{"missing_key" => "fetched_value"}
    end

    test "handles fetch error" do
      tx = Tx.new()

      fetch_fn = fn _key, state ->
        {{:error, :not_found}, state}
      end

      {new_tx, result, state} = Tx.get(tx, "missing_key", fetch_fn, :test_state)

      assert result == {:error, :not_found}
      assert state == :test_state
      assert new_tx.reads == %{"missing_key" => :clear}
    end

    test "handles cleared key in writes" do
      tx =
        Tx.new()
        |> Tx.clear("cleared_key")

      fetch_fn = fn _key, _state ->
        flunk("Should not call fetch_fn for cleared key")
      end

      {new_tx, result, state} = Tx.get(tx, "cleared_key", fetch_fn, :test_state)

      assert result == {:error, :not_found}
      assert new_tx == tx
      assert state == :test_state
    end

    test "handles cleared key in reads" do
      tx =
        Tx.new()
        |> then(&%{&1 | reads: %{"cleared_key" => :clear}})

      fetch_fn = fn _key, _state ->
        flunk("Should not call fetch_fn for cleared key in reads")
      end

      {new_tx, result, state} = Tx.get(tx, "cleared_key", fetch_fn, :test_state)

      assert result == {:error, :not_found}
      assert new_tx == tx
      assert state == :test_state
    end

    test "writes cache takes precedence over reads cache" do
      tx =
        Tx.new()
        # Value in reads
        |> then(&%{&1 | reads: %{"key" => "old_value"}})
        # Override in writes
        |> Tx.set("key", "new_value")

      fetch_fn = fn _key, _state ->
        flunk("Should not fetch when value is in writes cache")
      end

      {new_tx, result, state} = Tx.get(tx, "key", fetch_fn, :test_state)

      # Gets writes value, not reads
      assert result == {:ok, "new_value"}
      assert new_tx == tx
      assert state == :test_state
    end
  end

  describe "get_range/6" do
    test "gets empty range from empty transaction" do
      tx = Tx.new()

      read_range_fn = fn state, _start, _end, _opts ->
        {[], state}
      end

      {new_tx, results, new_state} = Tx.get_range(tx, "a", "z", read_range_fn, :test_state)

      assert results == []
      assert new_state == :test_state
      assert new_tx.range_reads == [{"a", "z"}]
    end

    test "gets range with only writes" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        # Outside range
        |> Tx.set("key0", "value0")

      read_range_fn = fn state, _start, _end, _opts ->
        # No storage data
        {[], state}
      end

      {new_tx, results, _state} = Tx.get_range(tx, "key1", "key2", read_range_fn, :test_state)

      assert results == [{"key1", "value1"}]
      assert new_tx.range_reads == [{"key1", "key2"}]
      assert new_tx.reads["key1"] == "value1"
    end

    test "merges writes with storage data" do
      tx =
        Tx.new()
        # Override storage
        |> Tx.set("key2", "tx_value2")

      read_range_fn = fn state, start_key, end_key, opts ->
        assert start_key == "key1"
        assert end_key == "key3"
        assert opts[:limit] == 1000
        {[{"key1", "storage_value1"}, {"key2", "storage_value2"}], state}
      end

      {new_tx, results, _state} = Tx.get_range(tx, "key1", "key3", read_range_fn, :test_state)

      # Transaction writes should override storage
      assert results == [{"key1", "storage_value1"}, {"key2", "tx_value2"}]
      assert new_tx.reads["key1"] == "storage_value1"
      assert new_tx.reads["key2"] == "tx_value2"
    end

    test "respects limit parameter" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        |> Tx.set("key3", "value3")

      read_range_fn = fn _state, _start, _end, _opts ->
        {[], :test_state}
      end

      {_tx, results, _state} =
        Tx.get_range(tx, "key1", "key9", read_range_fn, :test_state, limit: 2)

      assert length(results) == 2
      assert results == [{"key1", "value1"}, {"key2", "value2"}]
    end

    test "excludes cleared keys from results" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        # Clear key2
        |> Tx.clear("key2")

      read_range_fn = fn _state, _start, _end, _opts ->
        {[{"key2", "storage_value2"}], :test_state}
      end

      {_tx, results, _state} = Tx.get_range(tx, "key1", "key3", read_range_fn, :test_state)

      # key2 should be excluded because it's cleared
      assert results == [{"key1", "value1"}]
    end

    test "handles clear_range operations" do
      tx =
        Tx.new()
        |> Tx.clear_range("key1", "key3")

      read_range_fn = fn _state, _start, _end, _opts ->
        {[{"key1", "storage_value1"}, {"key2", "storage_value2"}], :test_state}
      end

      {_tx, results, _state} = Tx.get_range(tx, "key1", "key4", read_range_fn, :test_state)

      # All keys in cleared range should be excluded
      assert results == []
    end
  end

  describe "commit/1" do
    test "commits empty transaction" do
      tx = Tx.new()

      assert %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "commits transaction with only writes" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:set, "key2", "value2"}
               ],
               write_conflicts: [
                 {"key1", "key1\0"},
                 {"key2", "key2\0"}
               ],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end

    test "commits transaction with reads and writes" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> then(&%{&1 | reads: %{"read_key" => "read_value"}})

      # Provide a read_version since this transaction has reads
      read_version = Bedrock.DataPlane.Version.from_integer(123)

      assert %{
               mutations: [{:set, "key1", "value1"}],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {^read_version, [{"read_key", "read_key\0"}]}
             } = Tx.commit(tx, read_version)
    end

    test "commits complex transaction with multiple operations" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.clear("key2")
        |> Tx.set("key3", "value3")
        |> then(&%{&1 | reads: %{"read_key" => "read_value"}})

      # Provide a read_version since this transaction has reads
      read_version = Bedrock.DataPlane.Version.from_integer(456)

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:clear, "key2"},
                 {:set, "key3", "value3"}
               ],
               write_conflicts: [
                 {"key1", "key1\0"},
                 {"key2", "key2\0"},
                 {"key3", "key3\0"}
               ],
               read_conflicts: {^read_version, [{"read_key", "read_key\0"}]}
             } = Tx.commit(tx, read_version)
    end

    test "coalesces overlapping ranges in conflicts" do
      tx =
        Tx.new()
        |> then(&%{&1 | range_reads: [{"a", "m"}, {"k", "z"}, {"b", "n"}]})

      # Provide a read_version since this transaction has range reads
      read_version = Bedrock.DataPlane.Version.from_integer(789)

      assert %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {^read_version, [{"a", "m"}, {"k", "z"}, {"b", "n"}]}
             } = Tx.commit(tx, read_version)
    end
  end

  describe "edge cases and error handling" do
    test "handles large keys and values" do
      large_key = String.duplicate("k", 1000)
      large_value = String.duplicate("v", 10_000)

      tx =
        Tx.new()
        |> Tx.set(large_key, large_value)

      assert %{
               mutations: [{:set, ^large_key, ^large_value}],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = decode_commit(tx)

      expected_end_key = large_key <> "\0"
      assert write_conflicts == [{large_key, expected_end_key}]
    end

    test "handles mixed operations" do
      large_key = "large_key"
      large_value = "large_value"

      tx =
        Tx.new()
        |> Tx.set(large_key, large_value)
        |> Tx.clear("clear_key")
        |> Tx.clear_range("a", "b")

      # Mutations should be in chronological order: set large_key -> clear "clear_key" -> clear_range "a","b"
      assert %{
               mutations: [
                 {:set, ^large_key, ^large_value},
                 {:clear, "clear_key"},
                 {:clear_range, "a", "b"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = decode_commit(tx)

      expected_large_end_key = large_key <> "\0"

      assert write_conflicts == [
               {"a", "b"},
               {"clear_key", "clear_key\0"},
               {large_key, expected_large_end_key}
             ]
    end

    test "handles key collision between set and clear" do
      tx =
        Tx.new()
        |> Tx.set("collision_key", "value")
        |> Tx.clear("collision_key")

      # Clear should overwrite the set in writes map
      assert tx.writes["collision_key"] == :clear

      assert %{
               mutations: [
                 {:clear, "collision_key"}
               ],
               write_conflicts: [{"collision_key", "collision_key\0"}],
               read_conflicts: {nil, []}
             } = decode_commit(tx)
    end
  end
end
