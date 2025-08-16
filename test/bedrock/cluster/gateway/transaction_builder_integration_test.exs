defmodule Bedrock.Cluster.Gateway.TransactionBuilderIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  defmodule TestKeyCodec do
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
    def encode_value(value), do: {:ok, value}
    def decode_value(value), do: {:ok, value}
  end

  def create_test_transaction_system_layout do
    %{
      sequencer: :test_sequencer,
      proxies: [:test_proxy1, :test_proxy2],
      storage_teams: [
        %{
          key_range: {"", :end},
          storage_ids: ["storage1", "storage2"]
        }
      ],
      services: %{
        "storage1" => %{kind: :storage, status: {:up, :test_storage1_pid}},
        "storage2" => %{kind: :storage, status: {:up, :test_storage2_pid}}
      }
    }
  end

  def start_transaction_builder(opts \\ []) do
    default_opts = [
      gateway: self(),
      transaction_system_layout: create_test_transaction_system_layout(),
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec
    ]

    opts = Keyword.merge(default_opts, opts)
    start_supervised!({TransactionBuilder, opts})
  end

  describe "end-to-end transaction flows" do
    test "simple put -> fetch -> commit flow" do
      pid = start_transaction_builder()

      # Put a value
      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      # Fetch the value (should come from writes cache)
      result = GenServer.call(pid, {:fetch, "test_key"})
      assert result == {:ok, "test_value"}

      # Verify transaction state contains the write
      state = :sys.get_state(pid)

      assert Tx.commit(state.tx) == %{
               mutations: [{:set, "test_key", "test_value"}],
               write_conflicts: [{"test_key", "test_key\0"}],
               read_conflicts: {nil, []}
             }

      # Commit will fail due to missing infrastructure, but we can verify it tries
      result = GenServer.call(pid, :commit)
      assert {:error, _reason} = result
    end

    test "multiple operations in sequence" do
      pid = start_transaction_builder()

      # Multiple puts
      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key2", "value2"})
      GenServer.cast(pid, {:put, "key3", "value3"})
      :timer.sleep(10)

      # Fetch all values
      assert GenServer.call(pid, {:fetch, "key1"}) == {:ok, "value1"}
      assert GenServer.call(pid, {:fetch, "key2"}) == {:ok, "value2"}
      assert GenServer.call(pid, {:fetch, "key3"}) == {:ok, "value3"}

      # Verify final transaction state
      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"},
               {:set, "key3", "value3"}
             ]
    end

    test "put overwrite behavior" do
      pid = start_transaction_builder()

      # Initial put
      GenServer.cast(pid, {:put, "key", "initial_value"})
      :timer.sleep(10)

      # Verify initial value
      assert GenServer.call(pid, {:fetch, "key"}) == {:ok, "initial_value"}

      # Overwrite
      GenServer.cast(pid, {:put, "key", "updated_value"})
      :timer.sleep(10)

      # Verify updated value
      assert GenServer.call(pid, {:fetch, "key"}) == {:ok, "updated_value"}

      # Transaction should contain both operations
      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key", "updated_value"}
             ]
    end
  end

  describe "nested transaction integration" do
    test "nested transaction with operations across levels" do
      pid = start_transaction_builder()

      # Base level operations
      GenServer.cast(pid, {:put, "base_key", "base_value"})
      :timer.sleep(10)

      # Start nested transaction
      :ok = GenServer.call(pid, :nested_transaction)

      # Operations in nested transaction
      GenServer.cast(pid, {:put, "nested_key", "nested_value"})
      # Overwrite base key
      GenServer.cast(pid, {:put, "base_key", "overwritten_value"})
      :timer.sleep(10)

      # Fetch operations should see nested changes
      assert GenServer.call(pid, {:fetch, "base_key"}) == {:ok, "overwritten_value"}
      assert GenServer.call(pid, {:fetch, "nested_key"}) == {:ok, "nested_value"}

      # Verify nested transaction state
      state = :sys.get_state(pid)
      assert length(state.stack) == 1

      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "nested_key", "nested_value"},
               {:set, "base_key", "overwritten_value"}
             ]
    end

    test "rollback restores previous transaction state" do
      pid = start_transaction_builder()

      # Base transaction
      GenServer.cast(pid, {:put, "persistent_key", "persistent_value"})
      :timer.sleep(10)

      # Start nested transaction
      :ok = GenServer.call(pid, :nested_transaction)

      # Nested operations
      GenServer.cast(pid, {:put, "temporary_key", "temporary_value"})
      GenServer.cast(pid, {:put, "persistent_key", "modified_value"})
      :timer.sleep(10)

      # Verify nested state before rollback
      assert GenServer.call(pid, {:fetch, "persistent_key"}) == {:ok, "modified_value"}
      assert GenServer.call(pid, {:fetch, "temporary_key"}) == {:ok, "temporary_value"}

      # Rollback nested transaction
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      # Verify rollback restored base state
      state = :sys.get_state(pid)
      assert state.stack == []

      # Should still be able to fetch base values
      assert GenServer.call(pid, {:fetch, "persistent_key"}) == {:ok, "persistent_value"}

      # Process should still be alive
      assert Process.alive?(pid)
    end

    test "multiple nested levels with rollback" do
      pid = start_transaction_builder()

      # Level 0
      GenServer.cast(pid, {:put, "level0", "value0"})
      :timer.sleep(10)

      # Level 1
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "level1", "value1"})
      :timer.sleep(10)

      # Level 2
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "level2", "value2"})
      :timer.sleep(10)

      # Verify all levels visible
      assert GenServer.call(pid, {:fetch, "level0"}) == {:ok, "value0"}
      assert GenServer.call(pid, {:fetch, "level1"}) == {:ok, "value1"}
      assert GenServer.call(pid, {:fetch, "level2"}) == {:ok, "value2"}

      # Rollback from level 2 to level 1
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state = :sys.get_state(pid)
      # Should have level 0 on stack
      assert length(state.stack) == 1

      # Level 2 operations should be gone, but level 0 and 1 should remain
      assert GenServer.call(pid, {:fetch, "level0"}) == {:ok, "value0"}
      assert GenServer.call(pid, {:fetch, "level1"}) == {:ok, "value1"}
    end
  end

  describe "error propagation through system" do
    @tag :capture_log
    test "codec errors propagate through put operations" do
      defmodule FailingKeyCodec do
        def encode_key(_), do: :key_error
      end

      # This test validates fail-fast behavior - codec errors should crash TransactionBuilder
      pid = start_transaction_builder(key_codec: FailingKeyCodec)
      ref = Process.monitor(pid)

      # Put with invalid key should cause process to crash
      GenServer.cast(pid, {:put, :invalid_key, "value"})

      # Process should crash with KeyError due to codec validation
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({%KeyError{message: "key must be a binary"}, _}, reason)
    end

    @tag :capture_log
    test "fetch operations with missing keys cause runtime error" do
      # This test validates fail-fast behavior - missing read version should crash TransactionBuilder
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Fetch should cause exit due to RuntimeError in GenServer
      catch_exit(GenServer.call(pid, {:fetch, "non_existent_key"}))

      # Process should crash with RuntimeError about no read version
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}

      assert match?(
               {%RuntimeError{message: "No read version available for fetching key: " <> _}, _},
               reason
             )
    end

    test "commit with no writes returns error" do
      pid = start_transaction_builder()

      # Empty transaction should fail to commit
      result = GenServer.call(pid, :commit)

      # Should get an error (likely proxy unavailable or similar)
      assert {:error, _reason} = result

      # Process should still be alive after failed commit
      assert Process.alive?(pid)
    end
  end

  describe "process lifecycle integration" do
    test "process state consistency during complex operations" do
      pid = start_transaction_builder()

      # Complex sequence of operations
      GenServer.cast(pid, {:put, "key1", "value1"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key3", "value3"})
      :timer.sleep(10)

      # Verify consistent state throughout
      state = :sys.get_state(pid)
      assert state.state == :valid
      assert length(state.stack) == 2
      assert Process.alive?(pid)

      # All operations should be visible
      assert GenServer.call(pid, {:fetch, "key1"}) == {:ok, "value1"}
      assert GenServer.call(pid, {:fetch, "key2"}) == {:ok, "value2"}
      assert GenServer.call(pid, {:fetch, "key3"}) == {:ok, "value3"}
    end

    test "timeout handling maintains state consistency" do
      pid = start_transaction_builder()

      # Add some operations
      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      # Verify initial state
      initial_state = :sys.get_state(pid)
      assert initial_state.state == :valid

      # Send timeout message
      ref = Process.monitor(pid)
      send(pid, :timeout)

      # Process should terminate normally
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "rollback on empty stack terminates process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Add some operations first
      GenServer.cast(pid, {:put, "key", "value"})
      :timer.sleep(10)

      # Rollback with empty stack should terminate
      GenServer.cast(pid, :rollback)

      # Process should terminate normally
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end
  end

  describe "codec integration across operations" do
    test "custom codecs work end-to-end" do
      defmodule CustomKeyCodec do
        def encode_key(key), do: {:ok, "encoded_#{key}"}
      end

      defmodule CustomValueCodec do
        def encode_value(value), do: {:ok, "encoded_#{value}"}
        def decode_value(value), do: {:ok, value}
      end

      pid =
        start_transaction_builder(
          key_codec: CustomKeyCodec,
          value_codec: CustomValueCodec
        )

      # Put with custom codec
      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      # Fetch should work with encoded values
      result = GenServer.call(pid, {:fetch, "test_key"})
      assert result == {:ok, "encoded_test_value"}

      # Verify transaction has encoded values
      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)
      assert commit_result.mutations == [{:set, "encoded_test_key", "encoded_test_value"}]
    end
  end

  describe "configuration preservation" do
    test "transaction system layout preserved across operations" do
      custom_layout = %{
        sequencer: :custom_sequencer,
        proxies: [:custom_proxy],
        storage_teams: [],
        services: %{}
      }

      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      # Perform various operations
      GenServer.cast(pid, {:put, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(10)

      # Layout should be preserved
      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
    end

    test "codec configuration preserved across nested transactions" do
      defmodule PersistentKeyCodec do
        def encode_key(key), do: {:ok, "persistent_#{key}"}
      end

      pid = start_transaction_builder(key_codec: PersistentKeyCodec)

      # Operations across nested levels
      GenServer.cast(pid, {:put, "base", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "nested", "value"})
      :timer.sleep(10)

      # Codec should be preserved
      state = :sys.get_state(pid)
      assert state.key_codec == PersistentKeyCodec

      # Verify codec was used
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "persistent_base", "value"},
               {:set, "persistent_nested", "value"}
             ]
    end
  end
end
