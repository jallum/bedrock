defmodule Bedrock.Cluster.Gateway.TransactionBuilderTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  # This test file focuses on GenServer-specific functionality:
  # - Process lifecycle (start_link, initialization, termination)
  # - Message handling (handle_call, handle_cast, handle_info)
  # - GenServer state management
  # - Process supervision and error handling
  #
  # For end-to-end transaction flows, see transaction_builder_integration_test.exs
  # For individual module testing, see the respective module test files

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

  describe "GenServer process lifecycle" do
    test "starts successfully with valid options" do
      pid = start_transaction_builder()
      assert Process.alive?(pid)

      # Verify initial state structure
      state = :sys.get_state(pid)
      assert %State{} = state
      assert state.state == :valid
      assert state.gateway == self()
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.stack == []
      assert %Tx{} = state.tx
    end

    test "fails to start with missing required options" do
      assert_raise KeyError, fn ->
        TransactionBuilder.start_link([])
      end

      assert_raise KeyError, fn ->
        TransactionBuilder.start_link(gateway: self())
      end
    end

    test "terminates normally on timeout message" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "starts with custom configuration" do
      custom_layout = %{sequencer: :custom, proxies: []}

      pid =
        start_transaction_builder(
          transaction_system_layout: custom_layout,
          key_codec: TestKeyCodec,
          value_codec: TestValueCodec
        )

      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
    end
  end

  describe "GenServer call handling" do
    test ":nested_transaction call creates stack frame" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      initial_stack_size = length(initial_state.stack)

      # Call nested_transaction
      result = GenServer.call(pid, :nested_transaction)
      assert result == :ok

      final_state = :sys.get_state(pid)
      # Stack should have one more frame
      assert length(final_state.stack) == initial_stack_size + 1
      # Process should still be alive
      assert Process.alive?(pid)
    end

    test ":fetch call with cached key" do
      pid = start_transaction_builder()

      # Put a value first via cast
      GenServer.cast(pid, {:put, "test_key", "test_value"})
      :timer.sleep(10)

      # Call fetch - should return cached value
      result = GenServer.call(pid, {:fetch, "test_key"})
      assert result == {:ok, "test_value"}
    end

    test ":commit call returns error for empty transaction" do
      pid = start_transaction_builder()

      # Call commit on empty transaction
      result = GenServer.call(pid, :commit)
      # Should return an error (infrastructure not available)
      assert {:error, _reason} = result
      # Process should still be alive
      assert Process.alive?(pid)
    end

    test "multiple :nested_transaction calls stack properly" do
      pid = start_transaction_builder()

      # First nested call
      :ok = GenServer.call(pid, :nested_transaction)
      state1 = :sys.get_state(pid)
      assert length(state1.stack) == 1

      # Second nested call
      :ok = GenServer.call(pid, :nested_transaction)
      state2 = :sys.get_state(pid)
      assert length(state2.stack) == 2
    end

    @tag :capture_log
    test "unknown call crashes process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      catch_exit(GenServer.call(pid, :unknown_call))

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({:function_clause, _}, reason)
    end
  end

  describe "GenServer cast handling" do
    test "{:put, key, value} cast updates transaction state" do
      pid = start_transaction_builder()

      initial_state = :sys.get_state(pid)
      initial_mutations = Tx.commit(initial_state.tx).mutations

      # Cast put operation
      GenServer.cast(pid, {:put, "test_key", "test_value"})
      # Allow cast to process
      :timer.sleep(10)

      final_state = :sys.get_state(pid)
      final_mutations = Tx.commit(final_state.tx).mutations

      assert final_mutations == initial_mutations ++ [{:set, "test_key", "test_value"}]
    end

    test ":rollback cast with empty stack terminates process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Cast rollback with empty stack
      GenServer.cast(pid, :rollback)

      # Process should terminate normally
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test ":rollback cast with non-empty stack pops stack frame" do
      pid = start_transaction_builder()

      # Create nested transaction to populate stack
      :ok = GenServer.call(pid, :nested_transaction)
      state_before = :sys.get_state(pid)
      assert length(state_before.stack) == 1

      # Cast rollback
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      state_after = :sys.get_state(pid)
      assert Enum.empty?(state_after.stack)
      assert Process.alive?(pid)
    end

    test "multiple put casts accumulate in transaction" do
      pid = start_transaction_builder()

      # Multiple put casts
      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key2", "value2"})
      GenServer.cast(pid, {:put, "key3", "value3"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      mutations = Tx.commit(state.tx).mutations

      assert mutations == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"},
               {:set, "key3", "value3"}
             ]
    end

    test "put cast with same key overwrites in mutations but not state" do
      pid = start_transaction_builder()

      # Put same key twice
      GenServer.cast(pid, {:put, "key1", "value1"})
      GenServer.cast(pid, {:put, "key1", "updated_value"})
      :timer.sleep(10)

      state = :sys.get_state(pid)
      commit_result = Tx.commit(state.tx)

      assert commit_result.mutations == [
               {:set, "key1", "updated_value"}
             ]

      result = GenServer.call(pid, {:fetch, "key1"})
      assert result == {:ok, "updated_value"}
    end

    @tag :capture_log
    test "unknown cast crashes process" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      GenServer.cast(pid, :unknown_cast)

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({:function_clause, _}, reason)
    end
  end

  describe "GenServer info message handling" do
    test ":timeout message terminates process normally" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      send(pid, :timeout)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    @tag :capture_log
    test "unknown info messages crash the process" do
      # This test validates fail-fast behavior - TransactionBuilder should crash on unknown info messages
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Send unknown info message - will crash due to no catch-all clause
      send(pid, :unknown_message)

      # Process should crash with FunctionClauseError
      assert_receive {:DOWN, ^ref, :process, ^pid, reason}
      assert match?({:function_clause, _}, reason)
    end
  end

  describe "GenServer state management" do
    test "state is properly initialized with defaults" do
      pid = start_transaction_builder()

      state = :sys.get_state(pid)

      # Verify state structure and defaults
      assert %State{} = state
      assert state.state == :valid
      assert state.gateway == self()
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.read_version == nil
      assert state.commit_version == nil
      assert state.stack == []
      assert state.fastest_storage_servers == %{}
      assert is_integer(state.fetch_timeout_in_ms)
      assert is_integer(state.lease_renewal_threshold)
      assert %Tx{} = state.tx
    end

    test "state fields are preserved across operations" do
      custom_layout = %{custom: :test_layout}
      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      # Perform various operations
      GenServer.cast(pid, {:put, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(10)

      state = :sys.get_state(pid)

      # Core configuration should be preserved
      assert state.state == :valid
      assert state.transaction_system_layout == custom_layout
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.gateway == self()
    end

    test "stack management through GenServer operations" do
      pid = start_transaction_builder()

      # Initial stack should be empty
      initial_state = :sys.get_state(pid)
      assert Enum.empty?(initial_state.stack)

      # Add some data and nest
      GenServer.cast(pid, {:put, "base", "value"})
      :ok = GenServer.call(pid, :nested_transaction)

      nested_state = :sys.get_state(pid)
      assert length(nested_state.stack) == 1

      # Add more data and nest again
      GenServer.cast(pid, {:put, "nested", "value"})
      :ok = GenServer.call(pid, :nested_transaction)

      double_nested_state = :sys.get_state(pid)
      assert length(double_nested_state.stack) == 2

      # Rollback should reduce stack
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      after_rollback_state = :sys.get_state(pid)
      assert length(after_rollback_state.stack) == 1
    end

    test "transaction state accumulates properly" do
      pid = start_transaction_builder()

      # Add operations incrementally
      GenServer.cast(pid, {:put, "key1", "value1"})
      :timer.sleep(5)

      state1 = :sys.get_state(pid)
      mutations1 = Tx.commit(state1.tx).mutations
      assert length(mutations1) == 1

      GenServer.cast(pid, {:put, "key2", "value2"})
      :timer.sleep(5)

      state2 = :sys.get_state(pid)
      mutations2 = Tx.commit(state2.tx).mutations
      assert length(mutations2) == 2

      assert mutations2 == [
               {:set, "key1", "value1"},
               {:set, "key2", "value2"}
             ]
    end
  end

  describe "GenServer error handling and resilience" do
    test "process remains stable under normal message load" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Send many messages rapidly
      for i <- 1..50 do
        GenServer.cast(pid, {:put, "key_#{i}", "value_#{i}"})

        if rem(i, 10) == 0 do
          :ok = GenServer.call(pid, :nested_transaction)
        end
      end

      :timer.sleep(50)

      # Process should still be alive
      assert Process.alive?(pid)

      # No DOWN message should be received
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 10

      # State should be valid
      state = :sys.get_state(pid)
      assert state.state == :valid
      # 50/10 nested calls
      assert length(state.stack) == 5
    end

    test "process handles rapid rollbacks correctly" do
      pid = start_transaction_builder()

      # Build up nested stack
      for _i <- 1..5 do
        :ok = GenServer.call(pid, :nested_transaction)
      end

      initial_state = :sys.get_state(pid)
      assert length(initial_state.stack) == 5

      # Rapid rollbacks
      for _i <- 1..3 do
        GenServer.cast(pid, :rollback)
      end

      :timer.sleep(20)

      # Process should still be alive with reduced stack
      assert Process.alive?(pid)
      final_state = :sys.get_state(pid)
      assert length(final_state.stack) == 2
    end

    test "process terminates cleanly on final rollback" do
      pid = start_transaction_builder()
      ref = Process.monitor(pid)

      # Single rollback on empty stack should terminate
      GenServer.cast(pid, :rollback)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end
  end

  describe "GenServer configuration and customization" do
    test "custom codecs are applied and preserved" do
      defmodule CustomKeyCodec do
        def encode_key(key), do: {:ok, "custom_#{key}"}
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

      # Verify codecs are set
      state = :sys.get_state(pid)
      assert state.key_codec == CustomKeyCodec
      assert state.value_codec == CustomValueCodec

      # Verify codecs are used
      GenServer.cast(pid, {:put, "test", "value"})
      :timer.sleep(10)

      final_state = :sys.get_state(pid)
      mutations = Tx.commit(final_state.tx).mutations
      assert [{:set, "custom_test", "encoded_value"}] = mutations
    end

    test "transaction system layout is preserved" do
      custom_layout = %{
        sequencer: :custom_sequencer,
        proxies: [:custom_proxy1, :custom_proxy2],
        storage_teams: [%{custom: :team}],
        services: %{custom: :service}
      }

      pid = start_transaction_builder(transaction_system_layout: custom_layout)

      # Perform operations that might affect layout
      GenServer.cast(pid, {:put, "key", "value"})
      :ok = GenServer.call(pid, :nested_transaction)
      GenServer.cast(pid, :rollback)
      :timer.sleep(10)

      # Layout should be unchanged
      state = :sys.get_state(pid)
      assert state.transaction_system_layout == custom_layout
    end
  end
end
