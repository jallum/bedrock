defmodule Bedrock.Cluster.Gateway.TransactionBuilder.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  describe "struct creation and defaults" do
    test "creates state with default values" do
      state = %State{}

      assert state.state == nil
      assert state.gateway == nil
      assert state.transaction_system_layout == nil
      assert state.key_codec == nil
      assert state.value_codec == nil
      assert state.read_version == nil
      assert state.read_version_lease_expiration == nil
      assert state.commit_version == nil

      assert Tx.commit(state.tx) == %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {nil, []}
             }

      assert state.stack == []
      assert state.fastest_storage_servers == %{}
      assert state.fetch_timeout_in_ms == 50
      assert state.lease_renewal_threshold == 100
    end

    test "creates state with custom values" do
      tx = Tx.set(Tx.new(), "key", "value")
      layout = %{sequencer: :test_sequencer}

      state = %State{
        state: :valid,
        gateway: self(),
        transaction_system_layout: layout,
        key_codec: TestKeyCodec,
        value_codec: TestValueCodec,
        read_version: 12_345,
        read_version_lease_expiration: 67_890,
        commit_version: 11_111,
        tx: tx,
        stack: [Tx.new()],
        fastest_storage_servers: %{{:a, :b} => :pid},
        fetch_timeout_in_ms: 200,
        lease_renewal_threshold: 300
      }

      assert state.state == :valid
      assert state.gateway == self()
      assert state.transaction_system_layout == layout
      assert state.key_codec == TestKeyCodec
      assert state.value_codec == TestValueCodec
      assert state.read_version == 12_345
      assert state.read_version_lease_expiration == 67_890
      assert state.commit_version == 11_111
      assert state.tx == tx
      # Stack should contain one empty Tx - use length check since Tx.new() creates new struct
      assert length(state.stack) == 1
      [stacked_tx] = state.stack

      assert Tx.commit(stacked_tx) == %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {nil, []}
             }

      assert state.fastest_storage_servers == %{{:a, :b} => :pid}
      assert state.fetch_timeout_in_ms == 200
      assert state.lease_renewal_threshold == 300
    end
  end

  describe "state transitions" do
    test "valid state transitions" do
      state = %State{state: :valid}

      # Valid -> Committed
      committed_state = %{state | state: :committed, commit_version: 12_345}
      assert committed_state.state == :committed
      assert committed_state.commit_version == 12_345

      # Valid -> Rolled Back
      rolled_back_state = %{state | state: :rolled_back}
      assert rolled_back_state.state == :rolled_back

      # Valid -> Expired
      expired_state = %{state | state: :expired}
      assert expired_state.state == :expired
    end

    test "preserves other fields during state transitions" do
      original_state = %State{
        state: :valid,
        gateway: self(),
        transaction_system_layout: %{test: "layout"},
        key_codec: TestKeyCodec,
        value_codec: TestValueCodec,
        read_version: 12_345,
        tx: Tx.set(Tx.new(), "key", "value"),
        stack: [Tx.new()],
        fastest_storage_servers: %{range: :server}
      }

      committed_state = %{original_state | state: :committed, commit_version: 67_890}

      # State transition fields
      assert committed_state.state == :committed
      assert committed_state.commit_version == 67_890

      # All other fields preserved
      assert committed_state.gateway == original_state.gateway
      assert committed_state.transaction_system_layout == original_state.transaction_system_layout
      assert committed_state.key_codec == original_state.key_codec
      assert committed_state.value_codec == original_state.value_codec
      assert committed_state.read_version == original_state.read_version
      assert committed_state.tx == original_state.tx
      assert committed_state.stack == original_state.stack
      assert committed_state.fastest_storage_servers == original_state.fastest_storage_servers
    end
  end

  describe "transaction stack management" do
    test "pushes transaction to stack" do
      original_tx = Tx.set(Tx.new(), "key1", "value1")
      state = %State{tx: original_tx, stack: []}

      # Push current tx to stack, start new tx
      new_tx = Tx.set(Tx.new(), "key2", "value2")
      state_with_stack = %{state | tx: new_tx, stack: [original_tx]}

      assert length(state_with_stack.stack) == 1
      assert hd(state_with_stack.stack) == original_tx
      assert state_with_stack.tx == new_tx
    end

    test "pops transaction from stack" do
      stacked_tx = Tx.set(Tx.new(), "stacked", "value")
      current_tx = Tx.set(Tx.new(), "current", "value")
      state = %State{tx: current_tx, stack: [stacked_tx]}

      # Pop from stack
      state_after_pop = %{state | tx: stacked_tx, stack: []}

      assert state_after_pop.stack == []
      assert state_after_pop.tx == stacked_tx
    end

    test "handles multiple nested transactions" do
      tx1 = Tx.set(Tx.new(), "key1", "value1")
      tx2 = Tx.set(Tx.new(), "key2", "value2")
      tx3 = Tx.set(Tx.new(), "key3", "value3")

      # Start with tx1
      state = %State{tx: tx1, stack: []}

      # Nest tx2 (push tx1 to stack)
      state = %{state | tx: tx2, stack: [tx1]}

      # Nest tx3 (push tx2 to stack)
      state = %{state | tx: tx3, stack: [tx2, tx1]}

      assert length(state.stack) == 2
      assert state.tx == tx3
      assert hd(state.stack) == tx2
      assert List.last(state.stack) == tx1
    end
  end

  describe "read version and lease management" do
    test "sets read version and lease" do
      state = %State{read_version: nil, read_version_lease_expiration: nil}
      current_time = 50_000
      lease_duration = 5_000

      updated_state = %{
        state
        | read_version: 12_345,
          read_version_lease_expiration: current_time + lease_duration
      }

      assert updated_state.read_version == 12_345
      assert updated_state.read_version_lease_expiration == 55_000
    end

    test "updates lease expiration" do
      current_time = 60_000

      state = %State{
        read_version: 12_345,
        # Expired
        read_version_lease_expiration: current_time - 1000
      }

      renewed_state = %{state | read_version_lease_expiration: current_time + 3000}

      assert renewed_state.read_version == 12_345
      assert renewed_state.read_version_lease_expiration == 63_000
    end

    test "handles zero and large version numbers" do
      state = %State{}

      # Zero version
      zero_state = %{state | read_version: 0}
      assert zero_state.read_version == 0

      # Large version
      large_version = 999_999_999_999
      large_state = %{state | read_version: large_version}
      assert large_state.read_version == large_version
    end
  end

  describe "fastest storage servers management" do
    test "adds fastest storage server" do
      state = %State{fastest_storage_servers: %{}}
      range = {"a", "m"}
      server_pid = :test_pid

      updated_state = %{
        state
        | fastest_storage_servers: Map.put(state.fastest_storage_servers, range, server_pid)
      }

      assert updated_state.fastest_storage_servers == %{range => server_pid}
    end

    test "updates fastest storage servers" do
      range1 = {"a", "m"}
      range2 = {"m", :end}
      state = %State{fastest_storage_servers: %{range1 => :old_pid}}

      updated_state = %{
        state
        | fastest_storage_servers: %{
            range1 => :new_pid,
            range2 => :another_pid
          }
      }

      assert updated_state.fastest_storage_servers == %{
               range1 => :new_pid,
               range2 => :another_pid
             }
    end

    test "handles complex range keys" do
      state = %State{fastest_storage_servers: %{}}

      ranges = [
        {"", :end},
        {"a", "z"},
        # Single character diff
        {"key_prefix", "key_prefiy"},
        # Binary ranges
        {"\x00", "\xFF"}
      ]

      servers = Enum.with_index(ranges, fn range, i -> {range, :"pid_#{i}"} end)
      updated_state = %{state | fastest_storage_servers: Map.new(servers)}

      assert map_size(updated_state.fastest_storage_servers) == 4
      assert updated_state.fastest_storage_servers[{"", :end}] == :pid_0
      assert updated_state.fastest_storage_servers[{"\x00", "\xFF"}] == :pid_3
    end
  end

  describe "timeout configuration" do
    test "default timeout values" do
      state = %State{}

      assert state.fetch_timeout_in_ms == 50
      assert state.lease_renewal_threshold == 100
    end

    test "custom timeout values" do
      state = %State{
        fetch_timeout_in_ms: 1000,
        lease_renewal_threshold: 2000
      }

      assert state.fetch_timeout_in_ms == 1000
      assert state.lease_renewal_threshold == 2000
    end

    test "preserves timeout values during other updates" do
      original_state = %State{
        fetch_timeout_in_ms: 500,
        lease_renewal_threshold: 800
      }

      # Update other fields
      updated_state = %{
        original_state
        | state: :valid,
          read_version: 12_345,
          tx: Tx.set(Tx.new(), "key", "value")
      }

      # Timeout values should be preserved
      assert updated_state.fetch_timeout_in_ms == 500
      assert updated_state.lease_renewal_threshold == 800
    end
  end

  describe "field validation and type checking" do
    test "state field accepts valid states" do
      valid_states = [:valid, :committed, :rolled_back, :expired]

      for valid_state <- valid_states do
        state = %State{state: valid_state}
        assert state.state == valid_state
      end
    end

    test "tx field maintains Tx type" do
      state = %State{}
      assert %Tx{} = state.tx

      new_tx = Tx.set(Tx.new(), "key", "value")
      updated_state = %{state | tx: new_tx}
      assert %Tx{} = updated_state.tx
      assert updated_state.tx == new_tx
    end

    test "stack field maintains list of Tx" do
      state = %State{}
      assert is_list(state.stack)
      assert state.stack == []

      tx1 = Tx.set(Tx.new(), "key1", "value1")
      tx2 = Tx.set(Tx.new(), "key2", "value2")

      updated_state = %{state | stack: [tx1, tx2]}
      assert is_list(updated_state.stack)
      assert length(updated_state.stack) == 2
      assert Enum.all?(updated_state.stack, fn tx -> %Tx{} = tx end)
    end

    test "fastest_storage_servers maintains map type" do
      state = %State{}
      assert is_map(state.fastest_storage_servers)
      assert state.fastest_storage_servers == %{}

      servers = %{{"a", "z"} => :pid1, {"z", :end} => :pid2}
      updated_state = %{state | fastest_storage_servers: servers}
      assert is_map(updated_state.fastest_storage_servers)
      assert updated_state.fastest_storage_servers == servers
    end
  end

  describe "integration with transaction operations" do
    test "state changes during put operations" do
      initial_state = %State{
        state: :valid,
        tx: Tx.new()
      }

      # Simulate put operation effect
      new_tx = Tx.set(initial_state.tx, "key", "value")
      updated_state = %{initial_state | tx: new_tx}

      # State unchanged
      assert updated_state.state == :valid
      assert Tx.commit(updated_state.tx).mutations == [{:set, "key", "value"}]
    end

    test "state changes during commit operations" do
      tx_with_data = Tx.set(Tx.new(), "key", "value")

      initial_state = %State{
        state: :valid,
        tx: tx_with_data
      }

      # Simulate commit operation effect
      committed_state = %{initial_state | state: :committed, commit_version: 12_345}

      assert committed_state.state == :committed
      assert committed_state.commit_version == 12_345
      # Tx should be preserved for inspection
      assert committed_state.tx == tx_with_data
    end

    test "state changes during rollback operations" do
      current_tx = Tx.set(Tx.new(), "current", "value")
      stacked_tx = Tx.set(Tx.new(), "stacked", "value")

      initial_state = %State{
        state: :valid,
        tx: current_tx,
        stack: [stacked_tx]
      }

      # Simulate rollback (pop from stack)
      rolled_back_state = %{initial_state | tx: stacked_tx, stack: []}

      # Still valid after nested rollback
      assert rolled_back_state.state == :valid
      assert rolled_back_state.tx == stacked_tx
      assert rolled_back_state.stack == []
    end
  end
end
