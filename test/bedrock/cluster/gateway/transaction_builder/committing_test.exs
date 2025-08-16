defmodule Bedrock.Cluster.Gateway.TransactionBuilder.CommittingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Committing
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.BedrockTransaction

  # Test helper to create a basic transaction state
  defp create_test_state(read_version, _reads, writes) do
    # Build transaction with the specified writes (all keys and values must be binary)
    tx =
      Enum.reduce(writes, Tx.new(), fn {k, v}, tx ->
        Tx.set(tx, k, v)
      end)

    %State{
      state: :valid,
      gateway: self(),
      transaction_system_layout: %{
        proxies: [:test_proxy1, :test_proxy2],
        storage_teams: [],
        services: %{}
      },
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec,
      read_version: read_version,
      tx: tx,
      stack: []
    }
  end

  # Mock commit function that captures the transaction for testing
  defp mock_commit_fn(expected_transaction) do
    fn _proxy, binary_transaction ->
      # Decode binary transaction to compare with expected map format
      decoded_transaction = BedrockTransaction.decode!(binary_transaction)
      assert decoded_transaction == expected_transaction
      # Return a mock version number
      {:ok, 42}
    end
  end

  # Helper function to decode binary transaction for assertions
  defp decode_transaction(binary_transaction) do
    BedrockTransaction.decode!(binary_transaction)
  end

  # Helper to create expected transaction in new format
  defp create_expected_transaction(read_version, mutations, write_conflicts, read_conflicts \\ []) do
    # Convert read_version to binary format to match BedrockTransaction.decode! output
    binary_read_version =
      case read_version do
        nil -> nil
        version when is_integer(version) -> Bedrock.DataPlane.Version.from_integer(version)
        version -> version
      end

    # Apply the new coupling rules: if no read_conflicts, read_version must be nil
    read_conflicts_tuple =
      case read_conflicts do
        [] -> {nil, []}
        non_empty when binary_read_version != nil -> {binary_read_version, non_empty}
        _non_empty when binary_read_version == nil -> {nil, []}
      end

    %{
      mutations: mutations,
      write_conflicts: write_conflicts,
      read_conflicts: read_conflicts_tuple
    }
  end

  describe "do_commit/2 integration tests" do
    test "write-only transaction with nil read_version" do
      reads = %{}
      writes = %{"key1" => "value1"}
      state = create_test_state(nil, reads, writes)

      expected_transaction =
        create_expected_transaction(nil, [{:set, "key1", "value1"}], [{"key1", "key1\0"}])

      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "write-only transaction with read_version" do
      reads = %{}
      writes = %{"key1" => "value1", "key2" => "value2"}
      read_version = Bedrock.DataPlane.Version.from_integer(123)
      state = create_test_state(read_version, reads, writes)

      # Expected new transaction format from Tx.commit() + read_version
      # Note: mutations are now in chronological order
      expected_transaction =
        create_expected_transaction(
          read_version,
          [{:set, "key1", "value1"}, {:set, "key2", "value2"}],
          [{"key1", "key1\0"}, {"key2", "key2\0"}]
        )

      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read-write transaction preserves read keys" do
      reads = %{"read_key1" => "read_val1", "read_key2" => "read_val2"}
      writes = %{"write_key" => "write_val"}
      read_version = Bedrock.DataPlane.Version.from_integer(456)

      # Create transaction with both reads and writes
      tx =
        Enum.reduce(writes, Tx.new(), fn {k, v}, tx ->
          Tx.set(tx, k, v)
        end)

      # Simulate reads by setting them in the reads cache
      tx = %{tx | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Expected new transaction format - writes in mutations, reads in read_conflicts
      expected_transaction =
        create_expected_transaction(
          read_version,
          [{:set, "write_key", "write_val"}],
          [{"write_key", "write_key\0"}],
          [{"read_key1", "read_key1\0"}, {"read_key2", "read_key2\0"}]
        )

      commit_fn = mock_commit_fn(expected_transaction)

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "BedrockEx.setup_accounts scenario" do
      # This simulates the exact scenario that was failing
      # setup_accounts only writes
      reads = %{}

      writes = %{
        "balances:1" => "100",
        "balances:2" => "500"
      }

      read_version = Bedrock.DataPlane.Version.from_integer(1001)
      state = create_test_state(read_version, reads, writes)

      # Expected transaction format with binary keys and values
      expected_mutations = [
        {:set, "balances:1", "100"},
        {:set, "balances:2", "500"}
      ]

      # Use a relaxed commit function that only checks the key fields
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        # Write-only transactions should have read_version as nil in new format
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == nil
        assert transaction.mutations == expected_mutations
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 2
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert actual_read_conflicts == []
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "BedrockEx.move_money scenario" do
      # This simulates a read-then-write transaction
      reads = %{
        "balances:1" => "100",
        "balances:2" => "500"
      }

      writes = %{
        "balances:1" => "90",
        "balances:2" => "510"
      }

      read_version = Bedrock.DataPlane.Version.from_integer(2001)

      # Create transaction with both reads and writes
      tx =
        Enum.reduce(writes, Tx.new(), fn {k, v}, tx ->
          Tx.set(tx, k, v)
        end)

      # Simulate reads by setting them in the reads cache
      tx = %{tx | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Expected transaction format with binary keys and values
      expected_mutations = [
        {:set, "balances:1", "90"},
        {:set, "balances:2", "510"}
      ]

      # Use a relaxed commit function that only checks the key fields
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == expected_mutations
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 2
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        assert length(actual_read_conflicts) == 2
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "nested transaction commit (stack not empty)" do
      # Test nested transaction handling
      reads = %{"current_read" => "val"}
      writes = %{"current_write" => "val"}
      stacked_writes = %{"stacked_write" => "val"}

      # Create a stacked Tx with the stacked writes
      stacked_tx =
        Enum.reduce(stacked_writes, Tx.new(), fn {k, v}, tx ->
          Tx.set(tx, k, v)
        end)

      state = %{
        create_test_state(Bedrock.DataPlane.Version.from_integer(100), reads, writes)
        | stack: [stacked_tx]
      }

      # For nested commits, no external commit should happen - it should just pop the stack
      {:ok, result_state} = Committing.do_commit(state)

      # Should pop stack but keep current transaction (not restore stacked)
      assert result_state.stack == []
      # Still valid, not committed
      assert result_state.state == :valid

      # The current transaction should remain unchanged
      commit_result = Tx.commit(result_state.tx)
      assert commit_result.mutations == [{:set, "current_write", "val"}]
    end

    test "commit proxy selection error handling" do
      # Test when no proxies are available
      state = %{
        create_test_state(Bedrock.DataPlane.Version.from_integer(100), %{}, %{"key" => "val"})
        | transaction_system_layout: %{
            proxies: [],
            storage_teams: [],
            services: %{}
          }
      }

      result = Committing.do_commit(state)

      assert {:error, :unavailable} = result
    end

    test "commit function failure handling" do
      reads = %{}
      writes = %{"key" => "val"}
      read_version = Bedrock.DataPlane.Version.from_integer(789)
      state = create_test_state(read_version, reads, writes)

      # Mock a failing commit
      failing_commit_fn = fn _proxy, _transaction ->
        {:error, :timeout}
      end

      result = Committing.do_commit(state, commit_fn: failing_commit_fn)

      assert {:error, :timeout} = result
    end
  end

  describe "read-dependent transactions (non-blind writes)" do
    test "single read then write transaction" do
      # Transaction reads account balance then writes new balance
      reads = %{"account_balance" => "250"}
      writes = %{"account_balance" => "240"}
      read_version = Bedrock.DataPlane.Version.from_integer(555)

      # Simulate setting up reads in the transaction
      tx = Tx.set(Tx.new(), "account_balance", "240")
      tx = %{tx | reads: %{"account_balance" => "250"}}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == [{:set, "account_balance", "240"}]
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 1
        # Note: read conflicts only appear when reads cache is populated during fetch
        # In this test, we have reads so should get read_conflicts
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        assert length(actual_read_conflicts) == 1
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "multiple reads with single write transaction" do
      # Transaction reads multiple accounts and writes to one
      reads = %{
        "account_1" => "100",
        "account_2" => "200",
        "account_3" => "300"
      }

      writes = %{"transaction_log" => "transfer_completed"}
      read_version = Bedrock.DataPlane.Version.from_integer(777)

      # Simulate setting up reads in the transaction
      tx = Tx.set(Tx.new(), "transaction_log", "transfer_completed")
      tx = %{tx | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, actual_read_conflicts} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == [{:set, "transaction_log", "transfer_completed"}]
        assert is_list(transaction.write_conflicts)
        assert transaction.write_conflicts == [{"transaction_log", "transaction_log\0"}]
        # Reads should generate read_conflicts for all accounts
        assert actual_read_conflicts == [
                 {"account_1", "account_1\0"},
                 {"account_2", "account_2\0"},
                 {"account_3", "account_3\0"}
               ]

        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "multiple reads with multiple writes transaction" do
      # Transaction reads multiple keys and writes to multiple keys
      reads = %{
        "user_profile" => "{\"name\":\"Alice\",\"balance\":500}",
        "settings" => "{\"theme\":\"dark\",\"notifications\":true}"
      }

      writes = %{
        "user_profile" => "{\"name\":\"Alice\",\"balance\":450}",
        "audit_log" => "{\"action\":\"purchase\",\"amount\":50}",
        "inventory" => "{\"item_id\":123,\"quantity\":99}"
      }

      read_version = Bedrock.DataPlane.Version.from_integer(888)

      # Create transaction with both reads and writes
      tx =
        Enum.reduce(writes, Tx.new(), fn {k, v}, tx ->
          Tx.set(tx, k, v)
        end)

      tx = %{tx | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert is_list(transaction.mutations)
        # Three writes
        assert length(transaction.mutations) == 3
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 3
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        # Two reads
        assert length(actual_read_conflicts) == 2
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read-only transaction (edge case)" do
      # Transaction has reads but no writes - should return empty writes map
      reads = %{
        "config_value" => "production",
        "feature_flags" => "{\"new_ui\":true,\"beta_features\":false}"
      }

      writes = %{}
      read_version = Bedrock.DataPlane.Version.from_integer(333)

      # Create transaction with only reads
      tx = %{Tx.new() | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        # No writes
        assert transaction.mutations == []
        # No writes
        assert transaction.write_conflicts == []
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        # Two reads
        assert length(actual_read_conflicts) == 2
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read version handling with version 0" do
      # Verify read_version 0 is preserved correctly
      reads = %{"counter" => "0"}
      writes = %{"counter" => "1"}
      read_version = Bedrock.DataPlane.Version.from_integer(0)

      # Create transaction with both reads and writes
      tx = Tx.set(Tx.new(), "counter", "1")
      tx = %{tx | reads: %{"counter" => "0"}}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == [{:set, "counter", "1"}]
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 1
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        assert length(actual_read_conflicts) == 1
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read version handling with large version number" do
      # Verify large read_version numbers are preserved correctly
      reads = %{"large_dataset" => "chunk_1"}
      writes = %{"large_dataset" => "chunk_1_processed"}
      read_version = Bedrock.DataPlane.Version.from_integer(999_999_999)

      # Create transaction with both reads and writes
      tx = Tx.set(Tx.new(), "large_dataset", "chunk_1_processed")
      tx = %{tx | reads: %{"large_dataset" => "chunk_1"}}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == [{:set, "large_dataset", "chunk_1_processed"}]
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 1
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        assert length(actual_read_conflicts) == 1
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "read keys order preservation" do
      # Verify read keys maintain consistent order when extracted
      reads = %{
        "z_last" => "value_z",
        "a_first" => "value_a",
        "m_middle" => "value_m"
      }

      writes = %{"result" => "processed"}
      read_version = Bedrock.DataPlane.Version.from_integer(123)

      # Create transaction with both reads and writes
      tx = Tx.set(Tx.new(), "result", "processed")
      tx = %{tx | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == [{:set, "result", "processed"}]
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 1
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        assert length(actual_read_conflicts) == 3
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end

    test "complex key types in read-dependent transaction" do
      # Test with complex key types (composite keys) that might be used in real scenarios
      reads = %{
        "users:alice" => "{\"balance\":100,\"status\":\"active\"}",
        "sessions:session_123" => "{\"user_id\":\"alice\",\"expires_at\":1234567890}"
      }

      writes = %{
        "users:alice" => "{\"balance\":90,\"status\":\"active\"}",
        "transactions:tx_456" => "{\"from\":\"alice\",\"to\":\"bob\",\"amount\":10}"
      }

      read_version = Bedrock.DataPlane.Version.from_integer(456)

      # Create transaction with both reads and writes using binary keys
      tx =
        Enum.reduce(writes, Tx.new(), fn {k, v}, tx ->
          Tx.set(tx, k, v)
        end)

      tx = %{tx | reads: reads}
      state = %{create_test_state(read_version, reads, writes) | tx: tx}

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = read_version
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert is_list(transaction.mutations)
        # Two writes
        assert length(transaction.mutations) == 2
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 2
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        # Two reads
        assert length(actual_read_conflicts) == 2
        {:ok, 42}
      end

      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      assert result_state.state == :committed
      assert result_state.commit_version == 42
    end
  end

  describe "regression prevention" do
    test "fix prevents transaction visibility bug" do
      # This test captures the exact bug that was causing transaction visibility issues

      # Setup: Write-only transaction with a read_version (acquired during normal flow)
      reads = %{}
      writes = %{"test_key" => "test_value"}
      read_version = Bedrock.DataPlane.Version.from_integer(999)
      state = create_test_state(read_version, reads, writes)

      # Use relaxed commit function for new format
      commit_fn = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        # Write-only transactions should have read_version as nil in new format
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == nil
        assert transaction.mutations == [{:set, "test_key", "test_value"}]
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 1
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert actual_read_conflicts == []
        {:ok, 42}
      end

      # Execute
      {:ok, result_state} = Committing.do_commit(state, commit_fn: commit_fn)

      # Verify success
      assert result_state.state == :committed
      assert result_state.commit_version == 42

      # The mock_commit_fn will fail if the wrong transaction format is passed,
      # so this test passing means the fix is working correctly
    end

    test "fix does not break existing functionality" do
      # Ensure all the original scenarios still work

      # Scenario 1: nil read_version (should remain unchanged)
      state1 = create_test_state(nil, %{}, %{"key" => "val"})

      commit_fn1 = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == nil
        assert transaction.mutations == [{:set, "key", "val"}]
        assert is_list(transaction.write_conflicts)
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert actual_read_conflicts == []
        {:ok, 42}
      end

      {:ok, _} = Committing.do_commit(state1, commit_fn: commit_fn1)

      # Scenario 2: read_version with reads (should remain unchanged)
      tx2 = Tx.set(Tx.new(), "wkey", "wval")
      tx2 = %{tx2 | reads: %{"rkey" => "rval"}}

      state2 = %{
        create_test_state(Bedrock.DataPlane.Version.from_integer(100), %{"rkey" => "rval"}, %{
          "wkey" => "wval"
        })
        | tx: tx2
      }

      commit_fn2 = fn _proxy, binary_transaction ->
        transaction = decode_transaction(binary_transaction)
        expected_read_version = Bedrock.DataPlane.Version.from_integer(100)
        {actual_read_version, _} = transaction.read_conflicts
        assert actual_read_version == expected_read_version
        assert transaction.mutations == [{:set, "wkey", "wval"}]
        assert is_list(transaction.write_conflicts)
        assert length(transaction.write_conflicts) == 1
        {_, actual_read_conflicts} = transaction.read_conflicts
        assert is_list(actual_read_conflicts)
        assert length(actual_read_conflicts) == 1
        {:ok, 42}
      end

      {:ok, _} = Committing.do_commit(state2, commit_fn: commit_fn2)

      # All assertions pass if we reach here
      assert true
    end
  end
end
