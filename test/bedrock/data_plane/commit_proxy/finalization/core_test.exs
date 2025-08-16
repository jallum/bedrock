defmodule Bedrock.DataPlane.CommitProxy.FinalizationCoreTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias FinalizationTestSupport, as: Support

  describe "finalize_batch/2" do
    setup do
      log_server = Support.create_mock_log_server()
      transaction_system_layout = Support.basic_transaction_system_layout(log_server)

      %{transaction_system_layout: transaction_system_layout, log_server: log_server}
    end

    test "exits when resolver is unavailable", %{
      transaction_system_layout: transaction_system_layout
    } do
      batch = Support.create_test_batch(100, 99)

      # Test error case when resolve_transactions fails
      result = Finalization.finalize_batch(batch, transaction_system_layout)

      assert result == {:error, {:resolver_unavailable, :unavailable}}
    end

    test "handles batch with aborted transactions", %{
      transaction_system_layout: transaction_system_layout
    } do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end

      # Create binary transactions
      tx1_map = %{
        mutations: [{:set, <<"key1">>, <<"value1">>}],
        write_conflicts: [{<<"key1">>, <<"key1\0">>}],
        read_conflicts: [],
        read_version: nil
      }

      tx2_map = %{
        mutations: [{:set, <<"key2">>, <<"value2">>}],
        write_conflicts: [{<<"key2">>, <<"key2\0">>}],
        read_conflicts: [],
        read_version: nil
      }

      tx1_binary = BedrockTransaction.encode(tx1_map)
      tx2_binary = BedrockTransaction.encode(tx2_map)

      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(100),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(99),
        n_transactions: 2,
        buffer: [
          # index 0 - will be aborted
          {reply_fn1, tx1_binary},
          # index 1 - success
          {reply_fn2, tx2_binary}
        ]
      }

      # Mock resolver that aborts first transaction
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, [1]}
      end

      # Mock log push function that succeeds
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, _opts ->
        :ok
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )

      assert {:ok, 1, 1} = result

      expected_version = Bedrock.DataPlane.Version.from_integer(100)
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:ok, ^expected_version}}
    end

    test "handles empty batch", %{transaction_system_layout: transaction_system_layout} do
      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(100),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(99),
        n_transactions: 0,
        buffer: []
      }

      # Mock resolver that succeeds with no aborts
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, []}
      end

      # Mock log push function that succeeds (for empty transactions)
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, _opts ->
        :ok
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )

      assert {:ok, 0, 0} = result
    end

    test "handles all transactions aborted", %{
      transaction_system_layout: transaction_system_layout
    } do
      reply_fn1 = fn result -> send(self(), {:reply1, result}) end
      reply_fn2 = fn result -> send(self(), {:reply2, result}) end

      transaction_map1 = %{
        mutations: [{:set, <<"key1">>, <<"value1">>}],
        write_conflicts: [{<<"key1">>, <<"key1\0">>}],
        read_conflicts: [],
        read_version: nil
      }

      transaction_map2 = %{
        mutations: [{:set, <<"key2">>, <<"value2">>}],
        write_conflicts: [{<<"key2">>, <<"key2\0">>}],
        read_conflicts: [],
        read_version: nil
      }

      binary_transaction1 = BedrockTransaction.encode(transaction_map1)
      binary_transaction2 = BedrockTransaction.encode(transaction_map2)

      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(100),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(99),
        n_transactions: 2,
        buffer: [
          {reply_fn1, binary_transaction1},
          {reply_fn2, binary_transaction2}
        ]
      }

      # Mock resolver that aborts all transactions
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        # Abort both transactions
        {:ok, [0, 1]}
      end

      # Mock log push function for empty transactions (all aborted)
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, _opts ->
        :ok
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )

      assert {:ok, 2, 0} = result

      # Both should be aborted
      assert_receive {:reply1, {:error, :aborted}}
      assert_receive {:reply2, {:error, :aborted}}
    end

    test "handles log failure and returns error", %{
      transaction_system_layout: transaction_system_layout
    } do
      batch = Support.create_test_batch(100, 99)

      # Mock resolver that succeeds
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        # No aborts
        {:ok, []}
      end

      # Mock log push function that fails
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, _opts ->
        {:error, {:log_failures, [{"log_1", :timeout}]}}
      end

      # Should return error when logs fail
      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )

      assert result == {:error, {:log_failures, [{"log_1", :timeout}]}}

      # Transaction should be aborted due to log failure
      assert_receive {:reply, {:error, :aborted}}
    end

    test "returns insufficient_acknowledgments error when not all logs respond", %{
      transaction_system_layout: transaction_system_layout
    } do
      batch = Support.create_test_batch(100, 99)

      # Mock resolver that succeeds
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:ok, []}
      end

      # Mock log push function that returns insufficient_acknowledgments
      mock_log_push_fn = fn _layout, _last_version, _tx_by_tag, _commit_version, _opts ->
        {:error, {:insufficient_acknowledgments, 2, 3, [{"log_3", :timeout}]}}
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )

      # Should return the 4-element insufficient_acknowledgments tuple
      assert {:error, {:insufficient_acknowledgments, 2, 3, [{"log_3", :timeout}]}} = result

      # Transaction should be aborted due to insufficient acknowledgments
      assert_receive {:reply, {:error, :aborted}}
    end

    test "passes correct last_commit_version from batch to resolvers and logs", %{
      transaction_system_layout: transaction_system_layout
    } do
      # Create batch with NON-SEQUENTIAL version numbers to test version chain integrity
      # This verifies we use the exact values provided by the sequencer
      commit_version = 150
      # Intentional gap to test proper version chain
      last_commit_version = 142

      batch = Support.create_test_batch(commit_version, last_commit_version)

      # Convert to binary versions for assertions since system uses Bedrock.version() format
      commit_version_binary = Bedrock.DataPlane.Version.from_integer(commit_version)
      last_commit_version_binary = Bedrock.DataPlane.Version.from_integer(last_commit_version)
      test_pid = self()

      # Mock resolver that captures the exact last_version passed to it
      mock_resolver_fn = fn _resolvers,
                            last_version,
                            received_commit_version,
                            _summaries,
                            _opts ->
        send(test_pid, {:resolver_called, last_version, received_commit_version})
        # No aborts
        {:ok, []}
      end

      # Mock log push function that captures the exact last_version passed to it
      mock_log_push_fn = fn _layout, last_version, _tx_by_tag, received_commit_version, _opts ->
        send(test_pid, {:log_push_called, last_version, received_commit_version})
        :ok
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          batch_log_push_fn: mock_log_push_fn
        )

      assert {:ok, 0, 1} = result

      # Verify resolver received the exact last_commit_version from batch
      assert_receive {:resolver_called, ^last_commit_version_binary, ^commit_version_binary}

      # Verify log push received the exact last_commit_version from batch
      assert_receive {:log_push_called, ^last_commit_version_binary, ^commit_version_binary}

      assert_receive {:reply, {:ok, ^commit_version_binary}}
    end
  end

  describe "finalize_batch/3 with dependency injection" do
    test "uses injected abort_reply_fn" do
      transaction_system_layout = %{
        sequencer: :test_sequencer,
        resolvers: [{<<0>>, :test_resolver}],
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}},
        storage_teams: [%{tag: 0, key_range: {<<>>, :end}, storage_ids: ["storage_1"]}]
      }

      reply_fn = fn result -> send(self(), {:reply, result}) end

      transaction_map = %{
        mutations: [{:set, <<"key">>, <<"value">>}],
        write_conflicts: [{<<"key">>, <<"key\0">>}],
        read_conflicts: [],
        read_version: nil
      }

      binary_transaction = BedrockTransaction.encode(transaction_map)

      batch = %Bedrock.DataPlane.CommitProxy.Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(100),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(99),
        n_transactions: 1,
        buffer: [
          {reply_fn, binary_transaction}
        ]
      }

      # Track calls to custom abort function
      test_pid = self()

      custom_abort_fn = fn reply_fns ->
        send(test_pid, {:custom_abort_called, length(reply_fns)})
        Enum.each(reply_fns, & &1.({:error, :custom_abort}))
      end

      # Mock resolver that fails
      mock_resolver_fn = fn _resolvers, _last_version, _commit_version, _summaries, _opts ->
        {:error, :timeout}
      end

      result =
        Finalization.finalize_batch(
          batch,
          transaction_system_layout,
          resolver_fn: mock_resolver_fn,
          abort_reply_fn: custom_abort_fn
        )

      assert {:error, :timeout} = result
      assert_receive {:custom_abort_called, 1}
      assert_receive {:reply, {:error, :custom_abort}}
    end
  end
end
