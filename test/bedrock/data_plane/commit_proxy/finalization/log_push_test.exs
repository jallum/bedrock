defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogPushTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Version
  alias FinalizationTestSupport, as: Support

  describe "push_transaction_to_logs" do
    test "uses custom timeout option" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key">> => <<"value">>
          })
      }

      # Mock that tracks timeout usage
      test_pid = self()

      mock_async_stream_fn = fn _logs, _fun, opts ->
        timeout = Keyword.get(opts, :timeout, :default)
        send(test_pid, {:timeout_used, timeout})
        # Return successful result
        [ok: {"log_1", :ok}]
      end

      Finalization.push_transaction_to_logs(
        transaction_system_layout,
        Version.from_integer(99),
        transactions_by_tag,
        Version.from_integer(100),
        async_stream_fn: mock_async_stream_fn,
        # Custom timeout
        timeout: 2500
      )

      assert_receive {:timeout_used, 2500}
    end

    test "uses custom log_push_fn with success and failure scenarios" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key">> => <<"value">>
          })
      }

      test_pid = self()

      # Test success case
      custom_log_push_fn_success = fn service_descriptor, encoded_transaction, last_version ->
        send(
          test_pid,
          {:custom_push_called, service_descriptor, encoded_transaction, last_version}
        )

        :ok
      end

      result_success =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100),
          log_push_fn: custom_log_push_fn_success
        )

      assert result_success == :ok
      version_99 = Version.from_integer(99)
      assert_receive {:custom_push_called, _service_descriptor, _encoded_transaction, ^version_99}

      # Test failure case
      custom_log_push_fn_failure = fn _service_descriptor, _encoded_transaction, _last_version ->
        {:error, :custom_failure}
      end

      result_failure =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100),
          log_push_fn: custom_log_push_fn_failure
        )

      assert {:error, {:log_failures, [{"log_1", :custom_failure}]}} = result_failure
    end

    test "aborts immediately on first log failure" do
      transaction_system_layout = %{
        logs: %{
          "log_1" => [0],
          "log_2" => [1],
          "log_3" => [2]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, self()}},
          "log_2" => %{kind: :log, status: {:up, self()}},
          "log_3" => %{kind: :log, status: {:up, self()}}
        }
      }

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key1">> => <<"value1">>
          })
      }

      # Mock that returns first log failure, others would succeed
      mock_async_stream_fn =
        Support.mock_async_stream_with_responses(%{
          "log_1" => {:error, :first_failure},
          "log_2" => :ok,
          "log_3" => :ok
        })

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100),
          async_stream_fn: mock_async_stream_fn
        )

      # Should fail immediately on first error
      assert {:error, {:log_failures, [{"log_1", :first_failure}]}} = result
    end

    test "pushes transactions to single log successfully" do
      log_server = Support.create_mock_log_server()

      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, log_server}}}
      }

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key">> => <<"value">>
          })
      }

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100)
        )

      assert result == :ok
    end

    test "handles empty transactions_by_tag (all aborted)" do
      log_server = Support.create_mock_log_server()

      transaction_system_layout = %{
        logs: %{"log_1" => [0, 1]},
        services: %{"log_1" => %{kind: :log, status: {:up, log_server}}}
      }

      # Empty transactions_by_tag - all transactions were aborted
      transactions_by_tag = %{}

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100)
        )

      assert result == :ok
    end

    test "handles multiple logs requiring ALL to succeed and failures" do
      transaction_system_layout = Support.multi_log_transaction_system_layout()

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key1">> => <<"value1">>
          }),
        1 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key2">> => <<"value2">>
          })
      }

      # Mock async stream that simulates 2/3 success (but we need ALL now)
      mock_async_stream_fn =
        Support.mock_async_stream_with_responses(%{
          "log_1" => :ok,
          "log_2" => :ok,
          # One failure
          "log_3" => {:error, :unavailable}
        })

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100),
          async_stream_fn: mock_async_stream_fn
        )

      # Should fail because we need ALL logs now, not just majority
      assert {:error, {:log_failures, [{"log_3", :unavailable}]}} = result
    end

    test "verifies exact last_commit_version is passed to log_push_fn" do
      transaction_system_layout = %{
        logs: %{"log_1" => [0]},
        services: %{"log_1" => %{kind: :log, status: {:up, self()}}}
      }

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(175), %{
            <<"key">> => <<"value">>
          })
      }

      # Use NON-SEQUENTIAL version numbers to verify proper version chain handling
      commit_version = Version.from_integer(175)
      # Intentional gap to verify sequencer values are used
      last_commit_version = Version.from_integer(168)

      test_pid = self()

      # Custom log_push_fn that captures the exact last_version parameter
      custom_log_push_fn = fn service_descriptor, encoded_transaction, received_last_version ->
        send(
          test_pid,
          {:log_push_version_check, received_last_version, service_descriptor,
           encoded_transaction}
        )

        :ok
      end

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          last_commit_version,
          transactions_by_tag,
          commit_version,
          log_push_fn: custom_log_push_fn
        )

      assert result == :ok

      # Verify the log_push_fn received the exact last_commit_version from sequencer
      assert_receive {:log_push_version_check, ^last_commit_version, _service_descriptor,
                      _encoded_transaction}
    end

    test "returns insufficient_acknowledgments when async stream doesn't yield all required responses" do
      transaction_system_layout = %{
        logs: %{
          "log_1" => [0],
          "log_2" => [1],
          "log_3" => [2]
        },
        services: %{
          "log_1" => %{kind: :log, status: {:up, self()}},
          "log_2" => %{kind: :log, status: {:up, self()}},
          "log_3" => %{kind: :log, status: {:up, self()}}
        }
      }

      transactions_by_tag = %{
        0 =>
          BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(100), %{
            <<"key1">> => <<"value1">>
          })
      }

      # Mock async stream that only yields 2 out of 3 required responses
      # This simulates a timeout scenario where some logs don't respond
      mock_async_stream_fn = fn _logs, _fun, _opts ->
        [
          {:ok, {"log_1", :ok}},
          {:ok, {"log_2", :ok}}
          # log_3 doesn't respond (simulating timeout/hang)
        ]
      end

      result =
        Finalization.push_transaction_to_logs(
          transaction_system_layout,
          Version.from_integer(99),
          transactions_by_tag,
          Version.from_integer(100),
          async_stream_fn: mock_async_stream_fn
        )

      # Should return insufficient_acknowledgments with 4-element tuple
      assert {:error, {:insufficient_acknowledgments, 2, 3, []}} = result
    end
  end
end
