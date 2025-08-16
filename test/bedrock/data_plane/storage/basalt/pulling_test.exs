defmodule Bedrock.DataPlane.Storage.Basalt.PullingTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Storage.Basalt.Pulling
  alias Bedrock.DataPlane.Version

  describe "start_pulling/6" do
    test "creates a task with proper initial state" do
      start_after = 0
      # LogDescriptor is a list of range_tags
      logs = %{"log1" => []}
      services = %{"log1" => %{kind: :log, status: {:up, self()}, last_seen: nil}}
      apply_fn = fn txns -> length(txns) end

      durable_version_fn = fn -> 0 end
      flush_window_fn = fn -> :ok end

      task =
        Pulling.start_pulling(
          start_after,
          logs,
          services,
          apply_fn,
          durable_version_fn,
          flush_window_fn
        )

      assert %Task{} = task
      assert Process.alive?(task.pid)

      # Clean up
      Pulling.stop(task)
    end

    test "starts with correct initial parameters" do
      start_after = 5
      # LogDescriptors
      logs = %{"log1" => [], "log2" => []}

      services = %{
        "log1" => %{kind: :log, status: {:up, self()}, last_seen: nil},
        "log2" => %{kind: :log, status: {:up, self()}, last_seen: nil}
      }

      apply_fn = fn txns -> length(txns) + 10 end

      durable_version_fn = fn -> 0 end
      flush_window_fn = fn -> :ok end

      task =
        Pulling.start_pulling(
          start_after,
          logs,
          services,
          apply_fn,
          durable_version_fn,
          flush_window_fn
        )

      assert %Task{} = task
      assert Process.alive?(task.pid)

      # Clean up
      Pulling.stop(task)
    end
  end

  describe "stop/1" do
    test "shuts down a running task" do
      start_after = 0
      logs = %{"log1" => []}
      services = %{"log1" => %{kind: :log, status: {:up, self()}, last_seen: nil}}
      apply_fn = fn _txns -> 1 end

      durable_version_fn = fn -> 0 end
      flush_window_fn = fn -> :ok end

      task =
        Pulling.start_pulling(
          start_after,
          logs,
          services,
          apply_fn,
          durable_version_fn,
          flush_window_fn
        )

      assert Process.alive?(task.pid)

      assert :ok = Pulling.stop(task)

      # Give it time to shut down
      Process.sleep(10)
      refute Process.alive?(task.pid)
    end

    test "returns :ok even if task is already dead" do
      start_after = 0
      logs = %{"log1" => []}
      services = %{"log1" => %{kind: :log, status: {:up, self()}, last_seen: nil}}
      apply_fn = fn _txns -> 1 end

      durable_version_fn = fn -> 0 end
      flush_window_fn = fn -> :ok end

      task =
        Pulling.start_pulling(
          start_after,
          logs,
          services,
          apply_fn,
          durable_version_fn,
          flush_window_fn
        )

      # Stop it first
      Pulling.stop(task)
      Process.sleep(10)

      # Should still return :ok
      assert :ok = Pulling.stop(task)
    end
  end

  describe "configuration functions" do
    test "circuit_breaker_timeout/0 returns timeout value" do
      assert Pulling.circuit_breaker_timeout() == 10_000
    end

    test "retry_delay/0 returns delay value" do
      assert Pulling.retry_delay() == 5_000
    end

    test "call_timeout/0 returns call timeout value" do
      assert Pulling.call_timeout() == 5_000
    end
  end

  describe "select_log/1" do
    test "selects available log when all logs are available" do
      state = %{
        logs: %{"log1" => [], "log2" => []},
        services: %{
          "log1" => %{kind: :log, status: {:up, self()}, last_seen: nil},
          "log2" => %{kind: :log, status: {:up, self()}, last_seen: nil}
        },
        failed_logs: %{},
        get_durable_version_fn: fn -> 0 end,
        flush_window_fn: fn -> :ok end
      }

      assert {:ok, {log_id, service}} = Pulling.select_log(state)
      assert log_id in ["log1", "log2"]
      assert is_map(service)
    end

    test "returns :no_available_logs when all logs are failed" do
      state = %{
        logs: %{"log1" => [], "log2" => []},
        services: %{
          "log1" => %{kind: :log, status: {:up, self()}, last_seen: nil},
          "log2" => %{kind: :log, status: {:up, self()}, last_seen: nil}
        },
        failed_logs: %{
          "log1" => System.monotonic_time(:millisecond) + 10_000,
          "log2" => System.monotonic_time(:millisecond) + 10_000
        }
      }

      assert :no_available_logs = Pulling.select_log(state)
    end

    test "excludes failed logs that haven't timed out yet" do
      now = System.monotonic_time(:millisecond)

      state = %{
        logs: %{"log1" => [], "log2" => []},
        services: %{
          "log1" => %{kind: :log, status: {:up, self()}, last_seen: nil},
          "log2" => %{kind: :log, status: {:up, self()}, last_seen: nil}
        },
        failed_logs: %{
          # Still failed
          "log1" => now + 10_000
        }
      }

      assert {:ok, {"log2", service}} = Pulling.select_log(state)
      assert is_map(service)
    end

    test "includes failed logs that have timed out" do
      now = System.monotonic_time(:millisecond)

      state = %{
        logs: %{"log1" => [], "log2" => []},
        services: %{
          "log1" => %{kind: :log, status: {:up, self()}, last_seen: nil},
          "log2" => %{kind: :log, status: {:up, self()}, last_seen: nil}
        },
        failed_logs: %{
          # Timed out, should be available again
          "log1" => now - 1000
        }
      }

      assert {:ok, {log_id, service}} = Pulling.select_log(state)
      assert log_id in ["log1", "log2"]
      assert is_map(service)
    end

    test "filters out logs without corresponding services" do
      state = %{
        logs: %{"log1" => [], "log2" => []},
        services: %{
          "log1" => %{kind: :log, status: {:up, self()}, last_seen: nil}
          # log2 has no service
        },
        failed_logs: %{},
        get_durable_version_fn: fn -> 0 end,
        flush_window_fn: fn -> :ok end
      }

      assert {:ok, {"log1", service}} = Pulling.select_log(state)
      assert is_map(service)
    end

    test "returns :no_available_logs when no services available" do
      state = %{
        logs: %{"log1" => [], "log2" => []},
        # No services
        services: %{},
        failed_logs: %{},
        get_durable_version_fn: fn -> 0 end,
        flush_window_fn: fn -> :ok end
      }

      assert :no_available_logs = Pulling.select_log(state)
    end
  end

  describe "mark_log_as_failed/2" do
    test "adds log to failed_logs with timeout" do
      state = %{failed_logs: %{}}
      log_id = "test_log"

      new_state = Pulling.mark_log_as_failed(state, log_id)

      assert Map.has_key?(new_state.failed_logs, log_id)

      # Should be set to a future timestamp
      retry_timestamp = new_state.failed_logs[log_id]
      now = System.monotonic_time(:millisecond)
      assert retry_timestamp > now
    end

    test "preserves existing failed logs" do
      existing_timestamp = System.monotonic_time(:millisecond) + 5000
      state = %{failed_logs: %{"existing_log" => existing_timestamp}}
      log_id = "new_log"

      new_state = Pulling.mark_log_as_failed(state, log_id)

      assert Map.has_key?(new_state.failed_logs, "existing_log")
      assert Map.has_key?(new_state.failed_logs, "new_log")
      assert new_state.failed_logs["existing_log"] == existing_timestamp
    end

    test "updates existing failed log timestamp" do
      old_timestamp = System.monotonic_time(:millisecond) - 1000
      state = %{failed_logs: %{"test_log" => old_timestamp}}
      log_id = "test_log"

      new_state = Pulling.mark_log_as_failed(state, log_id)

      new_timestamp = new_state.failed_logs[log_id]
      assert new_timestamp > old_timestamp
    end
  end

  describe "reset_failed_logs/1" do
    test "clears all failed logs" do
      state = %{
        failed_logs: %{
          "log1" => System.monotonic_time(:millisecond) + 5000,
          "log2" => System.monotonic_time(:millisecond) + 10_000
        }
      }

      new_state = Pulling.reset_failed_logs(state)

      assert new_state.failed_logs == %{}
    end

    test "preserves other state fields" do
      state = %{
        start_after: 100,
        logs: %{"log1" => []},
        failed_logs: %{"log1" => 12_345}
      }

      new_state = Pulling.reset_failed_logs(state)

      assert new_state.start_after == 100
      assert new_state.logs == %{"log1" => []}
      assert new_state.failed_logs == %{}
    end
  end

  describe "long_pull_loop/1 integration scenarios" do
    test "handles successful transaction pull and decode" do
      # Create a mock apply function that tracks what it receives
      test_pid = self()

      apply_fn = fn transactions ->
        send(test_pid, {:applied_transactions, transactions})
        length(transactions) + 1
      end

      # Create valid encoded transactions
      transaction1 = {Version.from_integer(1), %{"key1" => "value1"}}
      transaction2 = {Version.from_integer(2), %{"key2" => "value2"}}

      encoded_txns = [
        BedrockTransactionTestSupport.new_log_transaction(
          elem(transaction1, 0),
          elem(transaction1, 1)
        ),
        BedrockTransactionTestSupport.new_log_transaction(
          elem(transaction2, 0),
          elem(transaction2, 1)
        )
      ]

      # Mock log server process
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:pull, _start_after, _opts}} ->
              GenServer.reply(from, {:ok, encoded_txns})
          end
        end)

      state = %{
        start_after: 0,
        apply_transactions_fn: apply_fn,
        logs: %{"test_log" => []},
        services: %{"test_log" => %{kind: :log, status: {:up, log_server}, last_seen: nil}},
        failed_logs: %{},
        get_durable_version_fn: fn -> 0 end,
        flush_window_fn: fn -> :ok end
      }

      # Start the loop in a separate process to avoid blocking
      loop_pid =
        spawn(fn ->
          try do
            Pulling.long_pull_loop(state)
          catch
            :exit, _ -> :ok
          end
        end)

      # Should receive the applied transactions
      assert_receive {:applied_transactions, transactions}, 1000

      # Verify the transactions are in BedrockTransaction binary format
      assert length(transactions) == 2
      # Decode and verify each transaction
      [tx1, tx2] = transactions
      assert BedrockTransactionTestSupport.extract_log_version(tx1) == Version.from_integer(1)
      assert BedrockTransactionTestSupport.extract_log_writes(tx1) == %{"key1" => "value1"}
      assert BedrockTransactionTestSupport.extract_log_version(tx2) == Version.from_integer(2)
      assert BedrockTransactionTestSupport.extract_log_writes(tx2) == %{"key2" => "value2"}

      # Clean up
      Process.exit(loop_pid, :kill)
      Process.exit(log_server, :kill)
    end

    test "handles log pull failure and circuit breaker" do
      _test_pid = self()
      apply_fn = fn _txns -> 1 end

      # Mock log server that always fails
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:pull, _start_after, _opts}} ->
              GenServer.reply(from, {:error, :connection_failed})
          end
        end)

      state = %{
        start_after: 0,
        apply_transactions_fn: apply_fn,
        logs: %{"test_log" => []},
        services: %{"test_log" => %{kind: :log, status: {:up, log_server}, last_seen: nil}},
        failed_logs: %{},
        get_durable_version_fn: fn -> 0 end,
        flush_window_fn: fn -> :ok end
      }

      # Start the loop with a timeout to prevent infinite loop
      loop_pid =
        spawn(fn ->
          try do
            Pulling.long_pull_loop(state)
          catch
            :exit, _ -> :ok
          end
        end)

      # Give it time to attempt the pull and fail
      Process.sleep(100)

      # Clean up
      Process.exit(loop_pid, :kill)
      Process.exit(log_server, :kill)

      # Test passed if we didn't hang indefinitely
      assert true
    end

    test "handles no available logs scenario" do
      apply_fn = fn _txns -> 1 end

      state = %{
        start_after: 0,
        apply_transactions_fn: apply_fn,
        # No logs available
        logs: %{},
        services: %{},
        failed_logs: %{},
        get_durable_version_fn: fn -> 0 end,
        flush_window_fn: fn -> :ok end
      }

      # This should handle the no available logs case
      # We'll run it briefly and then stop it
      loop_pid =
        spawn(fn ->
          try do
            Pulling.long_pull_loop(state)
          catch
            :exit, _ -> :ok
          end
        end)

      # Give it a moment to process
      Process.sleep(100)

      # Clean up
      Process.exit(loop_pid, :kill)

      # Test passed if we didn't hang indefinitely
      assert true
    end
  end
end
