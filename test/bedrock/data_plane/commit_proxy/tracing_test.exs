defmodule Bedrock.DataPlane.CommitProxy.TracingTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.CommitProxy.Tracing
  alias Bedrock.DataPlane.Version

  # Define a mock cluster for testing
  defmodule TestCluster do
    @moduledoc false
    def name, do: :test_cluster
  end

  # Common setup for all tests
  setup do
    # Set up logger metadata to avoid KeyError
    Logger.metadata(cluster: TestCluster)
    :ok
  end

  describe "trace/3" do
    test "logs transaction batch start with count and version" do
      measurements = %{n_transactions: 5}
      metadata = %{cluster: TestCluster, commit_version: Version.from_integer(123)}

      log_output =
        capture_log(fn ->
          Tracing.trace(:start, measurements, metadata)
        end)

      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,123> started with 5 transactions"
    end

    test "logs transaction batch completion with abort/ok counts and duration" do
      measurements = %{n_aborts: 2, n_oks: 8, duration_μs: 1500}
      metadata = %{commit_version: Version.from_integer(124)}

      log_output =
        capture_log(fn ->
          Tracing.trace(:stop, measurements, metadata)
        end)

      # The function rounds microseconds and includes completion info
      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,124> completed with 2 aborts and 8 oks"
      assert log_output =~ "1ms"
    end

    test "logs transaction batch failure when commit_version is in measurements" do
      # This test verifies the fix for the function_clause error
      # The original bug was that :failed expected commit_version in metadata
      # but it was actually passed in measurements
      measurements = %{
        duration_μs: 437,
        commit_version: Version.from_integer(1),
        n_transactions: 1
      }

      metadata = %{
        pid: self(),
        reason: {:log_failures, [{"byj7vnbf", :tx_out_of_order}]},
        cluster: Example.Cluster
      }

      # This should not raise a function_clause error
      log_output =
        capture_log(fn ->
          Tracing.trace(:failed, measurements, metadata)
        end)

      # Should contain all expected failure information
      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,1> failed"
      assert log_output =~ "{:log_failures, [{\"byj7vnbf\", :tx_out_of_order}]}"
      assert log_output =~ "437μs"
    end

    test "traces failed event with timeout reason" do
      measurements = %{duration_μs: 1000, commit_version: Version.from_integer(42)}
      metadata = %{reason: :timeout, cluster: TestCluster}

      log_output = capture_log(fn -> Tracing.trace(:failed, measurements, metadata) end)

      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,42> failed"
      assert log_output =~ "timeout"
      assert log_output =~ "1ms"
    end

    test "traces failed event with log failures reason" do
      measurements = %{duration_μs: 1000, commit_version: Version.from_integer(42)}
      metadata = %{reason: :log_failures, cluster: TestCluster}

      log_output = capture_log(fn -> Tracing.trace(:failed, measurements, metadata) end)

      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,42> failed"
      assert log_output =~ "log_failures"
      assert log_output =~ "1ms"
    end

    test "traces failed event with storage failures reason" do
      measurements = %{duration_μs: 1000, commit_version: Version.from_integer(42)}
      metadata = %{reason: :storage_failures, cluster: TestCluster}

      log_output = capture_log(fn -> Tracing.trace(:failed, measurements, metadata) end)

      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,42> failed"
      assert log_output =~ "storage_failures"
      assert log_output =~ "1ms"
    end
  end

  describe "handler/4" do
    test "handles telemetry events without crashing on failed transactions" do
      # This test ensures the telemetry handler correctly passes through
      # the measurements and metadata structure that caused the original bug
      event_name = [:bedrock, :data_plane, :commit_proxy, :failed]

      measurements = %{
        duration_μs: 500,
        commit_version: Version.from_integer(99),
        n_transactions: 3
      }

      metadata = %{
        pid: self(),
        reason: {:storage_failures, [{"storage1", :locked}]},
        cluster: TestCluster
      }

      # Should not crash with function_clause error
      log_output =
        capture_log(fn ->
          Tracing.handler(event_name, measurements, metadata, nil)
        end)

      assert log_output =~ "Transaction Batch <0,0,0,0,0,0,0,99> failed"
      assert log_output =~ "storage_failures"
    end
  end

  describe "telemetry integration" do
    test "attaches and detaches telemetry handlers successfully" do
      # Test that the telemetry handlers can be attached and detached
      assert :ok = Tracing.start()
      assert :ok = Tracing.stop()
    end
  end
end
