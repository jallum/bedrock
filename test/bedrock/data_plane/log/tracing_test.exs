defmodule Bedrock.DataPlane.Log.TracingTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Log.Tracing
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule MockCluster do
    def name, do: "test_cluster"
  end

  describe "telemetry handlers" do
    test "start/0 attaches telemetry handlers" do
      # Stop any existing handler first
      Tracing.stop()

      # Start should succeed
      assert :ok = Tracing.start()

      # Starting again should return already_exists
      assert {:error, :already_exists} = Tracing.start()

      # Clean up
      Tracing.stop()
    end

    test "stop/0 detaches telemetry handlers" do
      # Start handler first
      Tracing.start()

      # Stop should succeed
      assert :ok = Tracing.stop()

      # Stopping again should return not_found
      assert {:error, :not_found} = Tracing.stop()
    end
  end

  describe "log event handling" do
    setup do
      # Set up logger metadata for tracing
      Logger.metadata(
        cluster: MockCluster,
        id: "test_log_1"
      )

      {:ok, cluster: MockCluster}
    end

    test "handles :started event", %{cluster: cluster} do
      measurements = %{}

      metadata = %{
        cluster: cluster,
        id: "test_log_1",
        otp_name: :test_log_server
      }

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :started], measurements, metadata, nil)
        end)

      assert log =~ "Started log service: test_log_server"
      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end

    test "handles :lock_for_recovery event" do
      measurements = %{}
      metadata = %{epoch: 5}

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :lock_for_recovery], measurements, metadata, nil)
        end)

      assert log =~ "Lock for recovery in epoch 5"
      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end

    test "handles :recover_from event with no source" do
      measurements = %{}
      metadata = %{source_log: :none}

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :recover_from], measurements, metadata, nil)
        end)

      assert log =~ "Reset to initial version"
      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end

    test "handles :recover_from event with source log" do
      measurements = %{}

      metadata = %{
        source_log: :log_server_2,
        first_version: Version.from_integer(100),
        last_version: Version.from_integer(150)
      }

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :recover_from], measurements, metadata, nil)
        end)

      assert log =~
               "Recover from :log_server_2 with versions <0,0,0,0,0,0,0,100> to <0,0,0,0,0,0,0,150>"

      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end

    test "handles :push event" do
      version = Version.from_integer(200)

      encoded_transaction =
        BedrockTransactionTestSupport.new_log_transaction(
          version,
          %{"key1" => "value1", "key2" => "value2", "key3" => "value3"}
        )

      measurements = %{}
      metadata = %{transaction: encoded_transaction}

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :push], measurements, metadata, nil)
        end)

      assert log =~ "Push transaction (3 keys) with expected version <0,0,0,0,0,0,0,200>"
      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end

    test "handles :push_out_of_order event" do
      measurements = %{}

      metadata = %{
        expected_version: Version.from_integer(205),
        current_version: Version.from_integer(200)
      }

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :push_out_of_order], measurements, metadata, nil)
        end)

      assert log =~
               "Rejected out-of-order transaction: expected <0,0,0,0,0,0,0,205>, current <0,0,0,0,0,0,0,200>"

      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end

    test "handles :pull event" do
      measurements = %{}

      metadata = %{
        from_version: Version.from_integer(100),
        opts: [timeout: 5000]
      }

      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, :pull], measurements, metadata, nil)
        end)

      assert log =~
               "Pull transactions from version <0,0,0,0,0,0,0,100> with options [timeout: 5000]"

      assert log =~ "Bedrock [test_cluster/test_log_1]"
    end
  end
end
