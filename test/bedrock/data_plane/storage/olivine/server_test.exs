defmodule Bedrock.DataPlane.Storage.Olivine.ServerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Server

  describe "module structure" do
    test "all modules compile and load successfully" do
      modules = [Server, Database, IndexManager]

      for module <- modules do
        assert {:module, ^module} = Code.ensure_loaded(module)
      end
    end

    test "basic DETS operations work" do
      temp_path = "/tmp/test_olivine_#{System.unique_integer([:positive])}.dets"

      {result, _logs} = with_log(fn -> Database.open(:test_olivine, temp_path, pool_size: 1) end)
      assert {:ok, db} = result
      assert :ok = Database.close(db)

      File.rm(temp_path)
    end

    test "IndexManager can be created" do
      vm = IndexManager.new()
      assert %IndexManager{} = vm
    end

    test "basic telemetry events can be emitted" do
      alias Bedrock.DataPlane.Storage.Telemetry

      # Set metadata first for proper telemetry context
      Telemetry.trace_metadata(%{otp_name: :test_server, storage_id: "test_id"})
      assert :ok = Telemetry.trace_startup_start()
      assert :ok = Telemetry.trace_startup_complete()
      assert :ok = Telemetry.trace_log_pull_start(<<1::64>>, <<2::64>>)

      # Set telemetry metadata
      Telemetry.trace_metadata(%{otp_name: :test_server})

      # Test general storage telemetry events (shared)
      assert :ok = Telemetry.trace_read_operation_complete(:get, "test_key", total_duration_μs: 1000)

      # Test olivine-specific telemetry events
      assert :ok = Telemetry.trace_transactions_queued(5, 10)
      assert :ok = Telemetry.trace_transaction_processing_complete(3, 1500)
    end
  end

  describe "GenServer child_spec" do
    test "child_spec returns valid supervisor spec" do
      spec =
        Server.child_spec(
          otp_name: :test_olivine,
          foreman: self(),
          id: "test_id",
          path: "/tmp/test_olivine"
        )

      assert %{
               id: {Server, "test_id"},
               start: {GenServer, :start_link, _args}
             } = spec
    end

    test "child_spec raises when required options are missing" do
      assert_raise RuntimeError, "Missing :otp_name option", fn ->
        Server.child_spec(foreman: self(), id: "test", path: "/tmp")
      end

      assert_raise RuntimeError, "Missing :foreman option", fn ->
        Server.child_spec(otp_name: :test, id: "test", path: "/tmp")
      end
    end
  end
end
