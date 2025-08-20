defmodule Bedrock.DataPlane.Storage.Olivine.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Server
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager

  describe "module structure" do
    test "all modules compile and load successfully" do
      assert Code.ensure_loaded(Server) == {:module, Server}
      assert Code.ensure_loaded(Database) == {:module, Database}
      assert Code.ensure_loaded(VersionManager) == {:module, VersionManager}
    end

    test "basic DETS operations work" do
      temp_path = "/tmp/test_olivine_#{System.unique_integer([:positive])}.dets"

      assert {:ok, db} = Database.open(:test_olivine, temp_path)
      assert :ok = Database.close(db)

      File.rm(temp_path)
    end

    test "VersionManager can be created and closed" do
      vm = VersionManager.new()
      assert %VersionManager{} = vm
      assert vm.lookaside_buffer
      assert :ok = VersionManager.close(vm)
    end

    test "basic telemetry events can be emitted" do
      alias Bedrock.DataPlane.Storage.Olivine.Telemetry

      assert :ok = Telemetry.trace_fetch_start("test_key", <<1::64>>)
      assert :ok = Telemetry.trace_transaction_applied(<<1::64>>, 1)
      assert :ok = Telemetry.trace_persistent_error(:test_error)
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

      assert %{id: {Server, "test_id"}} = spec
      assert {GenServer, :start_link, _args} = spec.start
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
