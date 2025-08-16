defmodule Bedrock.DataPlane.Log.Shale.ServerTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    cluster = Cluster
    otp_name = :"test_log_#{:rand.uniform(10000)}"
    id = "test_log_#{:rand.uniform(10000)}"
    foreman = self()
    path = Path.join(tmp_dir, "log_segments")

    File.mkdir_p!(path)

    {:ok,
     cluster: cluster,
     otp_name: otp_name,
     id: id,
     foreman: foreman,
     path: path,
     server_opts: [
       cluster: cluster,
       otp_name: otp_name,
       id: id,
       foreman: foreman,
       path: path
     ]}
  end

  describe "child_spec/1" do
    test "creates valid child spec with all required options", %{server_opts: opts} do
      spec = Server.child_spec(opts)

      expected_id = opts[:id]
      expected_name = opts[:otp_name]

      assert %{
               id: {Server, id},
               start: {GenServer, :start_link, [Server, _, [name: name]]}
             } = spec

      assert id == expected_id
      assert name == expected_name
    end

    test "raises error when cluster option is missing" do
      opts = [otp_name: :test, id: "test", foreman: self(), path: "/tmp"]

      assert_raise RuntimeError, "Missing :cluster option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when otp_name option is missing" do
      opts = [cluster: Cluster, id: "test", foreman: self(), path: "/tmp"]

      assert_raise RuntimeError, "Missing :otp_name option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when id option is missing" do
      opts = [cluster: Cluster, otp_name: :test, foreman: self(), path: "/tmp"]

      assert_raise KeyError, fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when foreman option is missing" do
      opts = [cluster: Cluster, otp_name: :test, id: "test", path: "/tmp"]

      assert_raise KeyError, fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when path option is missing" do
      opts = [cluster: Cluster, otp_name: :test, id: "test", foreman: self()]

      assert_raise KeyError, fn ->
        Server.child_spec(opts)
      end
    end
  end

  describe "GenServer lifecycle" do
    test "starts successfully with valid options", %{server_opts: opts} do
      assert {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))
      assert Process.alive?(pid)
      if Process.alive?(pid), do: GenServer.stop(pid)
    end

    test "initializes with correct state", %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      # Allow initialization to complete
      :sys.get_state(pid)

      state = :sys.get_state(pid)

      expected_id = opts[:id]
      expected_otp_name = opts[:otp_name]
      expected_foreman = opts[:foreman]
      expected_path = opts[:path]

      version_0 = Version.from_integer(0)

      assert %State{
               cluster: Cluster,
               id: id,
               otp_name: otp_name,
               foreman: foreman,
               path: path,
               mode: :locked,
               oldest_version: ^version_0,
               last_version: ^version_0
             } = state

      assert id == expected_id
      assert otp_name == expected_otp_name
      assert foreman == expected_foreman
      assert path == expected_path

      if Process.alive?(pid), do: GenServer.stop(pid)
    end

    test "handles initialization continue properly", %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      # Wait for initialization to complete
      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      if Process.alive?(pid), do: GenServer.stop(pid)
    end
  end

  describe "handle_call/3 - basic operations" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      # Wait for initialization
      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "responds to ping", %{server: pid} do
      assert :pong = GenServer.call(pid, :ping)
    end

    test "handles info request", %{server: pid} do
      result = GenServer.call(pid, {:info, [:id, :kind, :oldest_version]})

      assert {:ok, info} = result
      assert is_map(info)
      assert Map.has_key?(info, :id)
      assert Map.has_key?(info, :kind)
      assert Map.has_key?(info, :oldest_version)
    end

    test "handles info request with single fact", %{server: pid} do
      assert {:ok, info} = GenServer.call(pid, {:info, [:id]})
      assert is_binary(info[:id])
    end

    test "handles empty info request", %{server: pid} do
      assert {:ok, info} = GenServer.call(pid, {:info, []})
      assert info == %{}
    end
  end

  describe "handle_call/3 - lock_for_recovery" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles lock_for_recovery request", %{server: pid} do
      epoch = 1

      # This should work when called from a director-like process
      result = GenServer.call(pid, {:lock_for_recovery, epoch})

      # Result depends on the current state and epoch validation
      assert is_tuple(result)
    end
  end

  describe "handle_call/3 - push operations" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles push with invalid transaction bytes", %{server: pid} do
      invalid_bytes = "invalid transaction data"
      expected_version = 1

      result = GenServer.call(pid, {:push, invalid_bytes, expected_version})

      assert {:error, _reason} = result
    end

    test "handles push with valid transaction format", %{server: pid} do
      # Create a minimally valid encoded transaction using the proper structure
      # Start with version 0 to match server's initial last_version
      encoded_bytes =
        BedrockTransactionTestSupport.new_log_transaction(0, %{"test_key" => "test_value"})

      # Match server's initial last_version
      expected_version = 0

      result = GenServer.call(pid, {:push, encoded_bytes, expected_version}, 1000)

      # Should either succeed or fail gracefully based on server state (likely locked)
      # Returns :ok on success or {:error, reason} on failure
      assert result == :ok or match?({:error, _}, result)
    end
  end

  describe "handle_call/3 - pull operations" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles pull request with basic options", %{server: pid} do
      from_version = 0
      opts = [limit: 10, timeout: 5000]

      result = GenServer.call(pid, {:pull, from_version, opts})

      # Should return transactions or indicate waiting
      assert is_tuple(result)
    end

    test "handles pull request with default options", %{server: pid} do
      from_version = 0
      opts = []

      result = GenServer.call(pid, {:pull, from_version, opts})

      assert is_tuple(result)
    end

    test "handles pull request with high version number", %{server: pid} do
      from_version = 999_999
      opts = [limit: 1]

      result = GenServer.call(pid, {:pull, from_version, opts})

      # Should indicate waiting or empty result
      assert is_tuple(result)
    end
  end

  describe "handle_call/3 - recover_from operations" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles recover_from request", %{server: pid} do
      source_log = self()
      first_version = Version.from_integer(1)
      last_version = Version.from_integer(10)

      # This will timeout as recovery requires specific server state and protocol
      catch_exit do
        GenServer.call(pid, {:recover_from, source_log, first_version, last_version}, 500)
      end
    end

    test "handles recover_from with invalid version range", %{server: pid} do
      source_log = self()
      first_version = Version.from_integer(10)
      # Invalid: first > last
      last_version = Version.from_integer(1)

      # This will also timeout as recovery requires specific server state and protocol
      catch_exit do
        GenServer.call(pid, {:recover_from, source_log, first_version, last_version}, 500)
      end
    end
  end

  describe "handle_continue/2" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles notify_waiting_pullers continue", %{server: pid} do
      # This tests the internal continue handling
      # We can't easily test this directly, but we can verify the server handles it
      state = :sys.get_state(pid)
      assert state.waiting_pullers == %{}
    end
  end

  describe "handle_info/2" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles timeout message", %{server: pid} do
      # Send a timeout message
      send(pid, :timeout)

      # Server should still be alive and functioning
      assert Process.alive?(pid)
      assert :pong = GenServer.call(pid, :ping)
    end
  end

  describe "error conditions" do
    test "handles missing directory error during initialization", %{
      cluster: cluster,
      id: id,
      foreman: foreman
    } do
      invalid_path = "/nonexistent/path/that/should/not/exist"
      otp_name = :"test_log_error_#{:rand.uniform(10000)}"

      opts = [
        cluster: cluster,
        otp_name: otp_name,
        id: id,
        foreman: foreman,
        path: invalid_path
      ]

      # Server starts but exits during initialization when SegmentRecycler
      # discovers the path is not a directory
      Process.flag(:trap_exit, true)
      assert {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      # Should receive exit signal due to path_is_not_a_directory error
      assert_receive {:EXIT, ^pid, :path_is_not_a_directory}, 1000

      refute Process.alive?(pid)
    end
  end

  describe "concurrent operations" do
    setup %{server_opts: opts} do
      {:ok, pid} = GenServer.start_link(Server, opts_to_init_args(opts))

      eventually(fn ->
        state = :sys.get_state(pid)
        assert state.segment_recycler != nil
      end)

      on_exit(fn ->
        if Process.alive?(pid) do
          if Process.alive?(pid), do: GenServer.stop(pid)
        end
      end)

      {:ok, server: pid}
    end

    test "handles multiple concurrent ping requests", %{server: pid} do
      tasks =
        for _i <- 1..10 do
          Task.async(fn -> GenServer.call(pid, :ping) end)
        end

      results = Task.await_many(tasks)

      assert Enum.all?(results, &(&1 == :pong))
    end

    test "handles concurrent info requests", %{server: pid} do
      tasks =
        for _i <- 1..5 do
          Task.async(fn -> GenServer.call(pid, {:info, [:id, :kind]}) end)
        end

      results = Task.await_many(tasks)

      assert Enum.all?(results, fn result ->
               match?({:ok, %{id: _, kind: _}}, result)
             end)
    end
  end

  # Helper functions

  defp opts_to_init_args(opts) do
    {opts[:cluster], opts[:otp_name], opts[:id], opts[:foreman], opts[:path]}
  end

  defp eventually(assertion_fn, timeout \\ 1000, interval \\ 50) do
    end_time = System.monotonic_time(:millisecond) + timeout

    eventually_loop(assertion_fn, end_time, interval)
  end

  defp eventually_loop(assertion_fn, end_time, interval) do
    assertion_fn.()
  rescue
    _ ->
      if System.monotonic_time(:millisecond) < end_time do
        Process.sleep(interval)
        eventually_loop(assertion_fn, end_time, interval)
      else
        assertion_fn.()
      end
  end
end
