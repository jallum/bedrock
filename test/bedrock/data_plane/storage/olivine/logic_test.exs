defmodule Bedrock.DataPlane.Storage.Olivine.LogicTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.ReadingTestHelpers
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.KeySelector

  setup do
    test_dir = Path.join(System.tmp_dir!(), "olivine_logic_test_#{:rand.uniform(100_000)}")
    File.rm_rf!(test_dir)

    on_exit(fn ->
      # Give WAL files time to be released after shutdown
      Process.sleep(50)
      # Try cleanup multiple times to handle WAL file locks
      Enum.reduce_while(1..3, :error, fn _attempt, _acc ->
        case File.rm_rf(test_dir) do
          {:ok, _} ->
            {:halt, :ok}

          _error ->
            Process.sleep(50)
            {:cont, :error}
        end
      end)
    end)

    {:ok, test_dir: test_dir}
  end

  # Helper function to create and start a test logic state
  defp create_test_state(test_dir, otp_name \\ :test_storage, id \\ "test") do
    # Suppress expected connection retry logs during logic startup
    {result, _logs} =
      with_log(fn ->
        Logic.startup(otp_name, self(), id, test_dir, pool_size: 1)
      end)

    {:ok, state} = result
    state
  end

  describe "startup/4" do
    test "creates directory and initializes state", %{test_dir: test_dir} do
      otp_name = :test_storage
      foreman = self()
      id = "test_shard"

      {result, _logs} =
        with_log(fn ->
          Logic.startup(otp_name, foreman, id, test_dir, pool_size: 1)
        end)

      assert {:ok,
              %State{
                path: ^test_dir,
                otp_name: ^otp_name,
                id: ^id,
                foreman: ^foreman,
                database: {_, _},
                index_manager: %IndexManager{}
              } = state} = result

      assert File.dir?(test_dir)
      Logic.shutdown(state)
    end

    test "handles directory creation failure" do
      invalid_path = "/invalid/read-only/path"

      result = Logic.startup(:test_storage, self(), "test", invalid_path, pool_size: 1)
      assert {:error, _posix_error} = result
    end
  end

  describe "shutdown/1" do
    test "properly shuts down all components", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      assert :ok = Logic.shutdown(state)
    end
  end

  describe "lock_for_recovery/3" do
    test "locks successfully with valid epoch", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      director = make_ref()
      epoch = 5

      assert {:ok,
              %State{
                mode: :locked,
                director: ^director,
                epoch: ^epoch
              } = locked_state} = Logic.lock_for_recovery(state, director, epoch)

      Logic.shutdown(locked_state)
    end

    test "rejects older epoch", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      state = %{state | epoch: 10}
      director = make_ref()
      old_epoch = 5

      assert {:error, :newer_epoch_exists} = Logic.lock_for_recovery(state, director, old_epoch)
      Logic.shutdown(state)
    end

    test "handles locking without active puller", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      director = make_ref()
      epoch = 1

      assert {:ok, %State{pull_task: nil} = locked_state} = Logic.lock_for_recovery(state, director, epoch)
      Logic.shutdown(locked_state)
    end
  end

  describe "unlock_after_recovery/3" do
    test "basic unlock functionality", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      locked_state = %{state | mode: :locked, epoch: 1}
      layout = %{logs: %{}, services: %{}}
      durable_version = 100

      assert {:ok, %State{mode: :running} = unlocked_state} =
               Logic.unlock_after_recovery(locked_state, durable_version, layout)

      Logic.shutdown(unlocked_state)
    end
  end

  describe "get/4" do
    test "returns error for missing key in sync mode", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      assert {:error, _} = ReadingTestHelpers.get(state, "nonexistent_key", 1, [])
      Logic.shutdown(state)
    end

    test "returns immediate error with reply_fn when fetch fails", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      reply_fn = fn result -> send(self(), {:async_result, result}) end

      # Immediate errors return error, not task pid
      assert {:error, _} = ReadingTestHelpers.get(state, "async_key", 1, reply_fn: reply_fn)
      Logic.shutdown(state)
    end

    test "handles KeySelector with immediate error", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      key_selector = KeySelector.first_greater_or_equal("selector_key")
      reply_fn = fn result -> send(self(), {:selector_result, result}) end

      assert {:error, _} = ReadingTestHelpers.get(state, key_selector, 1, reply_fn: reply_fn)
      Logic.shutdown(state)
    end

    test "handles various version scenarios", %{test_dir: test_dir} do
      state = create_test_state(test_dir)

      # Test different versions that trigger error paths
      assert {:error, _} = ReadingTestHelpers.get(state, "key", 0, [])
      assert {:error, _} = ReadingTestHelpers.get(state, "key", 999_999, [])
      Logic.shutdown(state)
    end
  end

  describe "get_range/5" do
    test "executes synchronously and returns version_too_old for empty database", %{test_dir: test_dir} do
      state = create_test_state(test_dir)

      assert {:error, :version_too_old} =
               ReadingTestHelpers.get_range(state, "range_key1", "range_key3", 1, [])

      Logic.shutdown(state)
    end

    test "returns immediate error with reply_fn when range fetch fails", %{test_dir: test_dir} do
      state = create_test_state(test_dir)
      reply_fn = fn result -> send(self(), {:range_result, result}) end

      assert {:error, _} =
               ReadingTestHelpers.get_range(state, "async_range1", "async_range3", 1, reply_fn: reply_fn)

      Logic.shutdown(state)
    end

    test "respects limit parameter in options", %{test_dir: test_dir} do
      state = create_test_state(test_dir)

      assert {:error, :version_too_old} =
               ReadingTestHelpers.get_range(state, "limit_key1", "limit_key9", 1, limit: 2)

      Logic.shutdown(state)
    end
  end

  describe "stop_pulling/1" do
    test "handles state with no puller task" do
      state = %State{pull_task: nil}
      assert %State{pull_task: nil} = Logic.stop_pulling(state)
    end
  end
end
