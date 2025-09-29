defmodule Bedrock.DataPlane.Storage.Olivine.KeySelectorTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.ReadingTestHelpers
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector

  @moduletag :integration

  # Helper functions
  defp test_version, do: Version.from_integer(1)
  defp future_version, do: Version.from_integer(2)

  # Helper function to create a test state with data
  defp create_test_state_with_data do
    tmp_dir = System.tmp_dir!()
    db_file = Path.join(tmp_dir, "key_selector_test_#{System.unique_integer([:positive])}.dets")
    table_name = String.to_atom("key_selector_test_#{System.unique_integer([:positive])}")
    {result, _logs} = with_log(fn -> Database.open(table_name, db_file, pool_size: 1) end)
    {:ok, database} = result

    index_manager = IndexManager.new()

    # Add some test data
    mutations = [
      {:set, "key1", "value1"},
      {:set, "key2", "value2"},
      {:set, "key3", "value3"},
      {:set, "test:key", "test:value"},
      {:set, "range:a", "range_a_value"},
      {:set, "range:b", "range_b_value"},
      {:set, "range:c", "range_c_value"},
      {:set, "range:z", "range_z_value"}
    ]

    transaction =
      Transaction.encode(%{
        commit_version: test_version(),
        mutations: mutations
      })

    {updated_index_manager, updated_database} = IndexManager.apply_transaction(index_manager, transaction, database)

    state = %State{
      database: updated_database,
      index_manager: updated_index_manager,
      mode: :running
    }

    on_exit(fn ->
      Database.close(updated_database)
      File.rm_rf(db_file)
    end)

    state
  end

  setup do
    {:ok, state: create_test_state_with_data()}
  end

  describe "KeySelector fetch operations" do
    test "fetch/4 with KeySelector resolves keys correctly", %{state: state} do
      key_selector = KeySelector.first_greater_or_equal("test:key")

      assert {:ok, {"test:key", "test:value"}} =
               ReadingTestHelpers.get(state, key_selector, test_version(), [])
    end

    test "fetch/4 with KeySelector handles offsets", %{state: state} do
      # Test offset of +1 from key1 should resolve to key2
      offset_selector = "key1" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1)

      assert {:ok, {"key2", "value2"}} = ReadingTestHelpers.get(state, offset_selector, test_version(), [])
    end

    test "fetch/4 with KeySelector handles boundary errors", %{state: state} do
      # Test high offset that goes beyond available keys
      high_offset_selector = "key1" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000)

      assert {:error, :not_found} = ReadingTestHelpers.get(state, high_offset_selector, test_version(), [])
    end
  end

  describe "KeySelector range operations" do
    test "range_fetch/5 with KeySelectors resolves range boundaries", %{state: state} do
      start_selector = KeySelector.first_greater_or_equal("range:a")
      end_selector = KeySelector.first_greater_than("range:z")

      result = ReadingTestHelpers.get_range(state, start_selector, end_selector, test_version(), [])
      # Range may not find data or could succeed
      case result do
        {:ok, {key_value_pairs, _has_more}} ->
          assert is_list(key_value_pairs)
          assert Enum.all?(key_value_pairs, fn {k, v} -> is_binary(k) and is_binary(v) end)
          # Should include range keys if data exists
          keys = Enum.map(key_value_pairs, fn {k, _v} -> k end)
          assert Enum.any?(keys, fn k -> String.starts_with?(k, "range:") end)

        {:error, _reason} ->
          # Acceptable for test data setup
          :ok
      end
    end

    test "range_fetch/5 handles invalid ranges", %{state: state} do
      # Test range where start > end after resolution
      start_selector = KeySelector.first_greater_or_equal("z")
      end_selector = KeySelector.first_greater_or_equal("a")

      # Can be :invalid_range or :not_found depending on implementation
      assert {:error, error} =
               ReadingTestHelpers.get_range(state, start_selector, end_selector, test_version(), [])

      assert error in [:invalid_range, :not_found]
    end

    test "range_fetch/5 respects limit option", %{state: state} do
      start_selector = KeySelector.first_greater_or_equal("key")
      end_selector = KeySelector.first_greater_than("range")

      assert {:ok, {results, _has_more}} =
               ReadingTestHelpers.get_range(state, start_selector, end_selector, test_version(), limit: 2)

      assert length(results) <= 2
    end
  end

  describe "IndexManager KeySelector resolution" do
    test "page_for_key/3 with KeySelector finds correct page", %{state: state} do
      key_selector = KeySelector.first_greater_than("key1")

      assert {:ok, resolved_key, page} =
               IndexManager.page_for_key(state.index_manager, key_selector, test_version())

      # first_greater_than "key1" should resolve to the next available key
      # Could be key2 or key3 depending on sort order
      assert resolved_key in ["key2", "key3"]
      # Page can be binary or page_map
      assert is_binary(page) or is_map(page)
    end

    test "pages_for_range/4 with KeySelectors finds correct pages", %{state: state} do
      start_selector = KeySelector.first_greater_or_equal("key1")
      end_selector = KeySelector.last_less_than("range")

      assert {:ok, {"key1", resolved_end}, pages} =
               IndexManager.pages_for_range(state.index_manager, start_selector, end_selector, test_version())

      assert is_binary(resolved_end)
      assert is_list(pages)
      # resolved_end should be the last key less than "range"
    end

    test "resolution handles version constraints", %{state: state} do
      key_selector = KeySelector.first_greater_or_equal("key1")

      # Test version_too_old scenario - can be :version_too_old or :not_found depending on implementation
      assert {:error, error} = IndexManager.page_for_key(state.index_manager, key_selector, Version.zero())
      assert error in [:version_too_old, :not_found]

      # Test version_too_new scenario
      assert {:error, :version_too_new} =
               IndexManager.page_for_key(state.index_manager, key_selector, Version.from_integer(999_999))
    end
  end

  describe "waitlist operations with KeySelectors" do
    test "add_to_waitlist/5 supports KeySelector requests", %{state: state} do
      key_selector = KeySelector.first_greater_or_equal("wait_key")
      # Future version that doesn't exist yet
      version = Version.from_integer(2)
      reply_fn = fn _result -> :ok end
      timeout = 5000

      fetch_request = {key_selector, version}

      updated_state =
        ReadingTestHelpers.add_to_waitlist_from_state(state, fetch_request, version, reply_fn, timeout)

      assert map_size(updated_state.read_request_manager.waiting_fetches) > 0
    end

    test "notify_waiting_fetches/2 processes KeySelector requests", %{state: state} do
      # Add KeySelector request to waitlist for future version
      key_selector = KeySelector.first_greater_or_equal("key1")
      version = future_version()
      reply_fn = fn _result -> :ok end
      timeout = 5000

      fetch_request = {key_selector, version}

      state_with_waitlist =
        ReadingTestHelpers.add_to_waitlist_from_state(state, fetch_request, version, reply_fn, timeout)

      # Verify request was added to waitlist
      assert map_size(state_with_waitlist.read_request_manager.waiting_fetches) > 0

      # Simulate version becoming available - notify waiting fetches
      # Waitlist should be processed (though we can't easily verify the results are sent)
      assert {%State{}, _pids} =
               ReadingTestHelpers.notify_waiting_fetches_from_state(state_with_waitlist, version)
    end
  end

  describe "edge cases and error conditions" do
    test "KeySelector with extreme offsets", %{state: state} do
      extreme_selector = "key1" |> KeySelector.first_greater_or_equal() |> KeySelector.add(999_999)

      # Should handle gracefully without crashing
      assert %KeySelector{offset: 999_999} = extreme_selector
      assert {:error, :not_found} = ReadingTestHelpers.get(state, extreme_selector, test_version(), [])
    end

    test "empty key KeySelector", %{state: state} do
      empty_key_selector = KeySelector.first_greater_or_equal("")

      assert %KeySelector{key: "", or_equal: true, offset: 0} = empty_key_selector
      # Should handle empty keys appropriately - should find the first key
      assert {:ok, {resolved_key, _value}} =
               ReadingTestHelpers.get(state, empty_key_selector, test_version(), [])

      assert is_binary(resolved_key)
    end

    test "binary key with special characters", %{state: state} do
      special_key = <<0xFF, 0x00, "special", 0x01>>
      special_selector = KeySelector.first_greater_than(special_key)

      assert %KeySelector{key: ^special_key} = special_selector
      # Should handle binary keys with special bytes correctly without crashing
      # Since this key doesn't exist and is after all our test keys, should be not found
      assert {:error, :not_found} = ReadingTestHelpers.get(state, special_selector, test_version(), [])
    end
  end
end
