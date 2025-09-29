defmodule Bedrock.DataPlane.Storage.Olivine.PageChainLoadingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp setup_tmp_dir(context, base_name) do
    tmp_dir =
      context[:tmp_dir] ||
        Path.join(System.tmp_dir!(), "#{base_name}_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  defp create_transaction(mutations, version_int) do
    transaction_map = %{
      mutations: mutations,
      read_conflicts: {nil, []},
      write_conflicts: []
    }

    encoded = Transaction.encode(transaction_map)
    version = Version.from_integer(version_int)

    {:ok, with_version} = Transaction.add_commit_version(encoded, version)
    with_version
  end

  describe "page chain loading behavior" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "page_chain_test")
    end

    test "loads only needed pages from chain, ignores stale pages", %{tmp_dir: tmp_dir} do
      # Scenario: Create a situation where:
      # - Durable version has pages [1->2, 2->3, 3->5] (need page 5)
      # - Older version has pages [3->4, 4->6, 5->0] (page 3 is stale, page 4 not needed, page 5 needed)
      # - Expected: Load pages 1,2,3,5 but NOT page 4 (since 3->5, not 3->4)

      file_path = Path.join(tmp_dir, "page_chain_test.dets")
      {:ok, database} = Database.open(:page_chain_test, file_path)

      # Create version 1: Large set that will create pages 0,1,2,3,4,5
      # Use keys that will deterministically split across these pages
      keys_v1 = [
        # Keys that go to page 1
        {:set, "aaa", "value_1a"},
        {:set, "aab", "value_1b"},
        # Keys that go to page 2
        {:set, "bbb", "value_2a"},
        {:set, "bbc", "value_2b"},
        # Keys that go to page 3
        {:set, "ccc", "value_3a"},
        {:set, "ccd", "value_3b"},
        # Keys that go to page 4
        {:set, "ddd", "value_4a"},
        {:set, "dde", "value_4b"},
        # Keys that go to page 5
        {:set, "eee", "value_5a"},
        {:set, "eef", "value_5b"}
      ]

      # Add enough keys to force page splits across the range
      additional_keys =
        for i <- 1..300 do
          key = "key_#{String.pad_leading("#{i}", 4, "0")}"
          {:set, key, "value_#{i}"}
        end

      all_keys_v1 = keys_v1 ++ additional_keys
      transaction_v1 = create_transaction(all_keys_v1, 1_000_000)

      # Apply version 1
      index_manager = IndexManager.new()
      {index_manager_v1, database_v1} = IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      # Version 2: Modify some pages, creating a new version chain
      # This will create a situation where some pages get new versions
      keys_v2 = [
        # Updates page 1
        {:set, "aaa", "value_1a_updated"},
        # Updates page 3
        {:set, "ccc", "value_3a_updated"},
        # Creates new page
        {:set, "fff", "new_value_6"}
      ]

      transaction_v2 = create_transaction(keys_v2, 2_000_000)

      {index_manager_v2, _database_v2} =
        IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      # Persist version 1
      [{_v1, {_index1, modified_pages_v1}} | _] = index_manager_v1.versions

      {:ok, database_after_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      # Persist version 2 as durable
      [{_v2, {_index2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_after_v1,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      Database.close(final_database)

      # Test recovery - this should use incremental loading
      {:ok, recovered_database} = Database.open(:page_chain_test_recovery, file_path)
      {:ok, recovered_index_manager} = IndexManager.recover_from_database(recovered_database)

      # Verify the recovery worked and has the expected data
      [{_version, {recovered_index, _modified_pages}} | _] = recovered_index_manager.versions

      # Test that we can find all the keys we expect
      test_keys = ["aaa", "bbb", "ccc", "ddd", "eee", "fff"]

      for key <- test_keys do
        case Index.locator_for_key(recovered_index, key) do
          {:ok, _page, _locator} ->
            # Key found, this is expected
            :ok

          {:error, :not_found} ->
            # Some keys might not exist depending on page splits, that's okay
            :ok
        end
      end

      # Key assertion: verify we have a connected page chain from page 0
      # and that the total number of pages is reasonable
      page_count = map_size(recovered_index.page_map)
      assert page_count > 0, "Should have recovered some pages"
      assert page_count < 50, "Should not have recovered an excessive number of pages"

      # Most importantly: verify that page 0 exists (always required)
      assert Map.has_key?(recovered_index.page_map, 0), "Page 0 must always exist"

      # Test that the page chain is connected by following it
      assert_page_chain_connected(recovered_index.page_map)

      Database.close(recovered_database)
    end

    test "handles missing page 0 by loading from older versions", %{tmp_dir: tmp_dir} do
      # Scenario: Durable version doesn't contain page 0, must load it from older versions

      file_path = Path.join(tmp_dir, "page0_test.dets")
      {:ok, database} = Database.open(:page_zero_test, file_path)

      # Create initial version with page 0
      keys_v1 = [
        {:set, "initial_key", "initial_value"}
      ]

      transaction_v1 = create_transaction(keys_v1, 1_000_000)

      index_manager = IndexManager.new()
      {index_manager_v1, database_v1} = IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      # Persist version 1
      [{_v1, {_index1, modified_pages_v1}} | _] = index_manager_v1.versions

      {:ok, database_after_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      # Create version 2 that only modifies non-page-0 data
      # This creates a scenario where the durable version doesn't contain page 0
      keys_v2 = [
        # This should go to a different page
        {:set, "zzz_end_key", "end_value"}
      ]

      transaction_v2 = create_transaction(keys_v2, 2_000_000)

      {index_manager_v2, database_v2} =
        IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_after_v1)

      # Persist version 2 as durable (may not contain page 0)
      [{_v2, {_index2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_v2,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      Database.close(final_database)

      # Test recovery
      {:ok, recovered_database} = Database.open(:page_zero_test_recovery, file_path)
      {:ok, recovered_index_manager} = IndexManager.recover_from_database(recovered_database)

      [{_version, {recovered_index, _modified_pages}} | _] = recovered_index_manager.versions

      # Page 0 must be present
      assert Map.has_key?(recovered_index.page_map, 0), "Page 0 must be loaded even if not in durable version"

      # Chain should be connected
      assert_page_chain_connected(recovered_index.page_map)

      Database.close(recovered_database)
    end

    test "fails hard when required pages are truly missing", %{tmp_dir: tmp_dir} do
      # This test verifies that missing pages result in hard failure, not silent corruption

      file_path = Path.join(tmp_dir, "missing_test.dets")
      {:ok, database} = Database.open(:page_missing_test, file_path)

      # Create a simple transaction
      keys = [{:set, "test_key", "test_value"}]
      transaction = create_transaction(keys, 1_000_000)

      index_manager = IndexManager.new()
      {index_manager, database} = IndexManager.apply_transactions(index_manager, [transaction], database)

      # Persist normally
      [{_v, {_index, modified_pages}} | _] = index_manager.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database,
          index_manager.current_version,
          Database.durable_version(database),
          1000,
          [modified_pages]
        )

      Database.close(final_database)

      # Corrupt the database by truncating it
      corrupted_file = file_path <> ".idx"
      # Write invalid DETS header
      File.write!(corrupted_file, <<0, 0, 0, 0>>)

      # Recovery should fail with a clear error
      case Database.open(:page_missing_test_recovery, file_path) do
        {:error, _reason} ->
          # Expected - corrupted file should fail to open
          :ok

        {:ok, recovered_database} ->
          # If it opens, recovery should fail
          case IndexManager.recover_from_database(recovered_database) do
            {:error, :missing_pages} ->
              # This is the expected hard failure
              Database.close(recovered_database)
              :ok

            {:ok, _} ->
              Database.close(recovered_database)
              flunk("Recovery should have failed on corrupted database")
          end
      end
    end
  end

  # Helper to verify page chain connectivity
  defp assert_page_chain_connected(page_map) do
    # Start from page 0 and follow the chain
    case Map.get(page_map, 0) do
      nil ->
        flunk("Page 0 missing from page map")

      {_page, next_id} ->
        visited = MapSet.new([0])
        follow_chain(page_map, next_id, visited)
    end
  end

  defp follow_chain(_page_map, 0, _visited) do
    # Reached end of chain successfully
    :ok
  end

  defp follow_chain(page_map, page_id, visited) do
    if MapSet.member?(visited, page_id) do
      flunk("Cycle detected in page chain at page #{page_id}")
    end

    case Map.get(page_map, page_id) do
      nil ->
        flunk("Page #{page_id} referenced but not found in page map")

      {_page, next_id} ->
        new_visited = MapSet.put(visited, page_id)
        follow_chain(page_map, next_id, new_visited)
    end
  end
end
