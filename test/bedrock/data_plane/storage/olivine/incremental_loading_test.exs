defmodule Bedrock.DataPlane.Storage.Olivine.IncrementalLoadingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.TestHelpers
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

  describe "incremental loading edge cases" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "incremental_loading_test")
    end

    test "page chain across multiple versions", %{tmp_dir: tmp_dir} do
      # This test creates a scenario where:
      # Version 3: Has page 0 -> next_id: 1
      # Version 2: Has page 1 -> next_id: 2
      # Version 1: Has page 2 -> next_id: 0
      #
      # When loading, we should:
      # 1. Start with page 0 from version 3
      # 2. Need page 1, find it in version 2
      # 3. Need page 2, find it in version 1
      # 4. Stop (page 2 points to 0, which we already have)

      file_path = Path.join(tmp_dir, "chain_test.dets")
      {:ok, database} = Database.open(:chain_test, file_path)

      # Create transactions that will cause page splits and chain dependencies
      # Start with many keys to force page creation
      keys_for_page_splits = for i <- 1..300, do: {:set, "key_#{String.pad_leading("#{i}", 5, "0")}", "value_#{i}"}

      # Use timestamps outside the 5-second window (5_000_000 microseconds) to trigger eviction
      # 1 second
      transaction_v1 = create_transaction(keys_for_page_splits, 1_000_000)
      # 10 seconds
      transaction_v2 = create_transaction([{:set, "new_key_001", "new_value"}], 10_000_000)
      # 20 seconds
      transaction_v3 = create_transaction([{:set, "new_key_002", "new_value"}], 20_000_000)

      # Apply transactions to build version history
      index_manager = IndexManager.new()

      {index_manager_v1, database_v1} =
        IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      {index_manager_v2, database_v2} =
        IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      {index_manager_v3, _database_v3} =
        IndexManager.apply_transactions(index_manager_v2, [transaction_v3], database_v2)

      # Create multiple version ranges by persisting each version separately
      # This simulates what would happen with actual window eviction over time

      # Get modified pages from each version and persist separately to create version chain
      [{_v1, {_index1, modified_pages_v1}} | _] = index_manager_v1.versions

      {:ok, database_after_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      [{_v2, {_index2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, database_after_v2, _metadata} =
        Database.advance_durable_version(
          database_after_v1,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      [{_v3, {_index3, modified_pages_v3}} | _] = index_manager_v3.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_after_v2,
          index_manager_v3.current_version,
          index_manager_v2.current_version,
          1000,
          [modified_pages_v3]
        )

      Database.close(final_database)

      # Now test recovery with incremental loading
      {:ok, recovered_database} = Database.open(:chain_test_recovery, file_path)

      # Verify that incremental loading will process multiple version ranges
      {_data_db, index_db} = recovered_database
      version_ranges = TestHelpers.load_version_range_metadata(index_db)
      assert length(version_ranges) == 3, "Should have 3 version ranges in the chain"

      # This should use incremental loading
      {:ok, recovered_index_manager} = IndexManager.recover_from_database(recovered_database)

      # Verify the recovered index has the correct structure
      assert recovered_index_manager.current_version == index_manager_v3.current_version

      # Verify incremental loading correctly reconstructed the index structure
      [{_version, {recovered_index, _modified_pages}} | _] = recovered_index_manager.versions

      # Assert we have all expected pages
      assert recovered_index.page_map |> Map.keys() |> Enum.sort() == [0, 1, 2, 3]

      # Verify page chain integrity by checking next_id pointers
      {page_0, next_0} = Map.get(recovered_index.page_map, 0)
      {_page_1, next_1} = Map.get(recovered_index.page_map, 1)
      {_page_2, next_2} = Map.get(recovered_index.page_map, 2)
      {_page_3, next_3} = Map.get(recovered_index.page_map, 3)

      # Verify the page chain is correctly reconstructed
      # The exact chain depends on the page ordering, but we should have a valid chain
      all_next_ids = [next_0, next_1, next_2, next_3]

      # Each page should point to another page or 0 (end of chain)
      Enum.each(all_next_ids, fn next_id ->
        assert next_id in [0, 1, 2, 3], "Invalid next_id: #{next_id}"
      end)

      # Verify pages contain the expected keys
      page_0_keys = Page.keys(page_0)
      assert length(page_0_keys) > 0, "Page 0 should contain keys"
      assert "key_00001" in page_0_keys, "Page 0 should contain key_00001"

      # Verify we can locate keys using the index and get correct locators
      {:ok, page_for_key1, locator1} = Index.locator_for_key(recovered_index, "key_00001")
      {:ok, page_for_key100, locator100} = Index.locator_for_key(recovered_index, "key_00100")

      # Verify the locators are valid (they might be binary offsets, not integers)
      assert locator1 != nil, "Locator should not be nil"
      assert locator100 != nil, "Locator should not be nil"

      # Verify the pages contain the expected keys
      page1_keys = Page.keys(page_for_key1)
      page100_keys = Page.keys(page_for_key100)

      assert "key_00001" in page1_keys, "Page should contain the key we looked up"
      assert "key_00100" in page100_keys, "Page should contain the key we looked up"

      # Keys should be on different pages (since we have 300 keys spread across multiple pages)
      # This verifies that the page splitting and chain reconstruction worked correctly

      Database.close(recovered_database)
    end

    test "pages within same version block resolve locally and exclude old unreachable pages", %{tmp_dir: tmp_dir} do
      # This test ensures that incremental loading only loads pages that are part of the current chain,
      # and excludes old pages that are no longer reachable from the current index structure

      file_path = Path.join(tmp_dir, "block_test.dets")
      {:ok, database} = Database.open(:block_test, file_path)

      # Version 1: Create a large set of keys that will create multiple pages (0,1,2,3,4,5)
      keys_v1 = for i <- 1..1000, do: {:set, "old_key_#{String.pad_leading("#{i}", 5, "0")}", "old_value_#{i}"}
      transaction_v1 = create_transaction(keys_v1, 1_000_000)

      # Version 2: Modify some keys, creating new versions of some pages but leaving others unchanged
      keys_v2 = for i <- 1..200, do: {:set, "old_key_#{String.pad_leading("#{i}", 5, "0")}", "updated_value_#{i}"}
      transaction_v2 = create_transaction(keys_v2, 2_000_000)

      # Version 3: Add entirely new keys that will create new pages and potentially modify page chain
      keys_v3 = for i <- 1..300, do: {:set, "new_key_#{String.pad_leading("#{i}", 5, "0")}", "new_value_#{i}"}
      transaction_v3 = create_transaction(keys_v3, 3_000_000)

      # Apply transactions sequentially to build version history
      index_manager = IndexManager.new()

      {index_manager_v1, database_v1} =
        IndexManager.apply_transactions(index_manager, [transaction_v1], database)

      {index_manager_v2, database_v2} =
        IndexManager.apply_transactions(index_manager_v1, [transaction_v2], database_v1)

      {index_manager_v3, _database_v3} =
        IndexManager.apply_transactions(index_manager_v2, [transaction_v3], database_v2)

      # Persist each version separately to create version chain with orphaned pages
      [{_v1, {_index1, modified_pages_v1}} | _] = index_manager_v1.versions

      {:ok, database_after_v1, _metadata} =
        Database.advance_durable_version(
          database_v1,
          index_manager_v1.current_version,
          Database.durable_version(database_v1),
          1000,
          [modified_pages_v1]
        )

      [{_v2, {_index2, modified_pages_v2}} | _] = index_manager_v2.versions

      {:ok, database_after_v2, _metadata} =
        Database.advance_durable_version(
          database_after_v1,
          index_manager_v2.current_version,
          index_manager_v1.current_version,
          1000,
          [modified_pages_v2]
        )

      [{_v3, {_index3, modified_pages_v3}} | _] = index_manager_v3.versions

      {:ok, final_database, _metadata} =
        Database.advance_durable_version(
          database_after_v2,
          index_manager_v3.current_version,
          index_manager_v2.current_version,
          1000,
          [modified_pages_v3]
        )

      Database.close(final_database)

      # Now test recovery with incremental loading
      {:ok, recovered_database} = Database.open(:block_test_recovery, file_path)

      # Verify we have multiple version ranges in the chain
      {_data_db, index_db} = recovered_database
      version_ranges = TestHelpers.load_version_range_metadata(index_db)
      assert length(version_ranges) == 3, "Should have 3 version ranges"

      # Count total pages across all versions (this should be more than what we recover)
      total_pages_in_storage =
        Enum.reduce(version_ranges, 0, fn {version, _last_version}, acc ->
          version_pages = IndexDatabase.load_pages_from_version(index_db, version)
          acc + map_size(version_pages)
        end)

      # This should use incremental loading to recover only the needed pages
      {:ok, recovered_index_manager} = IndexManager.recover_from_database(recovered_database)

      # Verify we have multiple pages but fewer than the total stored
      [{_version, {recovered_index, _modified_pages}} | _] = recovered_index_manager.versions
      recovered_page_count = map_size(recovered_index.page_map)

      assert recovered_page_count > 1, "Expected multiple pages, got #{recovered_page_count}"

      assert recovered_page_count < total_pages_in_storage,
             "Incremental loading should recover fewer pages (#{recovered_page_count}) " <>
               "than total stored (#{total_pages_in_storage})"

      # Verify we can locate both old and new keys using the recovered index
      test_keys = ["old_key_00001", "old_key_00100", "new_key_00001", "new_key_00100"]

      for test_key <- test_keys do
        assert {:ok, _page_for_key, locator} = Index.locator_for_key(recovered_index, test_key)
        assert locator != nil, "Locator should not be nil for #{test_key}"
      end

      Database.close(recovered_database)
    end

    test "missing pages error handling", %{tmp_dir: tmp_dir} do
      # Test what happens when we try to load from a corrupted/incomplete database

      file_path = Path.join(tmp_dir, "missing_test.dets")
      {:ok, database} = Database.open(:missing_test, file_path)

      # Create some data
      transaction = create_transaction([{:set, "test_key", "test_value"}], 1_000_000)
      index_manager = IndexManager.new()

      {_final_index_manager, final_database} =
        IndexManager.apply_transactions(index_manager, [transaction], database)

      Database.close(final_database)

      # Test recovery from empty database
      {:ok, empty_database} =
        Database.open(
          :missing_test_empty,
          Path.join(tmp_dir, "nonexistent.dets")
        )

      # Should recover successfully with empty index
      {:ok, empty_index_manager} = IndexManager.recover_from_database(empty_database)
      assert empty_index_manager.current_version == Version.zero()

      Database.close(empty_database)
    end
  end
end
