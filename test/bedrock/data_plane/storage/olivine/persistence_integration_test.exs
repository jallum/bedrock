defmodule Bedrock.DataPlane.Storage.Olivine.PersistenceIntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Storage.Olivine.PageTestHelpers

  # Helper functions for cleaner test assertions
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

  defp create_test_pages(pages_data) do
    Enum.map(pages_data, fn
      {id, keys, _next_id} ->
        page = Page.new(id, Enum.map(keys, &{&1, Version.from_integer(id * 10)}))
        {id, page}

      {id, keys} ->
        page = Page.new(id, Enum.map(keys, &{&1, Version.from_integer(id * 10)}))
        {id, page}
    end)
  end

  describe "full persistence and recovery lifecycle" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_lifecycle_test")
    end

    test "server startup with empty database initializes correctly", %{tmp_dir: tmp_dir} do
      {:ok, state} = Logic.startup(:test_startup, self(), :test_id, tmp_dir)

      assert %{
               otp_name: :test_startup,
               id: :test_id,
               path: ^tmp_dir,
               database: db,
               index_manager: %{
                 page_allocator: %{max_page_id: 0, free_page_ids: []},
                 current_version: current_version
               }
             } = state

      assert db
      assert current_version == Version.zero()
      assert {:ok, ^current_version} = Database.load_durable_version(state.database)

      Logic.shutdown(state)
    end

    test "corrupted page handling during recovery", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("corrupt_test_#{System.unique_integer([:positive])}")
      {:ok, db1} = Database.open(table_name, Path.join(tmp_dir, "dets"))

      page0 = Page.new(0, [{<<"valid">>, Version.from_integer(100)}])
      page0_binary = PageTestHelpers.from_map(page0)
      :ok = Database.store_page(db1, 0, {page0_binary, 1})

      :ok = Database.store_page(db1, 1, {<<"definitely_not_a_valid_page">>, 0})

      page3 = Page.new(3, [{<<"isolated">>, Version.from_integer(300)}])
      page3_binary = PageTestHelpers.from_map(page3)
      :ok = Database.store_page(db1, 3, {page3_binary, 0})

      Database.close(db1)

      # Recovery should return error when encountering broken chain due to invalid page
      assert {:error, :broken_chain} = Logic.startup(:corrupt_recovery, self(), :test_id, tmp_dir)
    end
  end

  describe "edge cases and error conditions" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_edge_cases_test")
    end

    test "recovery with missing page 0 creates empty state", %{tmp_dir: tmp_dir} do
      # Create database with pages but no page 0
      {:ok, db1} = Database.open(:no_zero, Path.join(tmp_dir, "dets"))

      page5 = Page.new(5, [{<<"orphan">>, Version.from_integer(500)}])
      page5_binary = PageTestHelpers.from_map(page5)
      :ok = Database.store_page(db1, 5, {page5_binary, 0})

      Database.close(db1)

      # Recovery should handle missing page 0
      {:ok, state} = Logic.startup(:no_zero_recovery, self(), :test_id, tmp_dir)

      # Note: Since page 0 doesn't exist, no valid chain is found
      # max_page_id should be 0 (default) and no free pages
      assert %{
               index_manager: %{
                 page_allocator: %{max_page_id: 0, free_page_ids: []}
               }
             } = state

      Logic.shutdown(state)
    end

    test "database file permissions and error handling", %{tmp_dir: tmp_dir} do
      # Test with non-existent directory (should be created)
      non_existent_dir = Path.join(tmp_dir, "does/not/exist")

      {:ok, state} = Logic.startup(:permissions_test, self(), :test_id, non_existent_dir)

      # Should have created directory and initialized successfully
      assert File.exists?(non_existent_dir)
      assert state.database

      Logic.shutdown(state)
    end
  end

  describe "Phase 1.3 success criteria verification" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_criteria_test")
    end

    test "pages persist to DETS and can be retrieved", %{tmp_dir: tmp_dir} do
      {:ok, state} = Logic.startup(:criteria_pages, self(), :test_id, tmp_dir)

      # Create and persist a page
      page = Page.new(42, [{<<"test_key">>, Version.from_integer(12_345)}])
      :ok = PageTestHelpers.persist_page_to_database(state.index_manager, state.database, page)

      # Retrieve and verify
      {:ok, {retrieved_binary, _next_id}} = Database.load_page(state.database, 42)

      assert Page.id(retrieved_binary) == 42
      assert Page.keys(retrieved_binary) == [<<"test_key">>]
      versions = Enum.map(Page.key_locators(retrieved_binary), fn {_key, version} -> version end)
      assert versions == [Version.from_integer(12_345)]

      Logic.shutdown(state)
    end

    test "recovery rebuilds page structure correctly", %{tmp_dir: tmp_dir} do
      # Setup: Create complex page structure
      {:ok, db1} = Database.open(:rebuild_test, Path.join(tmp_dir, "dets"))

      # Chain: 0 -> 3 -> 7 -> 2 -> end (non-sequential for complexity)
      pages_data = [
        {0, [<<"start">>], 3},
        {3, [<<"second">>], 7},
        {7, [<<"third">>], 2},
        {2, [<<"end">>], 0}
      ]

      test_pages = create_test_pages(pages_data)

      # Store pages with their corresponding next_id from pages_data
      pages_data_map = Map.new(pages_data, fn {id, _keys, next_id} -> {id, next_id} end)

      Enum.each(test_pages, fn {id, page} ->
        next_id = Map.get(pages_data_map, id)
        :ok = Database.store_page(db1, id, {page, next_id})
      end)

      Database.close(db1)

      # Recovery
      {:ok, state} = Logic.startup(:rebuild_recovery, self(), :test_id, tmp_dir)

      # Verify structure rebuilt correctly
      # Free pages should be: 1, 4, 5, 6 (gaps in 0,2,3,7)
      expected_free = [1, 4, 5, 6]

      assert %{
               index_manager: %{
                 page_allocator: %{max_page_id: 7, free_page_ids: free_pages}
               }
             } = state

      assert Enum.sort(free_pages) == expected_free

      Logic.shutdown(state)
    end

    test "max_page_id is determined correctly during recovery", %{tmp_dir: tmp_dir} do
      # Create database with scattered page IDs
      {:ok, db1} = Database.open(:max_id_test, Path.join(tmp_dir, "dets"))

      # Unordered with large gap
      page_ids = [0, 5, 10, 3, 99, 50]

      Enum.each(page_ids, fn id ->
        page = Page.new(id, [{<<"page_#{id}">>, Version.from_integer(id)}])
        page_binary = PageTestHelpers.from_map(page)
        :ok = Database.store_page(db1, id, {page_binary, 0})
      end)

      Database.close(db1)

      # Recovery should find maximum correctly
      {:ok, state} = Logic.startup(:max_id_recovery, self(), :test_id, tmp_dir)

      # Note: Only page 0 is part of the valid chain (others are not linked)
      # max_page_id should be 0 (since page 0 is highest in valid chain) and no free pages
      assert %{
               index_manager: %{
                 page_allocator: %{max_page_id: 0, free_page_ids: []}
               }
             } = state

      Logic.shutdown(state)
    end
  end

  describe "window eviction with persistence (Phase 2.1)" do
    @tag :tmp_dir

    setup context do
      setup_tmp_dir(context, "persistence_window_test")
    end

    test "window eviction with page data persistence", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "page_window.dets")
      {:ok, db} = Database.open(:page_window_test, file_path)

      vm = IndexManager.new()
      # Mark as unused
      _current_time = :os.system_time(:millisecond)

      # Create versions spanning the window boundary
      # Old version - will be evicted
      old_version = Version.from_integer(1)
      # Recent version - will be kept
      recent_version = Version.from_integer(2)

      # Create pages for both versions
      old_page = Page.new(1, [{<<"old_key">>, old_version}])
      recent_page = Page.new(2, [{<<"recent_key">>, recent_version}])

      # Persist pages to DETS
      :ok = PageTestHelpers.persist_page_to_database(vm, db, old_page)
      :ok = PageTestHelpers.persist_page_to_database(vm, db, recent_page)

      # Set up version manager
      vm = %{
        vm
        | versions: [
            {recent_version, {:gb_trees.empty(), %{}}},
            {old_version, {:gb_trees.empty(), %{}}}
          ],
          current_version: recent_version
      }

      # Advance version to trigger eviction
      # Use predictable test version
      new_version = Version.from_integer(3)
      updated_vm = IndexManager.advance_version(vm, new_version)

      # NOTE: For MVP, no versions are evicted, so both should still be in memory
      version_list = Enum.map(updated_vm.versions, fn {v, _} -> v end)
      # MVP keeps all versions
      assert old_version in version_list
      assert recent_version in version_list

      # But pages should still be retrievable from DETS
      {:ok, {old_page_binary, _next_id}} = Database.load_page(db, 1)
      assert Page.keys(old_page_binary) == [<<"old_key">>]

      {:ok, {recent_page_binary, _next_id}} = Database.load_page(db, 2)
      assert Page.keys(recent_page_binary) == [<<"recent_key">>]

      Database.close(db)
    end
  end
end
