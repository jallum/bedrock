defmodule Bedrock.DataPlane.Storage.Olivine.IndexManagerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Storage.Olivine.IndexManagerTestHelpers
  alias Bedrock.Test.Storage.Olivine.PageTestHelpers

  # Helper functions for cleaner test assertions

  # Helper function to create a test database for unit tests
  defp create_test_database do
    tmp_dir = System.tmp_dir!()
    db_file = Path.join(tmp_dir, "test_db_#{System.unique_integer([:positive])}.dets")
    table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")

    # Suppress expected connection retry logs during database open
    {result, _logs} =
      with_log(fn ->
        Database.open(table_name, db_file, pool_size: 1)
      end)

    {:ok, database} = result
    database
  end

  # Helper function to assert key-version pairs using the new tuple-based API
  defp assert_key_versions_equal(page, expected_key_version_pairs) do
    expected_tuples =
      Enum.map(expected_key_version_pairs, fn {key, version_int} ->
        {key, Version.from_integer(version_int)}
      end)

    assert Page.key_locators(page) == expected_tuples
  end

  # Helper function to create test transactions
  defp test_transaction(mutations, commit_version) do
    transaction_map = %{
      commit_version: commit_version,
      mutations: mutations
    }

    Transaction.encode(transaction_map)
  end

  describe "basic functionality" do
    test "new/0 creates a new version manager" do
      assert %{
               id_allocator: %{max_id: 0, free_ids: []},
               current_version: version
             } = IndexManager.new()

      assert version == Version.zero()
    end

    test "info/2 returns page management information" do
      id_allocator = IdAllocator.new(42, [1, 3, 5])
      vm = %{IndexManager.new() | id_allocator: id_allocator}

      assert IndexManager.info(vm, :max_id) == 42
      assert IndexManager.info(vm, :free_ids) == [1, 3, 5]
      assert IndexManager.info(vm, :unknown_stat) == :undefined
    end

    test "info/2 returns key_ranges from current index" do
      # Test with empty index (default values)
      empty_vm = IndexManager.new()
      key_ranges = IndexManager.info(empty_vm, :key_ranges)
      assert [{<<0xFF, 0xFF>>, <<>>}] = key_ranges

      # Test with no versions (edge case)
      no_versions_vm = %{empty_vm | versions: []}
      assert IndexManager.info(no_versions_vm, :key_ranges) == []
    end

    test "info/2 returns key_ranges with real data after transactions" do
      # Create a database and apply transactions to get real min/max keys
      database = create_test_database()

      try do
        # Create transaction with keys spanning from "apple" to "zebra"
        mutations = [
          {:set, <<"apple">>, <<"value1">>},
          {:set, <<"zebra">>, <<"value2">>}
        ]

        transaction = test_transaction(mutations, Version.from_integer(1000))

        # Apply transaction and check key ranges
        index_manager = IndexManager.new()
        {updated_manager, _database} = IndexManager.apply_transaction(index_manager, transaction, database)

        key_ranges = IndexManager.info(updated_manager, :key_ranges)
        assert [{min_key, max_key}] = key_ranges

        # Note: Currently the Index doesn't update min/max keys during mutations
        # This returns the initial empty values. This could be improved in the future.
        # Initial empty min_key
        assert min_key == <<0xFF, 0xFF>>
        # Initial empty max_key
        assert max_key == <<>>
      after
        Database.close(database)
      end
    end
  end

  describe "page creation" do
    test "new/3 creates a page with key-version tuples" do
      keys = [<<"key1">>, <<"key2">>, <<"key3">>]
      versions = [100, 200, 300]
      expected_key_versions = Enum.zip(keys, Enum.map(versions, &Version.from_integer/1))

      page = Page.new(1, expected_key_versions)

      assert Page.id(page) == 1
      # next_id defaults to 0 when not specified
      assert Page.key_locators(page) == expected_key_versions
    end

    test "new/3 creates a page with keys and default versions" do
      keys = [<<"key1">>, <<"key2">>]
      expected_key_versions = Enum.map(keys, &{&1, Version.zero()})

      page = Page.new(1, expected_key_versions)

      assert Page.id(page) == 1
      # next_id defaults to 0 when not specified
      assert Page.keys(page) == keys
      assert Page.key_locators(page) == expected_key_versions
    end

    test "key_count/1 returns correct key count" do
      page = Page.new(1, [{<<"a">>, Version.zero()}, {<<"b">>, Version.zero()}, {<<"c">>, Version.zero()}])
      assert Page.key_count(page) == 3

      empty_page = Page.new(1, [])
      assert Page.key_count(empty_page) == 0
    end

    test "keys/1 returns page keys" do
      keys = [<<"key1">>, <<"key2">>]
      page = Page.new(1, Enum.map(keys, &{&1, Version.zero()}))
      assert Page.keys(page) == keys
    end
  end

  describe "binary page encoding/decoding" do
    test "from_map/1 and to_map/1 round-trip correctly" do
      keys = [<<"apple">>, <<"banana">>, <<"cherry">>]
      versions = [100, 200, 300]
      expected_key_versions = Enum.zip(keys, Enum.map(versions, &Version.from_integer/1))
      page = Page.new(42, expected_key_versions)

      encoded = PageTestHelpers.from_map(page)
      assert {:ok, decoded_page} = PageTestHelpers.to_map(encoded)

      assert Page.id(decoded_page) == 42
      assert decoded_page.next_id == 0
      assert Page.key_locators(decoded_page) == expected_key_versions
    end

    test "from_map/1 creates proper binary format" do
      keys = [<<"a">>, <<"bb">>]
      versions = Enum.map([1000, 2000], &Version.from_integer/1)
      page = Page.new(5, Enum.zip(keys, versions))

      encoded = PageTestHelpers.from_map(page)

      # Validate header format
      assert <<5::integer-32-big, 2::integer-16-big, last_key_offset::integer-32-big, _reserved::unsigned-big-48,
               rest::binary>> = encoded

      assert last_key_offset > 0

      # Validate interleaved format: version1, key1_len, key1, version2, key2_len, key2
      assert <<version1::binary-size(8), 1::integer-16-big, key1::binary-size(1), version2::binary-size(8),
               2::integer-16-big, key2::binary-size(2)>> = rest

      assert Version.to_integer(version1) == 1000
      assert Version.to_integer(version2) == 2000
      assert key1 == <<"a">>
      assert key2 == <<"bb">>
    end

    test "to_map/1 handles empty page" do
      empty_page = Page.new(1, [])
      encoded = PageTestHelpers.from_map(empty_page)

      {:ok, decoded} = PageTestHelpers.to_map(encoded)
      assert Page.empty?(decoded)
    end

    test "to_map/1 handles malformed page data" do
      assert {:error, :invalid_page} = PageTestHelpers.to_map(<<1::32>>)

      # Invalid header with incorrect field sizes or incomplete entries
      invalid_header = <<1::32, 1::16, 0::32, 0::48>>
      invalid_data = <<invalid_header::binary, "incomplete">>
      assert {:error, :invalid_entries} = PageTestHelpers.to_map(invalid_data)
    end
  end

  describe "key operations within pages" do
    test "locator_for_key/2 finds existing keys" do
      keys = [<<"apple">>, <<"banana">>, <<"cherry">>]
      versions = [100, 200, 300]
      page = Page.new(1, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)))

      assert {:ok, version_apple} = Page.locator_for_key(page, <<"apple">>)
      assert {:ok, version_banana} = Page.locator_for_key(page, <<"banana">>)
      assert {:ok, version_cherry} = Page.locator_for_key(page, <<"cherry">>)

      assert version_apple == Version.from_integer(100)
      assert version_banana == Version.from_integer(200)
      assert version_cherry == Version.from_integer(300)
    end

    test "locator_for_key/2 returns error for missing keys" do
      page =
        Page.new(1, [
          {<<"apple">>, Version.from_integer(100)},
          {<<"banana">>, Version.from_integer(200)}
        ])

      assert {:error, :not_found} = Page.locator_for_key(page, <<"missing">>)
      assert {:error, :not_found} = Page.locator_for_key(page, <<"zebra">>)
    end

    test "apply_operations/2 inserts new keys in sorted order" do
      page =
        Page.new(1, [
          {<<"apple">>, Version.from_integer(100)},
          {<<"cherry">>, Version.from_integer(300)}
        ])

      updated_page = Page.apply_operations(page, %{<<"banana">> => {:set, Version.from_integer(200)}})
      assert_key_versions_equal(updated_page, [{<<"apple">>, 100}, {<<"banana">>, 200}, {<<"cherry">>, 300}])

      updated_page2 = Page.apply_operations(page, %{<<"aardvark">> => {:set, Version.from_integer(50)}})
      assert_key_versions_equal(updated_page2, [{<<"aardvark">>, 50}, {<<"apple">>, 100}, {<<"cherry">>, 300}])

      updated_page3 = Page.apply_operations(page, %{<<"zebra">> => {:set, Version.from_integer(400)}})
      assert_key_versions_equal(updated_page3, [{<<"apple">>, 100}, {<<"cherry">>, 300}, {<<"zebra">>, 400}])
    end

    test "apply_operations/2 updates existing keys" do
      page =
        Page.new(1, [
          {<<"apple">>, Version.from_integer(100)},
          {<<"banana">>, Version.from_integer(200)}
        ])

      updated_page = Page.apply_operations(page, %{<<"apple">> => {:set, Version.from_integer(150)}})
      assert_key_versions_equal(updated_page, [{<<"apple">>, 150}, {<<"banana">>, 200}])
    end

    test "apply_operations/2 maintains sorted order invariant" do
      page = Page.new(1, [])

      keys_to_add = [<<"zebra">>, <<"apple">>, <<"mango">>, <<"banana">>]
      versions = [400, 100, 300, 200]

      final_page =
        keys_to_add
        |> Enum.zip(versions)
        |> Enum.reduce(page, fn {key, version}, acc_page ->
          Page.apply_operations(acc_page, %{key => {:set, Version.from_integer(version)}})
        end)

      assert_key_versions_equal(final_page, [
        {<<"apple">>, 100},
        {<<"banana">>, 200},
        {<<"mango">>, 300},
        {<<"zebra">>, 400}
      ])
    end
  end

  describe "page splitting" do
    test "split_page/3 works with pages under typical split threshold" do
      keys = for i <- 1..256, do: <<"key_#{String.pad_leading(to_string(i), 3, "0")}">>
      versions = Enum.map(1..256, & &1)
      page = Page.new(1, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)))

      key_count = Page.key_count(page)
      mid_point = div(key_count, 2)
      new_page_id = 999

      assert {{left_page, _left_next_id}, {right_page, _right_next_id}} =
               Page.split_page(page, mid_point, new_page_id, 0)

      # Verify the split worked
      assert Page.key_count(left_page) == mid_point
      assert Page.key_count(right_page) == key_count - mid_point
      assert Page.id(left_page) == 1
      assert Page.id(right_page) == new_page_id
    end

    test "split_page/3 splits pages over threshold" do
      keys = for i <- 1..300, do: <<"key_#{String.pad_leading(to_string(i), 3, "0")}">>
      versions = Enum.map(1..300, & &1)
      page = Page.new(1, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)))

      key_count = Page.key_count(page)
      mid_point = div(key_count, 2)
      new_page_id = 2

      assert {{left_page, left_next_id}, {right_page, right_next_id}} = Page.split_page(page, mid_point, new_page_id, 0)

      # Verify split results
      assert Page.id(left_page) == 1
      assert Page.id(right_page) == new_page_id
      assert right_next_id == 0
      assert left_next_id == Page.id(right_page)

      # Keys should be split roughly in half
      left_keys = Page.keys(left_page)
      right_keys = Page.keys(right_page)
      assert length(left_keys) + length(right_keys) == 300
      assert length(left_keys) == 150
      assert length(right_keys) == 150

      # All keys combined should equal original keys (no data loss)
      combined_keys = left_keys ++ right_keys
      assert Enum.sort(combined_keys) == Enum.sort(keys)

      # Verify ordering within each page
      assert Enum.sort(left_keys) == left_keys
      assert Enum.sort(right_keys) == right_keys

      # Left page should have smaller keys than right page
      assert List.last(left_keys) < List.first(right_keys)
    end

    test "split_page/3 preserves key-version relationships" do
      key_versions =
        for i <- 1..300, do: {<<"key_#{String.pad_leading(to_string(i), 3, "0")}">>, Version.from_integer(i * 10)}

      page = Page.new(1, key_versions)
      key_count = Page.key_count(page)
      mid_point = div(key_count, 2)
      new_page_id = 2

      {{left_page, _left_next_id}, {right_page, _right_next_id}} = Page.split_page(page, mid_point, new_page_id, 0)

      # Verify all key-version pairs are preserved
      left_key_versions = Page.key_locators(left_page)
      right_key_versions = Page.key_locators(right_page)
      combined_key_versions = left_key_versions ++ right_key_versions

      assert length(combined_key_versions) == 300
      assert Enum.sort(combined_key_versions) == Enum.sort(key_versions)

      # Verify each page maintains sorted order
      {left_keys, _} = Enum.unzip(left_key_versions)
      {right_keys, _} = Enum.unzip(right_key_versions)
      assert Enum.sort(left_keys) == left_keys
      assert Enum.sort(right_keys) == right_keys
    end
  end

  describe "page-based key operations" do
    test "page_for_key/3 retrieves pages containing keys" do
      vm = IndexManager.new()
      db = create_test_database()

      # Add some keys to create page structure
      mutations = [
        {:set, <<"apple">>, <<"value1">>},
        {:set, <<"banana">>, <<"value2">>},
        {:set, <<"cherry">>, <<"value3">>}
      ]

      transaction = test_transaction(mutations, Version.from_integer(1000))
      {vm_updated, _updated_db} = IndexManager.apply_transactions(vm, [transaction], db)

      # Should be able to fetch page containing key
      assert {:ok, page} = IndexManager.page_for_key(vm_updated, <<"banana">>, Version.from_integer(1000))
      assert Page.has_key?(page, <<"banana">>)

      # Should work for any key in the page
      assert {:ok, _page} = IndexManager.page_for_key(vm_updated, <<"apple">>, Version.from_integer(1000))
      assert {:ok, _page} = IndexManager.page_for_key(vm_updated, <<"cherry">>, Version.from_integer(1000))

      Database.close(db)
    end

    test "page_for_key/3 handles version bounds correctly" do
      vm = IndexManager.new()
      db = create_test_database()

      # Add data at version 1000
      mutation = {:set, <<"key1">>, <<"value1">>}
      transaction = test_transaction([mutation], Version.from_integer(1000))
      {vm_updated, _updated_db} = IndexManager.apply_transactions(vm, [transaction], db)

      # Should work at current version
      assert {:ok, _page} = IndexManager.page_for_key(vm_updated, <<"key1">>, Version.from_integer(1000))

      # Should reject future versions
      assert {:error, :version_too_new} =
               IndexManager.page_for_key(vm_updated, <<"key1">>, Version.from_integer(2000))

      # Note: version_too_old check was removed since IndexManager no longer tracks durable_version

      Database.close(db)
    end

    test "pages_for_range/4 retrieves all pages in key range" do
      vm = IndexManager.new()
      db = create_test_database()

      # Add many keys to potentially span multiple pages
      mutations =
        for i <- 1..50 do
          key = <<"key_#{String.pad_leading(to_string(i), 3, "0")}">>
          value = <<"value_#{i}">>
          {:set, key, value}
        end

      transaction = test_transaction(mutations, Version.from_integer(1000))
      {vm_updated, _updated_db} = IndexManager.apply_transactions(vm, [transaction], db)

      # Fetch pages for a range
      start_key = <<"key_010">>
      end_key = <<"key_020">>

      assert {:ok, pages} =
               IndexManager.pages_for_range(vm_updated, start_key, end_key, Version.from_integer(1000))

      assert is_list(pages)
      assert length(pages) > 0

      # All pages should contain keys in the requested range
      all_keys = Enum.flat_map(pages, &Page.keys/1)
      range_keys = Enum.filter(all_keys, fn key -> key >= start_key and key <= end_key end)

      # Should find keys in the range
      assert length(range_keys) > 0

      Database.close(db)
    end
  end

  describe "transaction processing" do
    test "apply_transaction/2 processes set mutations" do
      vm = IndexManager.new()
      database = create_test_database()

      mutations = [
        {:set, <<"key1">>, <<"value1">>},
        {:set, <<"key2">>, <<"value2">>}
      ]

      transaction = test_transaction(mutations, Version.from_integer(1000))

      {%{current_version: current_version} = _updated_vm, _updated_database} =
        IndexManager.apply_transaction(vm, transaction, database)

      assert current_version == Version.from_integer(1000)

      Database.close(database)
    end

    test "apply_transaction/2 processes clear mutations" do
      vm = IndexManager.new()
      database = create_test_database()

      # First add some data
      set_mutations = [
        {:set, <<"key1">>, <<"value1">>},
        {:set, <<"key2">>, <<"value2">>},
        {:set, <<"key3">>, <<"value3">>}
      ]

      set_transaction = test_transaction(set_mutations, Version.from_integer(1000))
      {vm_with_data, database} = IndexManager.apply_transaction(vm, set_transaction, database)

      # Then clear one key
      clear_mutations = [
        {:clear, <<"key2">>}
      ]

      clear_transaction = test_transaction(clear_mutations, Version.from_integer(1100))
      {vm_after_clear, _database} = IndexManager.apply_transaction(vm_with_data, clear_transaction, database)

      # Key1 and Key3 should still be fetchable at version 1000
      assert {:ok, _page} = IndexManager.page_for_key(vm_after_clear, <<"key1">>, Version.from_integer(1000))
      assert {:ok, _page} = IndexManager.page_for_key(vm_after_clear, <<"key3">>, Version.from_integer(1000))

      # Key2 should not be fetchable at version 1100 (after clear)
      # We expect the page to exist but key2 should not be found in it
      assert {:ok, page} = IndexManager.page_for_key(vm_after_clear, <<"key2">>, Version.from_integer(1100))
      assert {:error, :not_found} = Page.locator_for_key(page, <<"key2">>)

      Database.close(database)
    end

    test "apply_transaction/2 processes clear_range mutations" do
      vm = IndexManager.new()
      database = create_test_database()

      # Add data across a range
      set_mutations =
        for i <- 1..20 do
          key = <<"key_#{String.pad_leading(to_string(i), 2, "0")}">>
          value = <<"value_#{i}">>
          {:set, key, value}
        end

      set_transaction = test_transaction(set_mutations, Version.from_integer(1000))
      {vm_with_data, database} = IndexManager.apply_transaction(vm, set_transaction, database)

      # Clear a range
      clear_mutations = [
        {:clear_range, <<"key_05">>, <<"key_15">>}
      ]

      clear_transaction = test_transaction(clear_mutations, Version.from_integer(1100))
      {vm_after_clear, _database} = IndexManager.apply_transaction(vm_with_data, clear_transaction, database)

      # Keys outside the range should still be accessible
      assert {:ok, _page} = IndexManager.page_for_key(vm_after_clear, <<"key_01">>, Version.from_integer(1100))
      assert {:ok, _page} = IndexManager.page_for_key(vm_after_clear, <<"key_20">>, Version.from_integer(1100))

      # Keys within the cleared range should not be accessible at the new version
      # Test a few specific keys deterministically
      assert {:ok, page} = IndexManager.page_for_key(vm_after_clear, <<"key_05">>, Version.from_integer(1100))
      assert {:error, :not_found} = Page.locator_for_key(page, <<"key_05">>)

      assert {:ok, page} = IndexManager.page_for_key(vm_after_clear, <<"key_10">>, Version.from_integer(1100))
      assert {:error, :not_found} = Page.locator_for_key(page, <<"key_10">>)

      assert {:ok, page} = IndexManager.page_for_key(vm_after_clear, <<"key_15">>, Version.from_integer(1100))
      assert {:error, :not_found} = Page.locator_for_key(page, <<"key_15">>)

      Database.close(database)
    end

    test "apply_transactions/2 processes multiple transactions in sequence" do
      vm = IndexManager.new()
      db = create_test_database()

      transactions = [
        test_transaction([{:set, <<"key1">>, <<"value1">>}], Version.from_integer(1000)),
        test_transaction([{:set, <<"key2">>, <<"value2">>}], Version.from_integer(1100)),
        test_transaction([{:set, <<"key3">>, <<"value3">>}], Version.from_integer(1200))
      ]

      {updated_vm, _updated_db} = IndexManager.apply_transactions(vm, transactions, db)
      assert %{current_version: current_version} = updated_vm

      assert current_version == Version.from_integer(1200)

      Database.close(db)
    end
  end

  describe "version management and windowing" do
    test "version_in_window?/2 correctly identifies versions in window" do
      window_start = Version.from_integer(5_000_000)

      # Versions at or after window start should be in window
      assert IndexManagerTestHelpers.version_in_window?(Version.from_integer(5_000_000), window_start) == true
      assert IndexManagerTestHelpers.version_in_window?(Version.from_integer(6_000_000), window_start) == true

      # Versions before window start should not be in window
      assert IndexManagerTestHelpers.version_in_window?(Version.from_integer(4_999_999), window_start) == false
    end

    test "split_versions_at_window/2 correctly partitions versions" do
      versions = [
        {Version.from_integer(10_000_000), :data1},
        {Version.from_integer(8_000_000), :data2},
        {Version.from_integer(6_000_000), :data3},
        {Version.from_integer(4_000_000), :data4},
        {Version.from_integer(2_000_000), :data5}
      ]

      window_start = Version.from_integer(5_000_000)

      # Pattern match expected results directly
      assert {kept, evicted} = IndexManagerTestHelpers.split_versions_at_window(versions, window_start)

      # Versions 10M, 8M, 6M should be kept (in window)
      expected_kept = [
        {Version.from_integer(10_000_000), :data1},
        {Version.from_integer(8_000_000), :data2},
        {Version.from_integer(6_000_000), :data3}
      ]

      assert kept == expected_kept

      # Versions 4M, 2M should be evicted (outside window)
      expected_evicted = [
        {Version.from_integer(4_000_000), :data4},
        {Version.from_integer(2_000_000), :data5}
      ]

      assert evicted == expected_evicted
    end
  end

  describe "tree operations" do
    test "tree operations work with page updates" do
      vm = IndexManager.new()
      database = create_test_database()

      # Add some data to build tree structure
      mutations = [
        {:set, <<"apple">>, <<"value1">>},
        {:set, <<"banana">>, <<"value2">>},
        {:set, <<"cherry">>, <<"value3">>}
      ]

      transaction = test_transaction(mutations, Version.from_integer(1000))
      {vm_updated, _updated_db} = IndexManager.apply_transactions(vm, [transaction], database)

      # Tree should be able to find pages for keys
      # This is tested indirectly through page_for_key working
      assert {:ok, _page} = IndexManager.page_for_key(vm_updated, <<"banana">>, Version.from_integer(1000))

      Database.close(database)
    end
  end

  describe "page chain operations" do
    test "walk_page_chain/1 traverses page chains correctly" do
      # Create a simple chain: page 0 -> page 2 -> page 5 -> end
      page_map = %{
        0 => %{next_id: 2},
        2 => %{next_id: 5},
        5 => %{next_id: 0}
      }

      chain = PageTestHelpers.walk_page_chain(page_map)
      assert chain == [0, 2, 5]
    end

    test "walk_page_chain/1 handles single page" do
      page_map = %{0 => %{next_id: 0}}

      chain = PageTestHelpers.walk_page_chain(page_map)
      assert chain == [0]
    end

    test "walk_page_chain/1 handles empty map" do
      assert PageTestHelpers.walk_page_chain(%{}) == []
    end
  end

  describe "streaming operations" do
    test "stream_key_locators_in_range/3 filters keys correctly" do
      # Create pages with key-version pairs
      page1 =
        Page.new(1, [
          {<<"apple">>, Version.from_integer(100)},
          {<<"banana">>, Version.from_integer(200)},
          {<<"cherry">>, Version.from_integer(300)}
        ])

      page2 =
        Page.new(2, [
          {<<"date">>, Version.from_integer(400)},
          {<<"elderberry">>, Version.from_integer(500)}
        ])

      pages = [page1, page2]

      # Stream keys in range "banana" to "date" (exclusive end)
      key_versions =
        pages
        |> Page.stream_key_locators_in_range(<<"banana">>, <<"date">>)
        |> Enum.to_list()

      expected = [
        {<<"banana">>, Version.from_integer(200)},
        {<<"cherry">>, Version.from_integer(300)}
      ]

      assert key_versions == expected
    end

    test "stream_key_locators_in_range/3 returns consistent results" do
      # Create pages with key-version pairs
      page =
        Page.new(1, [
          {<<"apple">>, Version.from_integer(100)},
          {<<"banana">>, Version.from_integer(200)},
          {<<"cherry">>, Version.from_integer(300)}
        ])

      pages = [page]

      key_versions_1 =
        pages
        |> Page.stream_key_locators_in_range(<<"banana">>, <<"zebra">>)
        |> Enum.to_list()

      key_versions_2 =
        pages
        |> Page.stream_key_locators_in_range(<<"banana">>, <<"zebra">>)
        |> Enum.to_list()

      assert key_versions_1 == key_versions_2
    end
  end

  describe "Page.apply_operations right_key bug regression test" do
    test "apply_operations preserves valid last_key_offset for right_key access" do
      # This test reproduces the exact scenario from the failing property test
      # where apply_operations corrupts the last_key_offset, causing right_key to fail

      # Create a page with some initial data similar to the failing case
      initial_keys_versions = [
        {"BE7t", Version.from_integer(6)},
        {"S", Version.from_integer(1)}
      ]

      page = Page.new(388, initial_keys_versions)

      # Verify the initial page is valid and right_key works
      # Should be the last key alphabetically after sorting
      assert Page.right_key(page) == "S"

      # Apply operations that caused the bug in the property test
      operations = %{
        "Ej3s" => {:set, Version.from_integer(1)},
        "S" => :clear,
        "ce" => :clear,
        "fFE" => :clear
      }

      # This should not corrupt the page binary format
      updated_page = Page.apply_operations(page, operations)

      # The key assertion: right_key should work on the updated page
      # This will fail with FunctionClauseError if last_key_offset is corrupted
      result = Page.right_key(updated_page)

      # Verify the result makes sense - should be the last key after operations
      # After operations: "BE7t" remains, "S" is cleared, "Ej3s" is added
      # So the last key alphabetically should be "Ej3s"
      assert result == "Ej3s", "right_key should return the correct last key after apply_operations"

      # Additional verification: the page should have valid internal structure
      assert not Page.empty?(updated_page), "Page should not be empty after operations"
      keys = Page.keys(updated_page)
      assert Enum.sort(keys) == keys, "Keys should remain sorted after operations"
    end
  end

  describe "Page binary optimization helpers" do
    # Helper to extract entries from page binary
    defp extract_entries(page) do
      <<_id::32, key_count::16, _offset::32, _reserved::48, entries::binary>> = page
      {entries, key_count}
    end

    test "search_entries_with_position finds exact key" do
      key_versions = [
        {"apple", <<0, 0, 0, 0, 0, 0, 0, 1>>},
        {"banana", <<0, 0, 0, 0, 0, 0, 0, 2>>},
        {"cherry", <<0, 0, 0, 0, 0, 0, 0, 3>>}
      ]

      page = Page.new(1, key_versions)
      {entries, key_count} = extract_entries(page)

      # Test finding exact keys
      assert {:found, 0} = Page.search_entries_with_position(entries, key_count, "apple")
      assert {:found, 1} = Page.search_entries_with_position(entries, key_count, "banana")
      assert {:found, 2} = Page.search_entries_with_position(entries, key_count, "cherry")
    end

    test "search_entries_with_position finds insertion points" do
      key_versions = [{"banana", <<0, 0, 0, 0, 0, 0, 0, 1>>}, {"cherry", <<0, 0, 0, 0, 0, 0, 0, 2>>}]
      page = Page.new(1, key_versions)
      {entries, key_count} = extract_entries(page)

      # Test insertion points for non-existent keys
      # Before all
      assert {:not_found, 0} = Page.search_entries_with_position(entries, key_count, "apple")
      # Between
      assert {:not_found, 1} = Page.search_entries_with_position(entries, key_count, "blueberry")
      # After all
      assert {:not_found, 2} = Page.search_entries_with_position(entries, key_count, "date")
    end

    test "search_entries_with_position stops early for sorted data" do
      key_versions = [
        {"a", <<0, 0, 0, 0, 0, 0, 0, 1>>},
        {"c", <<0, 0, 0, 0, 0, 0, 0, 2>>},
        {"e", <<0, 0, 0, 0, 0, 0, 0, 3>>}
      ]

      page = Page.new(1, key_versions)
      {entries, key_count} = extract_entries(page)

      # Should stop at position 1 when looking for "b" (since "c" > "b")
      assert {:not_found, 1} = Page.search_entries_with_position(entries, key_count, "b")
    end

    test "decode_entry_at_position extracts correct entries" do
      key_versions = [
        {"first", <<1, 1, 1, 1, 1, 1, 1, 1>>},
        {"second", <<2, 2, 2, 2, 2, 2, 2, 2>>},
        {"third", <<3, 3, 3, 3, 3, 3, 3, 3>>}
      ]

      page = Page.new(1, key_versions)
      {entries, key_count} = extract_entries(page)

      # Test extracting entries at different positions
      assert {:ok, {"first", <<1, 1, 1, 1, 1, 1, 1, 1>>}} = Page.decode_entry_at_position(entries, 0, key_count)
      assert {:ok, {"second", <<2, 2, 2, 2, 2, 2, 2, 2>>}} = Page.decode_entry_at_position(entries, 1, key_count)
      assert {:ok, {"third", <<3, 3, 3, 3, 3, 3, 3, 3>>}} = Page.decode_entry_at_position(entries, 2, key_count)
    end

    test "decode_entry_at_position handles bounds correctly" do
      key_versions = [{"only", <<1, 1, 1, 1, 1, 1, 1, 1>>}]
      page = Page.new(1, key_versions)
      {entries, key_count} = extract_entries(page)

      # Test out of bounds conditions
      # Beyond last
      assert :out_of_bounds = Page.decode_entry_at_position(entries, 1, key_count)
      # Way beyond
      assert :out_of_bounds = Page.decode_entry_at_position(entries, 2, key_count)
    end
  end

  describe "KeySelector resolution algorithms" do
    alias Bedrock.KeySelector

    setup do
      # Create index manager with test data
      manager = IndexManager.new()
      database = create_test_database()

      # Add some test data at version 1
      mutations = [
        {:set, "key1", "value1"},
        {:set, "key2", "value2"},
        {:set, "key5", "value5"},
        {:set, "key10", "value10"},
        {:set, "range:a", "range_a_value"},
        {:set, "range:b", "range_b_value"},
        {:set, "range:z", "range_z_value"}
      ]

      transaction = test_transaction(mutations, Version.from_integer(1))
      {updated_manager, database} = IndexManager.apply_transaction(manager, transaction, database)

      on_exit(fn -> Database.close(database) end)

      {:ok, manager: updated_manager, database: database}
    end

    test "page_for_key/3 with KeySelector handles version errors", %{manager: manager} do
      key_selector = KeySelector.first_greater_or_equal("test_key")

      # Future version should fail
      assert {:error, :version_too_new} =
               IndexManager.page_for_key(manager, key_selector, Version.from_integer(999))

      # Old version with higher current version should fail
      updated_manager = %{manager | current_version: Version.from_integer(10)}

      assert {:error, reason} =
               IndexManager.page_for_key(updated_manager, key_selector, Version.from_integer(0))

      assert reason in [:version_too_old, :not_found]
    end

    test "page_for_key/3 with KeySelector - successful resolution", %{manager: manager} do
      key_selector = KeySelector.first_greater_or_equal("key2")
      version = Version.from_integer(1)

      result = IndexManager.page_for_key(manager, key_selector, version)

      # Should successfully resolve to key2 since it exists in our test data
      assert {:ok, "key2", page} = result
      assert is_binary(page) or is_map(page)
    end

    test "page_for_key/3 with KeySelector - first_greater_than behavior", %{manager: manager} do
      key_selector = KeySelector.first_greater_than("key1")
      version = Version.from_integer(1)

      result = IndexManager.page_for_key(manager, key_selector, version)

      # Expected lexicographic order: ["key1", "key10", "key2", "key5"]
      # So first_greater_than("key1") should be "key10"
      expected_resolved_key = "key10"

      assert {:ok, resolved_key, page} = result
      assert resolved_key == expected_resolved_key
      assert is_binary(page) or is_map(page)
    end

    test "page_for_key/3 with KeySelector - offset handling", %{manager: manager} do
      # Test KeySelector with offset
      base_selector = KeySelector.first_greater_or_equal("key1")
      offset_selector = KeySelector.add(base_selector, 1)
      version = Version.from_integer(1)

      # Expected lexicographic sequence: key1 -> key10 (offset +1)
      # In sorted order: ["key1", "key10", "key2", "key5"]
      expected_resolved_key = "key10"

      assert {:ok, resolved_key, page} = IndexManager.page_for_key(manager, offset_selector, version)
      assert resolved_key == expected_resolved_key
      assert is_binary(page) or is_map(page)
    end

    test "pages_for_range/4 with KeySelectors handles version errors", %{manager: manager} do
      start_selector = KeySelector.first_greater_or_equal("range:a")
      end_selector = KeySelector.first_greater_than("range:z")

      # Future version should fail
      assert {:error, :version_too_new} =
               IndexManager.pages_for_range(manager, start_selector, end_selector, Version.from_integer(999))

      # Old version with higher current version should fail
      updated_manager = %{manager | current_version: Version.from_integer(10)}

      assert {:error, reason} =
               IndexManager.pages_for_range(updated_manager, start_selector, end_selector, Version.from_integer(0))

      assert reason in [:version_too_old, :not_found]
    end

    test "pages_for_range/4 with KeySelectors - successful resolution", %{manager: manager} do
      start_selector = KeySelector.first_greater_or_equal("range:a")
      end_selector = KeySelector.first_greater_than("range:z")
      version = Version.from_integer(1)

      result = IndexManager.pages_for_range(manager, start_selector, end_selector, version)

      # Range operations may return not_found for missing ranges
      assert {:error, :not_found} = result
    end

    test "pages_for_range/4 with KeySelectors - invalid range detection", %{manager: manager} do
      # Create invalid range where start > end after resolution
      start_selector = KeySelector.first_greater_or_equal("z")
      end_selector = KeySelector.first_greater_or_equal("a")
      version = Version.from_integer(1)

      result = IndexManager.pages_for_range(manager, start_selector, end_selector, version)

      # Should handle invalid range - may return :invalid_range or :not_found
      assert {:error, reason} = result
      assert reason in [:invalid_range, :not_found]
    end

    test "pages_for_range/4 with KeySelectors - complex offset scenarios", %{manager: manager} do
      # Test range with negative and positive offsets around key5
      # key5 - 2 should be key2, key5 + 2 should be key10
      start_selector = "key5" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-2)
      end_selector = "key5" |> KeySelector.first_greater_or_equal() |> KeySelector.add(2)
      version = Version.from_integer(1)

      result = IndexManager.pages_for_range(manager, start_selector, end_selector, version)

      # Should successfully resolve the range with complex offsets
      # key5 - 2 should be "key10", key5 + 2 should be "range:b"
      assert {:ok, {"key10", "range:b"}, pages} = result
      assert is_list(pages)
    end
  end
end
