defmodule Bedrock.DataPlane.Storage.Olivine.IndexManagerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager.Page
  alias Bedrock.DataPlane.Storage.Olivine.PageTestHelpers
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # Helper functions for cleaner test assertions

  # Helper function to create a test database for unit tests
  defp create_test_database do
    tmp_dir = System.tmp_dir!()
    db_file = Path.join(tmp_dir, "test_db_#{System.unique_integer([:positive])}.dets")
    table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
    {:ok, database} = Database.open(table_name, db_file)
    database
  end

  # Helper function to assert key-version pairs using the new tuple-based API
  defp assert_key_versions_equal(page, expected_key_version_pairs) do
    expected_tuples =
      Enum.map(expected_key_version_pairs, fn {key, version_int} ->
        {key, Version.from_integer(version_int)}
      end)

    assert Page.key_versions(page) == expected_tuples
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
      vm = IndexManager.new()
      assert vm.max_page_id == 0
      assert vm.free_page_ids == []
      assert vm.current_version == Version.zero()
    end

    test "next_id/1 allocates new page IDs sequentially" do
      vm = IndexManager.new()

      {page_id_1, vm1} = IndexManager.next_id(vm)
      assert page_id_1 == 1
      assert vm1.max_page_id == 1

      {page_id_2, vm2} = IndexManager.next_id(vm1)
      assert page_id_2 == 2
      assert vm2.max_page_id == 2
    end

    test "next_id/1 reuses free page IDs before allocating new ones" do
      vm = %{IndexManager.new() | free_page_ids: [5, 3], max_page_id: 10}

      {page_id_1, vm1} = IndexManager.next_id(vm)
      assert page_id_1 == 5
      assert vm1.free_page_ids == [3]
      assert vm1.max_page_id == 10

      {page_id_2, vm2} = IndexManager.next_id(vm1)
      assert page_id_2 == 3
      assert vm2.free_page_ids == []
      assert vm2.max_page_id == 10

      {page_id_3, vm3} = IndexManager.next_id(vm2)
      assert page_id_3 == 11
      assert vm3.max_page_id == 11
    end

    test "info/2 returns page management information" do
      vm = %{IndexManager.new() | max_page_id: 42, free_page_ids: [1, 3, 5]}

      assert IndexManager.info(vm, :max_page_id) == 42
      assert IndexManager.info(vm, :free_page_ids) == [1, 3, 5]
      assert IndexManager.info(vm, :unknown_stat) == :undefined
    end
  end

  describe "page creation" do
    test "new/3 creates a page with key-version tuples" do
      keys = [<<"key1">>, <<"key2">>, <<"key3">>]
      versions = [100, 200, 300]

      page = Page.new(1, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)))

      assert Page.id(page) == 1
      assert Page.next_id(page) == 0
      expected_key_versions = Enum.zip(keys, Enum.map(versions, &Version.from_integer/1))
      assert Page.key_versions(page) == expected_key_versions
    end

    test "new/3 creates a page with keys and default versions" do
      keys = [<<"key1">>, <<"key2">>]

      page = Page.new(1, Enum.map(keys, &{&1, Version.zero()}))

      assert Page.id(page) == 1
      assert Page.next_id(page) == 0
      assert Page.keys(page) == keys
      versions = Enum.map(Page.key_versions(page), fn {_key, version} -> version end)
      assert versions == [Version.zero(), Version.zero()]
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
      page = Page.new(42, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)), 99)

      encoded = Page.from_map(page)
      {:ok, decoded_page} = Page.to_map(encoded)

      assert Page.id(decoded_page) == 42
      assert Page.next_id(decoded_page) == 99
      expected_key_versions = Enum.zip(keys, Enum.map(versions, &Version.from_integer/1))
      assert Page.key_versions(decoded_page) == expected_key_versions
    end

    test "from_map/1 creates proper binary format" do
      keys = [<<"a">>, <<"bb">>]
      versions = Enum.map([1000, 2000], &Version.from_integer/1)
      page = Page.new(5, Enum.zip(keys, versions), 10)

      encoded = Page.from_map(page)

      <<id::integer-32-big, next_id::integer-32-big, key_count::integer-16-big, last_key_offset::integer-32-big,
        _reserved::unsigned-big-16, rest::binary>> = encoded

      assert id == 5
      assert next_id == 10
      assert key_count == 2
      assert last_key_offset > 0

      # New interleaved format: version1, key1_len, key1, version2, key2_len, key2
      <<version1::binary-size(8), key1_len::integer-16-big, key1::binary-size(key1_len), version2::binary-size(8),
        key2_len::integer-16-big, key2::binary-size(key2_len)>> = rest

      assert Version.to_integer(version1) == 1000
      assert Version.to_integer(version2) == 2000
      assert key1_len == 1
      assert key1 == <<"a">>
      assert key2_len == 2
      assert key2 == <<"bb">>
    end

    test "to_map/1 handles empty page" do
      empty_page = Page.new(1, [])
      encoded = Page.from_map(empty_page)

      {:ok, decoded} = Page.to_map(encoded)
      assert Page.empty?(decoded)
    end

    test "to_map/1 handles malformed page data" do
      assert {:error, :invalid_page} = Page.to_map(<<1::32>>)

      # Invalid header with incorrect field sizes or incomplete entries
      invalid_header = <<1::32, 0::32, 1::16, 0::32, 0::16>>
      invalid_data = <<invalid_header::binary, "incomplete">>
      assert {:error, :invalid_entries} = Page.to_map(invalid_data)
    end
  end

  describe "key operations within pages" do
    test "find_version_for_key/2 finds existing keys" do
      keys = [<<"apple">>, <<"banana">>, <<"cherry">>]
      versions = [100, 200, 300]
      page = Page.new(1, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)))

      {:ok, version_apple} = Page.find_version_for_key(page, <<"apple">>)
      {:ok, version_banana} = Page.find_version_for_key(page, <<"banana">>)
      {:ok, version_cherry} = Page.find_version_for_key(page, <<"cherry">>)

      assert version_apple == Version.from_integer(100)
      assert version_banana == Version.from_integer(200)
      assert version_cherry == Version.from_integer(300)
    end

    test "find_version_for_key/2 returns error for missing keys" do
      page =
        Page.new(1, [
          {<<"apple">>, Version.from_integer(100)},
          {<<"banana">>, Version.from_integer(200)}
        ])

      assert {:error, :not_found} = Page.find_version_for_key(page, <<"missing">>)
      assert {:error, :not_found} = Page.find_version_for_key(page, <<"zebra">>)
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

      {left_page, right_page} = Page.split_page(page, mid_point, new_page_id)

      # Verify the split worked
      assert Page.key_count(left_page) == mid_point
      assert Page.key_count(right_page) == key_count - mid_point
      assert Page.id(left_page) == 1
      assert Page.id(right_page) == new_page_id
    end

    test "split_page/3 splits pages over threshold" do
      keys = for i <- 1..300, do: <<"key_#{String.pad_leading(to_string(i), 3, "0")}">>
      versions = Enum.map(1..300, & &1)
      page = Page.new(1, Enum.zip(keys, Enum.map(versions, &Version.from_integer/1)), 99)

      key_count = Page.key_count(page)
      mid_point = div(key_count, 2)
      new_page_id = 2

      {left_page, right_page} = Page.split_page(page, mid_point, new_page_id)

      # Verify split results
      assert Page.id(left_page) == 1
      assert Page.id(right_page) == new_page_id
      assert Page.next_id(right_page) == 99
      assert Page.next_id(left_page) == Page.id(right_page)

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

      {left_page, right_page} = Page.split_page(page, mid_point, new_page_id)

      # Verify all key-version pairs are preserved
      left_key_versions = Page.key_versions(left_page)
      right_key_versions = Page.key_versions(right_page)
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
    test "fetch_page_for_key/3 retrieves pages containing keys" do
      vm = IndexManager.new()
      db = create_test_database()

      # Add some keys to create page structure
      mutations = [
        {:set, <<"apple">>, <<"value1">>},
        {:set, <<"banana">>, <<"value2">>},
        {:set, <<"cherry">>, <<"value3">>}
      ]

      transaction = test_transaction(mutations, Version.from_integer(1000))
      vm_updated = IndexManager.apply_transactions(vm, [transaction], db)

      # Should be able to fetch page containing key
      assert {:ok, page} = IndexManager.fetch_page_for_key(vm_updated, <<"banana">>, Version.from_integer(1000))
      assert Page.has_key?(page, <<"banana">>)

      # Should work for any key in the page
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_updated, <<"apple">>, Version.from_integer(1000))
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_updated, <<"cherry">>, Version.from_integer(1000))

      Database.close(db)
    end

    test "fetch_page_for_key/3 handles version bounds correctly" do
      vm = IndexManager.new()
      db = create_test_database()

      # Add data at version 1000
      mutation = {:set, <<"key1">>, <<"value1">>}
      transaction = test_transaction([mutation], Version.from_integer(1000))
      vm_updated = IndexManager.apply_transactions(vm, [transaction], db)

      # Should work at current version
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_updated, <<"key1">>, Version.from_integer(1000))

      # Should reject future versions
      assert {:error, :version_too_new} =
               IndexManager.fetch_page_for_key(vm_updated, <<"key1">>, Version.from_integer(2000))

      # Note: version_too_old check was removed since IndexManager no longer tracks durable_version

      Database.close(db)
    end

    test "fetch_pages_for_range/4 retrieves all pages in key range" do
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
      vm_updated = IndexManager.apply_transactions(vm, [transaction], db)

      # Fetch pages for a range
      start_key = <<"key_010">>
      end_key = <<"key_020">>

      assert {:ok, pages} =
               IndexManager.fetch_pages_for_range(vm_updated, start_key, end_key, Version.from_integer(1000))

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
    test "apply_single_transaction/2 processes set mutations" do
      vm = IndexManager.new()
      database = create_test_database()

      mutations = [
        {:set, <<"key1">>, <<"value1">>},
        {:set, <<"key2">>, <<"value2">>}
      ]

      transaction = test_transaction(mutations, Version.from_integer(1000))

      vm_updated = IndexManager.apply_single_transaction(transaction, vm, database)

      # Version should be updated
      assert vm_updated.current_version == Version.from_integer(1000)

      Database.close(database)
    end

    test "apply_single_transaction/2 processes clear mutations" do
      vm = IndexManager.new()
      database = create_test_database()

      # First add some data
      set_mutations = [
        {:set, <<"key1">>, <<"value1">>},
        {:set, <<"key2">>, <<"value2">>},
        {:set, <<"key3">>, <<"value3">>}
      ]

      set_transaction = test_transaction(set_mutations, Version.from_integer(1000))
      vm_with_data = IndexManager.apply_single_transaction(set_transaction, vm, database)

      # Then clear one key
      clear_mutations = [
        {:clear, <<"key2">>}
      ]

      clear_transaction = test_transaction(clear_mutations, Version.from_integer(1100))
      vm_after_clear = IndexManager.apply_single_transaction(clear_transaction, vm_with_data, database)

      # Key1 and Key3 should still be fetchable at version 1000
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_after_clear, <<"key1">>, Version.from_integer(1000))
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_after_clear, <<"key3">>, Version.from_integer(1000))

      # Key2 should not be fetchable at version 1100 (after clear)
      case IndexManager.fetch_page_for_key(vm_after_clear, <<"key2">>, Version.from_integer(1100)) do
        {:ok, page} ->
          # If we get a page, the key should not be found in it
          assert {:error, :not_found} = Page.find_version_for_key(page, <<"key2">>)

        {:error, :not_found} ->
          # Or the page itself might not exist if it was emptied
          :ok
      end

      Database.close(database)
    end

    test "apply_single_transaction/2 processes clear_range mutations" do
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
      vm_with_data = IndexManager.apply_single_transaction(set_transaction, vm, database)

      # Clear a range
      clear_mutations = [
        {:clear_range, <<"key_05">>, <<"key_15">>}
      ]

      clear_transaction = test_transaction(clear_mutations, Version.from_integer(1100))
      vm_after_clear = IndexManager.apply_single_transaction(clear_transaction, vm_with_data, database)

      # Keys outside the range should still be accessible
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_after_clear, <<"key_01">>, Version.from_integer(1100))
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_after_clear, <<"key_20">>, Version.from_integer(1100))

      # Keys within the cleared range should not be accessible at the new version
      for i <- 5..15 do
        key = <<"key_#{String.pad_leading(to_string(i), 2, "0")}">>

        case IndexManager.fetch_page_for_key(vm_after_clear, key, Version.from_integer(1100)) do
          {:ok, page} ->
            # If page exists, key should not be found in it
            assert {:error, :not_found} = Page.find_version_for_key(page, key)

          {:error, :not_found} ->
            # Or page might not exist if emptied
            :ok
        end
      end

      Database.close(database)
    end

    test "apply_transactions/2 processes multiple transactions in sequence" do
      vm = IndexManager.new()
      db = create_test_database()

      transaction1 = test_transaction([{:set, <<"key1">>, <<"value1">>}], Version.from_integer(1000))
      transaction2 = test_transaction([{:set, <<"key2">>, <<"value2">>}], Version.from_integer(1100))
      transaction3 = test_transaction([{:set, <<"key3">>, <<"value3">>}], Version.from_integer(1200))

      vm_updated = IndexManager.apply_transactions(vm, [transaction1, transaction2, transaction3], db)

      # Current version should be the latest
      assert vm_updated.current_version == Version.from_integer(1200)

      # Note: Value access testing moved to Database tests since IndexManager no longer handles values

      Database.close(db)
    end
  end

  describe "version management and windowing" do
    test "calculate_window_start/1 computes correct window boundary" do
      # 10 seconds
      vm = %{IndexManager.new() | current_version: Version.from_integer(10_000_000)}

      window_start = IndexManager.calculate_window_start(vm)
      # 5 seconds ago
      expected_start = Version.from_integer(5_000_000)

      assert window_start == expected_start
    end

    test "version_in_window?/2 correctly identifies versions in window" do
      window_start = Version.from_integer(5_000_000)

      # Versions at or after window start should be in window
      assert IndexManager.version_in_window?(Version.from_integer(5_000_000), window_start) == true
      assert IndexManager.version_in_window?(Version.from_integer(6_000_000), window_start) == true

      # Versions before window start should not be in window
      assert IndexManager.version_in_window?(Version.from_integer(4_999_999), window_start) == false
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

      {kept, evicted} = IndexManager.split_versions_at_window(versions, window_start)

      # Versions 10M, 8M, 6M should be kept (in window)
      assert length(kept) == 3
      assert elem(Enum.at(kept, 0), 0) == Version.from_integer(10_000_000)
      assert elem(Enum.at(kept, 1), 0) == Version.from_integer(8_000_000)
      assert elem(Enum.at(kept, 2), 0) == Version.from_integer(6_000_000)

      # Versions 4M, 2M should be evicted (outside window)
      assert length(evicted) == 2
      assert elem(Enum.at(evicted, 0), 0) == Version.from_integer(4_000_000)
      assert elem(Enum.at(evicted, 1), 0) == Version.from_integer(2_000_000)
    end
  end

  describe "tree operations" do
    test "get_current_tree/1 returns tree from current version" do
      vm = IndexManager.new()

      tree = IndexManager.get_current_tree(vm)
      # Should return the gb_tree structure
      assert tree
    end

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
      vm_updated = IndexManager.apply_transactions(vm, [transaction], database)

      _tree = IndexManager.get_current_tree(vm_updated)

      # Tree should be able to find pages for keys
      # This is tested indirectly through fetch_page_for_key working
      assert {:ok, _page} = IndexManager.fetch_page_for_key(vm_updated, <<"banana">>, Version.from_integer(1000))

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
    test "stream_key_versions_in_range/3 filters keys correctly" do
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
        |> Page.stream_key_versions_in_range(<<"banana">>, <<"date">>)
        |> Enum.to_list()

      expected = [
        {<<"banana">>, Version.from_integer(200)},
        {<<"cherry">>, Version.from_integer(300)}
      ]

      assert key_versions == expected
    end

    test "stream_key_versions_in_range/3 returns consistent results" do
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
        |> Page.stream_key_versions_in_range(<<"banana">>, <<"zebra">>)
        |> Enum.to_list()

      key_versions_2 =
        pages
        |> Page.stream_key_versions_in_range(<<"banana">>, <<"zebra">>)
        |> Enum.to_list()

      assert key_versions_1 == key_versions_2
    end
  end
end
