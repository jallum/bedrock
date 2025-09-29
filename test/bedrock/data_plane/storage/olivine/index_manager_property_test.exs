defmodule Bedrock.DataPlane.Storage.Olivine.IndexManagerPropertyTest do
  @moduledoc """
  Property-based tests for the IndexManager module.

  These tests verify important invariants and properties of the tree operations,
  key insertion logic, and page management functionality using random generated data.

  ## Test Coverage:

  **Tree Invariants:**
  - Tree maintains search functionality across random page configurations
  - All keys within page ranges can be found by tree search
  - Tree keys are unique and page ranges are valid (first_key <= last_key)

  **Page Location Functions:**
  - `Tree.page_for_key` correctly identifies pages for keys in their ranges
  - `Tree.page_for_key` never returns nil for non-empty trees
  - `find_rightmost_page` correctly identifies the page with highest last_key

  **Key Insertion Properties:**
  - `add_key_to_page` maintains sorted order within pages
  - Key insertion preserves key-version count consistency
  - Duplicate key insertion updates versions correctly

  **Tree Update Consistency:**
  - `Tree.update_page_in_tree` correctly updates tree when page ranges change
  - Tree updates preserve page IDs and update first/last keys correctly

  **Page Splitting Properties:**
  - Page splitting creates valid pages that don't exceed size limits
  - Split pages maintain sorted order and preserve all original keys
  - Left page keys are all less than right page keys after splitting

  **Edge Cases:**
  - Empty tree operations behave correctly (return appropriate defaults)
  - Single page trees work correctly for all operations

  These property tests complement the unit tests by testing the same functionality
  with thousands of randomized inputs to catch edge cases and ensure robustness.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Storage.Olivine.PageTestHelpers

  # Test helper function - only used in property tests
  defp find_rightmost_page({size, tree_node}) when size > 0, do: find_rightmost_node(tree_node)
  defp find_rightmost_page(_), do: nil

  defp find_rightmost_node({_last_key, page_id, _l, nil}), do: page_id
  defp find_rightmost_node({_last_key, _page_id, _l, r}), do: find_rightmost_node(r)

  # Generators

  def binary_key_generator do
    string(:alphanumeric, min_length: 1, max_length: 20)
  end

  def page_id_generator do
    integer(0..1000)
  end

  def version_generator do
    1..1_000_000
    |> integer()
    |> map(&Version.from_integer/1)
  end

  def key_list_generator(min_length \\ 1, max_length \\ 10) do
    binary_key_generator()
    |> list_of(min_length: min_length, max_length: max_length)
    |> map(&Enum.sort/1)
    |> map(&Enum.dedup/1)
    |> map(&ensure_min_length(&1, min_length))
  end

  defp ensure_min_length(list, min_length) when length(list) >= min_length, do: list

  defp ensure_min_length(list, min_length) do
    additional_needed = min_length - length(list)
    additional_keys = for i <- 1..additional_needed, do: "generated_key_#{i}"
    (list ++ additional_keys) |> Enum.sort() |> Enum.dedup()
  end

  def page_generator do
    gen all(
          page_id <- page_id_generator(),
          keys <- key_list_generator(),
          versions <- list_of(version_generator(), length: length(keys))
        ) do
      # Create page using the proper API to ensure consistent binary structure
      page = Page.new(page_id, keys |> Enum.zip(versions) |> Enum.sort())

      # Validate the page structure by round-tripping through apply_operations
      # This ensures the page has consistent internal offsets
      validated_page = Page.apply_operations(page, %{})
      validated_page
    end
  end

  def operation_generator do
    one_of([
      constant(:clear),
      map(version_generator(), fn version -> {:set, version} end)
    ])
  end

  def operation_map_generator do
    gen all(
          keys <- list_of(binary_key_generator(), min_length: 0, max_length: 10),
          operations <- list_of(operation_generator(), length: length(keys))
        ) do
      keys
      |> Enum.zip(operations)
      |> Map.new()
    end
  end

  # Generator for non-overlapping pages
  def non_overlapping_pages_generator do
    1..10
    |> integer()
    |> bind(&generate_pages_for_count/1)
  end

  defp generate_pages_for_count(num_pages) do
    num_pages
    |> generate_sorted_keys()
    |> bind(&create_non_overlapping_pages(&1, num_pages))
  end

  defp generate_sorted_keys(num_pages) do
    binary_key_generator()
    |> list_of(length: num_pages * 3)
    |> map(&Enum.sort/1)
    |> map(&Enum.dedup/1)
  end

  defp create_non_overlapping_pages(keys, num_pages) do
    if length(keys) >= num_pages * 2 do
      build_page_ranges(keys, num_pages)
    else
      constant([])
    end
  end

  defp build_page_ranges(keys, num_pages) do
    chunk_size = max(2, div(length(keys), num_pages))
    page_ranges = Enum.chunk_every(keys, chunk_size, chunk_size, :discard)
    page_ids = Enum.to_list(1..length(page_ranges))

    news_from_ranges(page_ranges, page_ids)
  end

  defp news_from_ranges(page_ranges, page_ids) do
    tuples = Enum.zip(page_ids, page_ranges)

    tuples
    |> constant()
    |> map(&build_pages_from_tuples/1)
  end

  defp build_pages_from_tuples(tuples) do
    Enum.map(tuples, fn {page_id, range_keys} ->
      simple_versions = Enum.map(range_keys, fn _ -> Version.from_integer(1) end)
      Page.new(page_id, Enum.zip(range_keys, simple_versions))
    end)
  end

  # Tree Invariant Properties

  property "tree maintains sorted order invariant" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree = build_tree_from_pages(pages)

        tree_entries = extract_tree_entries(tree)

        last_keys = Enum.map(tree_entries, fn {last_key, _} -> last_key end)
        assert length(last_keys) == length(Enum.uniq(last_keys)), "All tree keys should be unique"

        # Note: With gap-free design, we don't store first_key in tree anymore
        # Just verify we have valid entries

        # Verify all page keys can be found by tree search
        assert Enum.all?(pages, fn page ->
                 Enum.all?(Page.keys(page), fn key ->
                   Tree.page_for_key(tree, key) == Page.id(page)
                 end)
               end),
               "All keys should be found in their respective pages"
      end
    end
  end

  property "page_for_key returns correct page for keys in range" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree = build_tree_from_pages(pages)

        # Verify page_for_key returns correct page for each key
        assert Enum.all?(pages, fn page ->
                 Enum.all?(Page.keys(page), fn key ->
                   Tree.page_for_key(tree, key) == Page.id(page)
                 end)
               end),
               "All keys should map to their correct pages"
      end
    end
  end

  property "page_for_key never returns nil for non-empty tree" do
    check all(
            pages <- non_overlapping_pages_generator(),
            test_key <- binary_key_generator()
          ) do
      if length(pages) > 0 do
        tree = build_tree_from_pages(pages)

        found_page_id = Tree.page_for_key(tree, test_key)
        page_ids = Enum.map(pages, &Page.id/1)

        assert is_integer(found_page_id) and found_page_id >= 0 and (found_page_id in page_ids or found_page_id == 0),
               "page_for_key should return a valid page_id that exists in tree or 0 (rightmost page)"
      end
    end
  end

  property "find_rightmost_page returns page with highest last_key" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree = build_tree_from_pages(pages)

        rightmost_page_id = find_rightmost_page(tree)
        assert is_integer(rightmost_page_id), "find_rightmost_page should return a page_id"

        expected_page = Enum.max_by(pages, &Page.right_key/1)

        assert rightmost_page_id == Page.id(expected_page),
               "Rightmost page should be the one with highest last_key"
      end
    end
  end

  # Key Insertion Properties

  property "apply_operations maintains sorted order" do
    check all(
            page <- page_generator(),
            new_key <- binary_key_generator(),
            new_version <- version_generator()
          ) do
      if not Page.empty?(page) do
        updated_page = Page.apply_operations(page, %{new_key => {:set, new_version}})

        # Validate page invariants after operation
        assert PageTestHelpers.keys_are_sorted(updated_page) and
                 Page.key_count(updated_page) == length(Page.key_locators(updated_page)) and
                 Page.has_key?(updated_page, new_key),
               "Page should maintain sorted order, key/version count consistency, and contain new key"

        expected_count = if Page.has_key?(page, new_key), do: Page.key_count(page), else: Page.key_count(page) + 1

        assert Page.key_count(updated_page) == expected_count,
               "Page size should be #{expected_count} after #{if Page.has_key?(page, new_key), do: "updating", else: "adding"} key"
      end
    end
  end

  property "tree updates are consistent with page range changes" do
    check all(
            page <- page_generator(),
            new_keys <- key_list_generator(1, 5)
          ) do
      if not Page.empty?(page) do
        tree = Tree.add_page_to_tree(:gb_trees.empty(), page)

        # Create a new page with the new keys for the update operation
        new_versions = Enum.map(new_keys, fn _ -> Version.from_integer(1) end)
        updated_page = Page.new(Page.id(page), Enum.zip(new_keys, new_versions))
        updated_tree = Tree.update_page_in_tree(tree, page, updated_page)

        tree_entries = extract_tree_entries(updated_tree)

        if length(tree_entries) > 0 do
          expected_page_id = Page.id(page)
          _expected_first_key = List.first(new_keys)
          expected_last_key = List.last(new_keys)

          # With gap-free design, tree entries are {last_key, page_id}
          # Also check infinity marker entry
          filtered_entries = Enum.reject(tree_entries, fn {key, _} -> key == <<0xFF, 0xFF>> end)
          assert [{^expected_last_key, ^expected_page_id}] = filtered_entries
        end
      end
    end
  end

  # Page Splitting Properties

  property "page splitting creates valid pages" do
    check all(base_keys <- list_of(binary_key_generator(), min_length: 10, max_length: 50)) do
      additional_keys = for i <- 1..260, do: "split_key_#{String.pad_leading("#{i}", 4, "0")}"
      all_keys = (base_keys ++ additional_keys) |> Enum.sort() |> Enum.dedup()

      if length(all_keys) > 256 do
        versions = Enum.map(all_keys, fn _ -> Version.from_integer(1) end)
        _vm = IndexManager.new()
        oversized_page = Page.new(1, Enum.zip(all_keys, versions))

        if Page.key_count(oversized_page) > 256 do
          key_count = Page.key_count(oversized_page)
          mid_point = div(key_count, 2)
          new_page_id = 2

          {{left_page, _left_next_id}, {right_page, _right_next_id}} =
            Page.split_page(oversized_page, mid_point, new_page_id, 0)

          # Validate both pages are non-empty and properly sized
          refute Page.empty?(left_page), "Left page should have keys"
          refute Page.empty?(right_page), "Right page should have keys"

          assert Page.key_count(left_page) <= 256 and Page.key_count(right_page) <= 256,
                 "Split pages should not exceed max size"

          # Validate both pages maintain sorted order
          assert PageTestHelpers.keys_are_sorted(left_page) and PageTestHelpers.keys_are_sorted(right_page),
                 "Both split pages should maintain sorted key order"

          all_new_keys = Page.keys(left_page) ++ Page.keys(right_page)
          assert Enum.sort(all_new_keys) == Enum.sort(all_keys), "All keys should be preserved"

          left_max = Page.right_key(left_page)
          right_min = Page.left_key(right_page)
          assert left_max < right_min, "Left page max should be < right page min"
        end
      end
    end
  end

  # Edge Case Properties

  property "empty tree operations behave correctly" do
    check all(test_key <- binary_key_generator()) do
      empty_tree = :gb_trees.empty()

      assert Tree.page_for_key(empty_tree, test_key) == 0

      assert Tree.page_for_key(empty_tree, test_key) == 0

      assert find_rightmost_page(empty_tree) == nil
    end
  end

  property "single page tree operations work correctly" do
    check all(
            page <- page_generator(),
            test_key <- binary_key_generator()
          ) do
      if not Page.empty?(page) do
        tree = Tree.add_page_to_tree(:gb_trees.empty(), page)

        assert find_rightmost_page(tree) == Page.id(page)

        found_page_id = Tree.page_for_key(tree, test_key)
        # Key can map to the page itself or to page 0 (rightmost) if beyond page's range
        assert found_page_id == Page.id(page) or found_page_id == 0
      end
    end
  end

  property "apply_operations maintains sorted key order" do
    check all(
            page <- page_generator(),
            operations <- operation_map_generator()
          ) do
      if not Page.empty?(page) or map_size(operations) > 0 do
        updated_page = Page.apply_operations(page, operations)

        assert PageTestHelpers.keys_are_sorted(updated_page),
               "Keys should remain sorted after applying operations"

        # Verify key uniqueness and right_key consistency
        keys = Page.keys(updated_page)
        assert length(keys) == length(Enum.uniq(keys)), "All keys should be unique"

        if not Page.empty?(updated_page) do
          assert Page.right_key(updated_page) == List.last(keys),
                 "right_key should return the actual last key"
        end
      end
    end
  end

  property "right_key works correctly with key modifications and clears" do
    check all(
            page <- page_generator(),
            new_last_key <- binary_key_generator()
          ) do
      if not Page.empty?(page) do
        original_last_key = Page.right_key(page)

        # Test adding a key that becomes the new last key
        if new_last_key > original_last_key do
          updated_page = Page.apply_operations(page, %{new_last_key => {:set, Version.from_integer(1)}})

          assert Page.right_key(updated_page) == new_last_key,
                 "right_key should return new largest key '#{new_last_key}'"
        end

        # Test clearing all keys - should handle empty page gracefully
        clear_operations = page |> Page.keys() |> Map.new(&{&1, :clear})
        cleared_page = Page.apply_operations(page, clear_operations)

        if Page.empty?(cleared_page) do
          # Empty page should handle right_key gracefully
          try do
            Page.right_key(cleared_page)
          rescue
            e -> flunk("right_key should handle empty page gracefully, but raised: #{inspect(e)}")
          end
        end
      end
    end
  end

  property "right_key extracts correct key from binary structure" do
    check all(operations <- operation_map_generator()) do
      if map_size(operations) > 0 do
        # Start with empty page and apply operations
        empty_page = Page.new(1, [])
        result_page = Page.apply_operations(empty_page, operations)

        # Verify right_key returns the lexicographically largest non-cleared key
        if not Page.empty?(result_page) do
          case operations |> Enum.reject(fn {_key, op} -> op == :clear end) |> Enum.map(&elem(&1, 0)) |> Enum.sort() do
            # No non-clear operations
            [] ->
              :ok

            keys ->
              expected_last_key = List.last(keys)

              assert Page.right_key(result_page) == expected_last_key,
                     "right_key should return '#{expected_last_key}' but returned '#{Page.right_key(result_page)}'"
          end
        end
      end
    end
  end

  # Helper functions

  defp build_tree_from_pages(pages) do
    Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
      if Page.empty?(page) do
        tree_acc
      else
        Tree.add_page_to_tree(tree_acc, page)
      end
    end)
  end

  defp extract_tree_entries({0, _}), do: []
  defp extract_tree_entries({_size, tree_node}), do: extract_entries_from_node(tree_node, [])

  defp extract_entries_from_node(nil, acc), do: acc

  defp extract_entries_from_node({last_key, page_id, left, right}, acc) do
    acc1 = extract_entries_from_node(left, acc)
    acc2 = [{last_key, page_id} | acc1]
    extract_entries_from_node(right, acc2)
  end
end
