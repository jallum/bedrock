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
  - `Tree.page_for_insertion` never returns nil for non-empty trees
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

  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager.Tree
  alias Bedrock.DataPlane.Storage.Olivine.PageTestHelpers
  alias Bedrock.DataPlane.Version

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
      Page.new(page_id, keys |> Enum.zip(versions) |> Enum.sort())
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
        tree =
          Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
            if Page.empty?(page) do
              tree_acc
            else
              Tree.add_page_to_tree(tree_acc, page)
            end
          end)

        tree_entries = extract_tree_entries(tree)

        last_keys = Enum.map(tree_entries, fn {last_key, _} -> last_key end)
        assert length(last_keys) == length(Enum.uniq(last_keys)), "All tree keys should be unique"

        Enum.each(tree_entries, fn {last_key, {_page_id, first_key}} ->
          assert first_key <= last_key, "Page range should be valid: first_key <= last_key"
        end)

        Enum.each(pages, fn page ->
          Enum.each(Page.keys(page), fn key ->
            found_page_id = Tree.page_for_key(tree, key)
            assert found_page_id == Page.id(page), "Key '#{key}' should find page #{Page.id(page)}"
          end)
        end)
      end
    end
  end

  property "page_for_key returns correct page for keys in range" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree =
          Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
            Tree.add_page_to_tree(tree_acc, page)
          end)

        Enum.each(pages, fn page ->
          Enum.each(Page.keys(page), fn key ->
            found_page_id = Tree.page_for_key(tree, key)

            assert found_page_id == Page.id(page),
                   "Key '#{key}' should be found in page #{Page.id(page)}, but found in #{inspect(found_page_id)}"
          end)
        end)
      end
    end
  end

  property "page_for_insertion never returns nil for non-empty tree" do
    check all(
            pages <- non_overlapping_pages_generator(),
            test_key <- binary_key_generator()
          ) do
      if length(pages) > 0 do
        tree =
          Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
            Tree.add_page_to_tree(tree_acc, page)
          end)

        found_page_id = Tree.page_for_insertion(tree, test_key)
        assert is_integer(found_page_id), "page_for_insertion should always return a page_id"
        assert found_page_id >= 0, "page_id should be non-negative"

        page_ids = Enum.map(pages, &Page.id/1)
        assert found_page_id in page_ids, "Returned page_id should exist in the tree"
      end
    end
  end

  property "find_rightmost_page returns page with highest last_key" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree =
          Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
            Tree.add_page_to_tree(tree_acc, page)
          end)

        rightmost_page_id = Tree.find_rightmost_page(tree)
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

        assert PageTestHelpers.keys_are_sorted(updated_page),
               "Keys should remain sorted after insertion"

        assert Page.key_count(updated_page) == length(Page.key_versions(updated_page)),
               "Number of keys and versions should match"

        assert Page.has_key?(updated_page, new_key), "New key should be present in the page"

        if Page.has_key?(page, new_key) do
          assert Page.key_count(updated_page) == Page.key_count(page),
                 "Page size shouldn't change when updating existing key"
        else
          assert Page.key_count(updated_page) == Page.key_count(page) + 1,
                 "Page size should increase by 1 when adding new key"
        end
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
          {last_key, {found_page_id, first_key}} = List.first(tree_entries)
          assert found_page_id == Page.id(page), "Page ID should be preserved"
          assert first_key == List.first(new_keys), "First key should be updated"
          assert last_key == List.last(new_keys), "Last key should be updated"
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

          {left_page, right_page} = Page.split_page(oversized_page, mid_point, new_page_id)

          assert not Page.empty?(left_page), "Left page should have keys"
          assert not Page.empty?(right_page), "Right page should have keys"
          assert Page.key_count(left_page) <= 256, "Left page should not exceed max size"
          assert Page.key_count(right_page) <= 256, "Right page should not exceed max size"

          assert PageTestHelpers.keys_are_sorted(left_page), "Left page keys should be sorted"
          assert PageTestHelpers.keys_are_sorted(right_page), "Right page keys should be sorted"

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

      assert Tree.page_for_key(empty_tree, test_key) == nil

      assert Tree.page_for_insertion(empty_tree, test_key) == 0

      assert Tree.find_rightmost_page(empty_tree) == nil
    end
  end

  property "single page tree operations work correctly" do
    check all(
            page <- page_generator(),
            test_key <- binary_key_generator()
          ) do
      if not Page.empty?(page) do
        tree = Tree.add_page_to_tree(:gb_trees.empty(), page)

        assert Tree.find_rightmost_page(tree) == Page.id(page)

        found_page_id = Tree.page_for_insertion(tree, test_key)
        assert found_page_id == Page.id(page)
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

        # Additional check: verify all keys are unique
        keys = Page.keys(updated_page)

        assert length(keys) == length(Enum.uniq(keys)),
               "All keys should be unique after applying operations"

        # Critical check: verify right_key returns the actual last key
        if not Page.empty?(updated_page) do
          expected_last_key = List.last(keys)
          actual_last_key = Page.right_key(updated_page)

          assert actual_last_key == expected_last_key,
                 "right_key should return '#{expected_last_key}', but returned '#{actual_last_key}'"
        end
      end
    end
  end

  property "right_key works correctly after last key changes" do
    check all(
            page <- page_generator(),
            new_last_key <- binary_key_generator()
          ) do
      if not Page.empty?(page) do
        original_last_key = Page.right_key(page)

        # Test case 1: Add a key that becomes the new last key
        if new_last_key > original_last_key do
          operations = %{new_last_key => {:set, Version.from_integer(1)}}
          updated_page = Page.apply_operations(page, operations)

          assert Page.right_key(updated_page) == new_last_key,
                 "After adding '#{new_last_key}', right_key should return it"
        end

        # Test case 2: Clear all keys
        clear_operations =
          page
          |> Page.keys()
          |> Map.new(&{&1, :clear})

        cleared_page = Page.apply_operations(page, clear_operations)

        if Page.empty?(cleared_page) do
          # Empty page should handle right_key gracefully
          try do
            Page.right_key(cleared_page)
          rescue
            e -> flunk("right_key should handle empty page, but raised: #{inspect(e)}")
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

        if not Page.empty?(result_page) do
          # Get all operations that weren't clears
          non_clear_ops = Enum.reject(operations, fn {_key, op} -> op == :clear end)

          if length(non_clear_ops) > 0 do
            # The last key should be the lexicographically largest key from operations
            expected_last_key =
              non_clear_ops
              |> Enum.map(fn {key, _op} -> key end)
              |> Enum.sort()
              |> List.last()

            actual_last_key = Page.right_key(result_page)

            assert actual_last_key == expected_last_key,
                   "right_key should return '#{expected_last_key}' but returned '#{actual_last_key}'"
          end
        end
      end
    end
  end

  # Helper functions

  defp extract_tree_entries({0, _}), do: []
  defp extract_tree_entries({_size, tree_node}), do: extract_entries_from_node(tree_node, [])

  defp extract_entries_from_node(nil, acc), do: acc

  defp extract_entries_from_node({last_key, {page_id, first_key}, left, right}, acc) do
    acc1 = extract_entries_from_node(left, acc)
    acc2 = [{last_key, {page_id, first_key}} | acc1]
    extract_entries_from_node(right, acc2)
  end
end
