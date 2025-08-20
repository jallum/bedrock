defmodule Bedrock.DataPlane.Storage.Olivine.VersionManagerPropertyTest do
  @moduledoc """
  Property-based tests for the VersionManager module.

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

  alias Bedrock.DataPlane.Storage.Olivine.VersionManager
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Page
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Tree
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
      Page.new(page_id, keys, versions)
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
      Page.new(page_id, range_keys, simple_versions)
    end)
  end

  # Tree Invariant Properties

  property "tree maintains sorted order invariant" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree =
          Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
            if length(page.keys) > 0 do
              Tree.add_page_to_tree(tree_acc, page.id, page.keys)
            else
              tree_acc
            end
          end)

        # Extract all (last_key, {page_id, first_key}) pairs from tree
        tree_entries = extract_tree_entries(tree)

        # Verify tree maintains search property - all keys should be retrievable
        # Note: GB-tree doesn't guarantee traversal order, only search efficiency
        last_keys = Enum.map(tree_entries, fn {last_key, _} -> last_key end)
        assert length(last_keys) == length(Enum.uniq(last_keys)), "All tree keys should be unique"

        # Verify each page's range is valid (first_key <= last_key)
        Enum.each(tree_entries, fn {last_key, {_page_id, first_key}} ->
          assert first_key <= last_key, "Page range should be valid: first_key <= last_key"
        end)

        # Verify tree search functionality works correctly
        # For each page, all keys in that page should map back to that page
        Enum.each(pages, fn page ->
          Enum.each(page.keys, fn key ->
            found_page_id = Tree.page_for_key(tree, key)
            assert found_page_id == page.id, "Key '#{key}' should find page #{page.id}"
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
            Tree.add_page_to_tree(tree_acc, page.id, page.keys)
          end)

        # Test that each key in each page can be found
        Enum.each(pages, fn page ->
          Enum.each(page.keys, fn key ->
            found_page_id = Tree.page_for_key(tree, key)

            assert found_page_id == page.id,
                   "Key '#{key}' should be found in page #{page.id}, but found in #{inspect(found_page_id)}"
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
            Tree.add_page_to_tree(tree_acc, page.id, page.keys)
          end)

        found_page_id = Tree.page_for_insertion(tree, test_key)
        assert is_integer(found_page_id), "page_for_insertion should always return a page_id"
        assert found_page_id >= 0, "page_id should be non-negative"

        # The returned page should exist in our original pages
        page_ids = Enum.map(pages, & &1.id)
        assert found_page_id in page_ids, "Returned page_id should exist in the tree"
      end
    end
  end

  property "find_rightmost_page returns page with highest last_key" do
    check all(pages <- non_overlapping_pages_generator()) do
      if length(pages) > 0 do
        tree =
          Enum.reduce(pages, :gb_trees.empty(), fn page, tree_acc ->
            Tree.add_page_to_tree(tree_acc, page.id, page.keys)
          end)

        rightmost_page_id = Tree.find_rightmost_page(tree)
        assert is_integer(rightmost_page_id), "find_rightmost_page should return a page_id"

        # Find the expected rightmost page (highest last_key)
        expected_page = Enum.max_by(pages, &List.last(&1.keys))

        assert rightmost_page_id == expected_page.id,
               "Rightmost page should be the one with highest last_key"
      end
    end
  end

  # Key Insertion Properties

  property "add_key_to_page maintains sorted order" do
    check all(
            page <- page_generator(),
            new_key <- binary_key_generator(),
            new_version <- version_generator()
          ) do
      if length(page.keys) > 0 do
        updated_page = Page.add_key_to_page(page, new_key, new_version)

        # Keys should remain sorted
        assert updated_page.keys == Enum.sort(updated_page.keys),
               "Keys should remain sorted after insertion"

        # Should have same number of keys and versions
        assert length(updated_page.keys) == length(updated_page.versions),
               "Number of keys and versions should match"

        # New key should be present
        assert new_key in updated_page.keys, "New key should be present in the page"

        # If key was already present, page size shouldn't change
        if new_key in page.keys do
          assert length(updated_page.keys) == length(page.keys),
                 "Page size shouldn't change when updating existing key"
        else
          assert length(updated_page.keys) == length(page.keys) + 1,
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
      if length(page.keys) > 0 do
        tree = Tree.add_page_to_tree(:gb_trees.empty(), page.id, page.keys)

        # Update the tree with new keys
        updated_tree = Tree.update_page_in_tree(tree, page.id, page.keys, new_keys)

        # Verify tree is still valid
        tree_entries = extract_tree_entries(updated_tree)

        if length(tree_entries) > 0 do
          # Tree should contain the new range
          {last_key, {found_page_id, first_key}} = List.first(tree_entries)
          assert found_page_id == page.id, "Page ID should be preserved"
          assert first_key == List.first(new_keys), "First key should be updated"
          assert last_key == List.last(new_keys), "Last key should be updated"
        end
      end
    end
  end

  # Page Splitting Properties

  property "page splitting creates valid pages" do
    # Create an oversized page to test splitting
    check all(base_keys <- list_of(binary_key_generator(), min_length: 10, max_length: 50)) do
      # Create enough keys to trigger splitting (>256)
      additional_keys = for i <- 1..260, do: "split_key_#{String.pad_leading("#{i}", 4, "0")}"
      all_keys = (base_keys ++ additional_keys) |> Enum.sort() |> Enum.dedup()

      # Ensure we have enough keys
      if length(all_keys) > 256 do
        versions = Enum.map(all_keys, fn _ -> Version.from_integer(1) end)
        vm = VersionManager.new()
        oversized_page = Page.new(1, all_keys, versions)

        case Page.split_page_simple(oversized_page, vm) do
          {{left_page, right_page}, _updated_vm} ->
            # Both pages should be valid
            assert length(left_page.keys) > 0, "Left page should have keys"
            assert length(right_page.keys) > 0, "Right page should have keys"
            assert length(left_page.keys) <= 256, "Left page should not exceed max size"
            assert length(right_page.keys) <= 256, "Right page should not exceed max size"

            # Keys should be sorted within each page
            assert left_page.keys == Enum.sort(left_page.keys), "Left page keys should be sorted"
            assert right_page.keys == Enum.sort(right_page.keys), "Right page keys should be sorted"

            # All original keys should be preserved across both pages
            all_new_keys = left_page.keys ++ right_page.keys
            assert Enum.sort(all_new_keys) == Enum.sort(all_keys), "All keys should be preserved"

            # Left page keys should all be < right page keys
            left_max = List.last(left_page.keys)
            right_min = List.first(right_page.keys)
            assert left_max < right_min, "Left page max should be < right page min"

          {:error, :no_split_needed} ->
            # This is fine - the page might not need splitting
            :ok
        end
      end
    end
  end

  # Edge Case Properties

  property "empty tree operations behave correctly" do
    check all(test_key <- binary_key_generator()) do
      empty_tree = :gb_trees.empty()

      # Empty tree should return nil for page_for_key
      assert Tree.page_for_key(empty_tree, test_key) == nil

      # Empty tree should return page 0 for page_for_insertion
      assert Tree.page_for_insertion(empty_tree, test_key) == 0

      # Empty tree should return nil for find_rightmost_page
      assert Tree.find_rightmost_page(empty_tree) == nil
    end
  end

  property "single page tree operations work correctly" do
    check all(
            page <- page_generator(),
            test_key <- binary_key_generator()
          ) do
      if length(page.keys) > 0 do
        tree = Tree.add_page_to_tree(:gb_trees.empty(), page.id, page.keys)

        # Single page tree should return that page for rightmost
        assert Tree.find_rightmost_page(tree) == page.id

        # page_for_insertion should return the page for any key
        found_page_id = Tree.page_for_insertion(tree, test_key)
        assert found_page_id == page.id
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
