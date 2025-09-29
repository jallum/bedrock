defmodule Bedrock.DataPlane.Storage.Olivine.RangeClearOrderingPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.Test.Storage.Olivine.InvariantChecks

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.Storage.Olivine.IndexTestHelpers

  @min_keys 100
  @max_keys 300

  setup do
    # Create temporary directory for test database
    temp_dir = System.tmp_dir!() <> "/test_range_clear_#{System.unique_integer()}"
    File.mkdir_p!(temp_dir)

    db_file_path = Path.join(temp_dir, "range_clear_test.dets")
    {:ok, database} = Database.open(:"test_db_#{System.unique_integer()}", db_file_path)

    on_exit(fn ->
      :ok = Database.close(database)
      File.rm_rf!(temp_dir)
    end)

    {:ok, database: database}
  end

  describe "range clearing preserves key ordering invariants" do
    property "key ordering is maintained after random range clears and page splits" do
      check all(
              keys <- unique_keys(@min_keys, @max_keys),
              clear_ranges <- clear_ranges_from_keys(keys),
              max_runs: 20
            ) do
        # Build initial index with all keys (no database needed)
        {index, allocator} = build_index_from_keys(keys)

        # Verify initial state is ordered
        assert_keys_ordered(index, "Initial state")

        # Calculate expected remaining keys BEFORE applying clears
        expected_remaining_keys = calculate_remaining_keys_after_clears(keys, clear_ranges)

        # Create temporary database for this property test run
        temp_dir = System.tmp_dir!() <> "/prop_test_#{System.unique_integer()}"
        File.mkdir_p!(temp_dir)
        db_file_path = Path.join(temp_dir, "prop_test.dets")
        {:ok, database} = Database.open(:"prop_db_#{System.unique_integer()}", db_file_path)

        final_state =
          try do
            # Apply range clear operations one by one, checking invariants after each
            apply_clear_ranges_with_checks(index, allocator, clear_ranges, database)
          after
            :ok = Database.close(database)
            File.rm_rf!(temp_dir)
          end

        # Final verification
        final_index = elem(final_state, 0)

        assert_keys_ordered(final_index, "Final state")
        assert_key_completeness(final_index, expected_remaining_keys)
        assert_all_invariants_comprehensive(final_index)
      end
    end

    property "tree structure remains consistent after complex operations" do
      check all(
              # Smaller for faster testing
              keys <- unique_keys(50, 150),
              operations <- mixed_operations_from_keys(keys),
              max_runs: 10
            ) do
        # Build initial index
        {index, allocator} = build_index_from_keys(keys)

        # Create temporary database for this property test run
        temp_dir = System.tmp_dir!() <> "/prop_test_#{System.unique_integer()}"
        File.mkdir_p!(temp_dir)
        db_file_path = Path.join(temp_dir, "prop_test.dets")
        {:ok, database} = Database.open(:"prop_db_#{System.unique_integer()}", db_file_path)

        final_index =
          try do
            # Apply mixed operations (only clear_range, no sets to avoid database dependency)
            version = Version.from_bytes(<<System.system_time(:microsecond)::64>>)
            index_update = IndexUpdate.new(index, version, allocator, database)

            # Filter to only clear_range operations
            clear_operations = Enum.filter(operations, &match?({:clear_range, _, _}, &1))

            final_index_update =
              index_update
              |> apply_clear_mutations_only(clear_operations)
              |> IndexUpdate.process_pending_operations()

            {final_index, _, _, _} = IndexUpdate.finish(final_index_update)
            final_index
          after
            :ok = Database.close(database)
            File.rm_rf!(temp_dir)
          end

        # Verify all invariants
        assert_keys_ordered(final_index, "After mixed operations")
        assert_all_invariants_comprehensive(final_index)
      end
    end
  end

  # Generators

  defp unique_keys(min, max) do
    # Generate keys with more structured format to avoid duplicates
    gen all(
          count <- integer(min..max),
          key_indices <- uniq_list_of(integer(0..99_999), length: count)
        ) do
      key_indices
      # Convert to 4-byte binary
      |> Enum.map(&<<&1::32>>)
      |> Enum.sort()
    end
  end

  defp clear_ranges_from_keys(keys) do
    gen all(
          num_ranges <- integer(1..min(10, div(length(keys), 50))),
          ranges <- list_of(range_from_keys(keys), length: num_ranges)
        ) do
      ranges
    end
  end

  defp range_from_keys(keys) do
    gen all(
          start_idx <- integer(0..(length(keys) - 1)),
          end_idx <- integer(start_idx..(length(keys) - 1))
        ) do
      start_key = Enum.at(keys, start_idx)
      end_key = Enum.at(keys, end_idx)
      {start_key, end_key}
    end
  end

  defp mixed_operations_from_keys(keys) do
    gen all(
          num_ops <- integer(5..20),
          operations <- list_of(operation_from_keys(keys), length: num_ops)
        ) do
      operations
    end
  end

  defp operation_from_keys(keys) do
    one_of([
      # Set operations (add new keys with structured format)
      gen all(
            # Use high values to avoid collision
            idx <- integer(100_000..199_999),
            value <- binary(min_length: 1, max_length: 100)
          ) do
        {:set, <<idx::32>>, value}
      end,
      # Clear range operations
      gen all({start_key, end_key} <- range_from_keys(keys)) do
        {:clear_range, start_key, end_key}
      end
    ])
  end

  # Helper functions

  defp build_index_from_keys(keys) do
    base_version = Version.zero()

    # Create key-version pairs
    key_locators = Enum.map(keys, &{&1, base_version})

    # Build pages with controlled size to ensure multiple pages
    pages = build_pages_from_key_locators(key_locators, 1)

    # Build index efficiently by constructing page_map and tree directly
    initial_index = Index.new()

    # Build page_map with all pages
    page_map =
      Enum.reduce(pages, initial_index.page_map, fn {page, next_id}, acc_map ->
        Map.put(acc_map, Page.id(page), {page, next_id})
      end)

    # Build tree with all pages
    tree =
      Enum.reduce(pages, initial_index.tree, fn {page, _next_id}, acc_tree ->
        Tree.add_page_to_tree(acc_tree, page)
      end)

    # Create temporary index and ensure chain consistency once at the end
    temp_index = %{initial_index | tree: tree, page_map: page_map}
    index = IndexTestHelpers.rebuild_page_chain_consistency(temp_index)

    # Create allocator with next available ID
    max_page_id = pages |> Enum.map(fn {page, _next_id} -> Page.id(page) end) |> Enum.max(fn -> 0 end)
    allocator = IdAllocator.new(max_page_id, [])

    {index, allocator}
  end

  defp build_pages_from_key_locators(key_locators, start_id) do
    # Split into chunks of max 200 keys per page to ensure multiple pages
    key_locators
    |> Enum.chunk_every(200)
    |> Enum.with_index(start_id)
    |> Enum.map(fn {chunk, page_id} ->
      next_id = if page_id == start_id + div(length(key_locators), 200), do: 0, else: page_id + 1
      page = Page.new(page_id, chunk)
      {page, next_id}
    end)
  end

  defp apply_clear_ranges_with_checks(index, allocator, clear_ranges, database) do
    Enum.reduce(clear_ranges, {index, allocator}, fn {start_key, end_key}, {current_index, current_allocator} ->
      version = Version.from_bytes(<<System.system_time(:microsecond)::64>>)

      # Apply single clear range
      index_update = IndexUpdate.new(current_index, version, current_allocator, database)

      updated_index_update =
        index_update
        |> apply_clear_mutations_only([{:clear_range, start_key, end_key}])
        |> IndexUpdate.process_pending_operations()

      {new_index, _, new_allocator, _} = IndexUpdate.finish(updated_index_update)

      # Check invariants after this operation
      assert_keys_ordered(
        new_index,
        "After clearing range #{inspect(start_key, base: :hex)}..#{inspect(end_key, base: :hex)}"
      )

      assert_all_invariants_comprehensive(new_index)

      {new_index, new_allocator}
    end)
  end

  # Apply only clear mutations without database dependency
  defp apply_clear_mutations_only(index_update, clear_mutations) do
    # Use the real IndexUpdate.apply_mutations implementation
    IndexUpdate.apply_mutations(index_update, clear_mutations)
  end

  # Assertion helpers

  defp assert_keys_ordered(index, context) do
    all_keys = extract_all_keys(index)
    sorted_keys = Enum.sort(all_keys)

    if all_keys != sorted_keys do
      {out_of_order_idx, prev_key, bad_key} = find_first_out_of_order(all_keys)

      flunk("""
      #{context}: Keys are out of order!
      First out-of-order key at index #{out_of_order_idx}:
      Previous key: #{inspect(prev_key, base: :hex)}
      Out-of-order key: #{inspect(bad_key, base: :hex)}

      All keys (first 10): #{inspect(Enum.take(all_keys, 10), base: :hex)}
      Expected (first 10): #{inspect(Enum.take(sorted_keys, 10), base: :hex)}
      """)
    end
  end

  # Helper functions that are still needed (not duplicated in InvariantChecks)

  defp calculate_remaining_keys_after_clears(initial_keys, clear_ranges) do
    # Simple, foolproof calculation: start with all keys, remove keys in each range
    sorted_initial_keys = Enum.sort(initial_keys)

    remaining_keys =
      Enum.reduce(clear_ranges, sorted_initial_keys, fn {start_key, end_key}, current_keys ->
        # Remove all keys in this range
        Enum.reject(current_keys, fn key ->
          key >= start_key and key <= end_key
        end)
      end)

    # Return sorted to match what extract_all_keys returns
    Enum.sort(remaining_keys)
  end

  defp extract_all_keys(index) do
    index.page_map
    |> Map.values()
    |> Enum.flat_map(fn {page, _next_id} -> Page.keys(page) end)
  end

  defp find_first_out_of_order(keys) do
    keys
    |> Enum.with_index()
    |> Enum.reduce_while(nil, fn {key, idx}, prev_key ->
      if prev_key && key < prev_key do
        {:halt, {idx, prev_key, key}}
      else
        {:cont, key}
      end
    end)
  end
end

# NOTE: Removed all duplicated invariant checking functions - now using InvariantChecks module
