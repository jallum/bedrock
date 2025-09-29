defmodule Bedrock.Test.Storage.Olivine.InvariantChecks do
  @moduledoc """
  Comprehensive invariant checks for Olivine Index structures.

  Validates all critical invariants:
  1. Page key ordering (start_key <= end_key for each page)
  2. Tree ordering (pages ordered by end_key in strictly ascending order)
  3. Page chain integrity (starts at page 0, terminates at 0, no cycles)
  4. Chain and tree order consistency (chain order matches tree order)
  5. Index consistency (tree pages match page_map pages)
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  # Helper function to get all page IDs from tree in order
  # Exclude infinity marker to avoid duplicate rightmost page
  defp get_all_page_ids_from_tree(tree) do
    tree
    |> :gb_trees.to_list()
    |> Enum.reject(fn {key, _page_id} -> key == <<0xFF, 0xFF>> end)
    |> Enum.map(fn {_key, page_id} -> page_id end)
  end

  @doc """
  Validates all invariants for an Index structure.
  Returns :ok if all invariants hold, or {:error, reason} if any are violated.
  """
  @spec check_all_invariants(Index.t()) :: :ok | {:error, String.t()}
  def check_all_invariants(index) do
    with :ok <- check_page_key_ordering(index),
         :ok <- check_tree_no_duplicate_page_ids(index),
         :ok <- check_tree_ordering(index),
         :ok <- check_page_chain_integrity(index),
         :ok <- check_chain_tree_consistency(index),
         :ok <- check_index_consistency(index) do
      check_page_size_limits(index)
    end
  end

  @doc """
  Validates all invariants including chain key ordering for comprehensive testing.
  This includes the additional chain key ordering check that's useful for tests.
  """
  @spec check_all_invariants_comprehensive(Index.t()) :: :ok | {:error, String.t()}
  def check_all_invariants_comprehensive(index) do
    with :ok <- check_all_invariants(index) do
      check_chain_key_ordering(index)
    end
  end

  @doc """
  Validates that each page has start_key <= end_key.
  """
  def check_page_key_ordering(index) do
    Enum.reduce_while(index.page_map, :ok, fn {page_id, {page, _next_id}}, acc ->
      case validate_page_key_ordering(page_id, page) do
        :ok -> {:cont, acc}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_page_key_ordering(page_id, page) do
    start_key = Page.left_key(page)
    end_key = Page.right_key(page)

    cond do
      both_keys_nil?(start_key, end_key) -> :ok
      valid_key_range?(start_key, end_key) -> :ok
      single_key_page?(start_key, end_key) -> validate_single_key_page(page_id, start_key, end_key)
      true -> invalid_key_order_error(page_id, start_key, end_key)
    end
  end

  defp both_keys_nil?(start_key, end_key), do: start_key == nil and end_key == nil
  defp valid_key_range?(start_key, end_key), do: start_key != nil and end_key != nil and start_key <= end_key
  defp single_key_page?(start_key, end_key), do: start_key == nil or end_key == nil

  defp validate_single_key_page(page_id, start_key, end_key) do
    if start_key == end_key do
      :ok
    else
      {:error,
       "Page #{page_id} has inconsistent single key: start=#{inspect(start_key, base: :hex)}, end=#{inspect(end_key, base: :hex)}"}
    end
  end

  defp invalid_key_order_error(page_id, start_key, end_key) do
    {:error,
     "Page #{page_id} has start_key > end_key: #{inspect(start_key, base: :hex)} > #{inspect(end_key, base: :hex)}"}
  end

  @doc """
  Validates that the tree has no duplicate page IDs.
  Each page should appear exactly once in the tree.
  """
  def check_tree_no_duplicate_page_ids(index) do
    tree_entries = :gb_trees.to_list(index.tree)
    page_ids = Enum.map(tree_entries, fn {_key, page_id} -> page_id end)
    unique_page_ids = Enum.uniq(page_ids)

    if length(page_ids) == length(unique_page_ids) do
      :ok
    else
      duplicates = page_ids -- unique_page_ids

      tree_details =
        Enum.map(tree_entries, fn {key, page_id} ->
          {inspect(key, base: :hex), page_id}
        end)

      {:error, "Tree has duplicate page IDs: #{inspect(duplicates)}. Tree entries: #{inspect(tree_details)}"}
    end
  end

  @doc """
  Validates that pages in the tree are ordered by end_key in strictly ascending order.
  """
  def check_tree_ordering(index) do
    # Get tree entries excluding infinity marker to avoid duplicate rightmost page
    tree_entries =
      index.tree
      |> :gb_trees.to_list()
      |> Enum.reject(fn {key, _page_id} -> key == <<0xFF, 0xFF>> end)

    tree_entries
    |> Enum.map(fn {_key, page_id} -> Map.get(index.page_map, page_id) end)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn {page, _next_id} -> {Page.id(page), Page.right_key(page)} end)
    |> check_ascending_end_keys()
  end

  defp check_ascending_end_keys(page_end_keys) do
    page_end_keys
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.reduce_while(:ok, fn [{left_id, left_end}, {right_id, right_end}], acc ->
      cond do
        left_end == nil and right_end == nil ->
          # Both empty pages
          {:cont, acc}

        left_end == nil ->
          # Left empty, right non-empty - valid
          {:cont, acc}

        right_end == nil ->
          # Left non-empty, right empty - invalid (empty pages should not be in tree)
          {:halt, {:error, "Empty page #{right_id} found after non-empty page #{left_id} in tree order"}}

        left_end < right_end ->
          # Strictly ascending - correct
          {:cont, acc}

        left_end >= right_end ->
          {:halt,
           {:error,
            "Tree ordering violation: page #{left_id} end_key #{inspect(left_end, base: :hex)} >= page #{right_id} end_key #{inspect(right_end, base: :hex)}"}}
      end
    end)
  end

  @doc """
  Validates page chain integrity:
  - Chain starts at leftmost page (referenced by page 0)
  - Chain terminates with next_id = 0
  - No cycles in the chain
  - All pages in page_map are reachable through the chain
  """
  def check_page_chain_integrity(index) do
    with :ok <- check_chain_starts_correctly(index),
         :ok <- check_chain_terminates(index) do
      check_all_pages_reachable(index)
    end
  end

  defp check_chain_starts_correctly(%{page_map: %{0 => {_page_0, leftmost_id}} = page_map}) do
    if leftmost_id == 0 do
      # Empty index - valid
      :ok
    else
      case Map.get(page_map, leftmost_id) do
        nil ->
          {:error, "Page 0 references non-existent leftmost page #{leftmost_id}"}

        _leftmost_page ->
          :ok
      end
    end
  end

  defp check_chain_starts_correctly(_index), do: {:error, "Page 0 (leftmost marker) not found in page_map"}

  defp check_chain_terminates(index) do
    {_page_0, leftmost_id} = Map.get(index.page_map, 0)

    if leftmost_id == 0 do
      # Empty index
      :ok
    else
      case follow_chain_to_end(index, leftmost_id, MapSet.new()) do
        {:ok, _visited} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp follow_chain_to_end(index, current_id, visited) do
    if MapSet.member?(visited, current_id) do
      {:error, "Cycle detected in page chain at page #{current_id}"}
    else
      process_chain_page(index, current_id, visited)
    end
  end

  defp process_chain_page(index, current_id, visited) do
    case Map.get(index.page_map, current_id) do
      nil -> {:error, "Chain references non-existent page #{current_id}"}
      {page, next_id} -> handle_chain_page(index, current_id, page, next_id, visited)
    end
  end

  defp handle_chain_page(index, current_id, _page, next_id, visited) do
    new_visited = MapSet.put(visited, current_id)

    case next_id do
      0 -> {:ok, new_visited}
      _ -> follow_chain_to_end(index, next_id, new_visited)
    end
  end

  defp check_all_pages_reachable(index) do
    # Get all non-zero page IDs from page_map
    all_page_ids = Map.keys(index.page_map) -- [0]

    # Get all page IDs reachable through chain
    {_page_0, leftmost_id} = Map.get(index.page_map, 0)

    reachable_ids =
      if leftmost_id == 0 do
        MapSet.new()
      else
        case collect_chain_pages(index, leftmost_id, MapSet.new()) do
          {:ok, reachable} -> reachable
          # Error will be caught elsewhere
          {:error, _} -> MapSet.new()
        end
      end

    unreachable = Enum.reject(all_page_ids, &MapSet.member?(reachable_ids, &1))

    if unreachable == [] do
      :ok
    else
      {:error, "Pages unreachable through chain: #{inspect(unreachable)}"}
    end
  end

  defp collect_chain_pages(index, current_id, collected) do
    if MapSet.member?(collected, current_id) do
      {:error, "Cycle detected"}
    else
      collect_page_from_chain(index, current_id, collected)
    end
  end

  defp collect_page_from_chain(index, current_id, collected) do
    case Map.get(index.page_map, current_id) do
      nil -> {:error, "Missing page #{current_id}"}
      {_page, next_id} -> process_collected_page(index, current_id, next_id, collected)
    end
  end

  defp process_collected_page(index, current_id, next_id, collected) do
    new_collected = MapSet.put(collected, current_id)

    case next_id do
      0 -> {:ok, new_collected}
      _ -> collect_chain_pages(index, next_id, new_collected)
    end
  end

  @doc """
  Validates that chain order matches tree order.
  """
  def check_chain_tree_consistency(index) do
    chain_order = get_chain_order(index)
    tree_order = get_all_page_ids_from_tree(index.tree)

    if chain_order == tree_order do
      :ok
    else
      {:error, "Chain order #{inspect(chain_order)} does not match tree order #{inspect(tree_order)}"}
    end
  end

  defp get_chain_order(index) do
    collect_chain_order(index.page_map, 0, [])
  end

  defp collect_chain_order(page_map, page_id, collected_page_ids) do
    {_page, next_id} = Map.fetch!(page_map, page_id)
    updated_collected = [page_id | collected_page_ids]

    if next_id == 0 do
      Enum.reverse(updated_collected)
    else
      collect_chain_order(page_map, next_id, updated_collected)
    end
  end

  @doc """
  Validates that following the page chain yields keys in strictly ascending order.
  """
  def check_chain_key_ordering(index) do
    case Map.get(index.page_map, 0) do
      nil -> {:error, "Page 0 must always exist as the leftmost page"}
      page_0_tuple -> validate_chain_key_ordering(index, page_0_tuple)
    end
  end

  defp validate_chain_key_ordering(index, {page_0, next_id}) do
    all_keys = collect_all_keys_from_chain(index, page_0, next_id)

    case length(all_keys) do
      count when count <= 1 -> :ok
      _ -> check_keys_in_order(all_keys)
    end
  end

  defp check_keys_in_order(all_keys) do
    case find_out_of_order_keys(all_keys) do
      nil -> :ok
      {idx, prev_key, bad_key} -> keys_out_of_order_error(idx, prev_key, bad_key)
    end
  end

  defp keys_out_of_order_error(idx, prev_key, bad_key) do
    {:error,
     "Keys out of order in chain at position #{idx}: #{inspect(prev_key, base: :hex)} >= #{inspect(bad_key, base: :hex)}"}
  end

  defp collect_all_keys_from_chain(index, page_0, next_id) do
    if Page.empty?(page_0) do
      # Page 0 is empty sentinel - start from the page it points to
      collect_keys_from_chain(index, next_id, [])
    else
      # Page 0 contains data - start from page 0 itself
      collect_keys_from_chain(index, 0, [])
    end
  end

  defp collect_keys_from_chain(_index, 0, acc), do: Enum.reverse(acc)

  defp collect_keys_from_chain(index, current_id, acc) do
    case Map.get(index.page_map, current_id) do
      # Broken chain, but that's checked elsewhere
      nil ->
        Enum.reverse(acc)

      {page, next_id} ->
        page_keys = Page.keys(page)
        collect_keys_from_chain(index, next_id, Enum.reverse(page_keys, acc))
    end
  end

  defp find_out_of_order_keys(keys) do
    keys
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.with_index()
    |> Enum.find_value(fn {[prev_key, curr_key], idx} ->
      if prev_key >= curr_key, do: {idx + 1, prev_key, curr_key}
    end)
  end

  @doc """
  Validates that tree and page_map are consistent.
  """
  def check_index_consistency(index) do
    tree_page_ids = get_all_page_ids_from_tree(index.tree)

    # Get all pages that should be in tree: non-empty pages + page 0 (always in tree even if empty)
    tree_eligible_page_ids =
      index.page_map
      |> Enum.filter(fn {page_id, {page, _next_id}} ->
        page_id == 0 or not Page.empty?(page)
      end)
      |> Enum.map(fn {page_id, {_page, _next_id}} -> page_id end)

    tree_page_set = MapSet.new(tree_page_ids)
    map_page_set = MapSet.new(tree_eligible_page_ids)

    if MapSet.equal?(tree_page_set, map_page_set) do
      :ok
    else
      {:error, "Tree pages #{inspect(tree_page_set)} don't match tree-eligible page_map pages #{inspect(map_page_set)}"}
    end
  end

  @doc """
  Validates that all pages are within the size limit (≤256 keys per page).
  """
  def check_page_size_limits(index) do
    max_keys_per_page = 256

    Enum.reduce_while(index.page_map, :ok, fn {page_id, {page, _next_id}}, acc ->
      key_count = Page.key_count(page)

      if key_count <= max_keys_per_page do
        {:cont, acc}
      else
        {:halt, {:error, "Page #{page_id} exceeds size limit: #{key_count} keys > #{max_keys_per_page} keys"}}
      end
    end)
  end

  @doc """
  Asserts all invariants for testing. Calls `flunk/1` if any invariant fails.
  This is a test-friendly version that integrates with ExUnit.
  """
  def assert_all_invariants(index) do
    case check_all_invariants(index) do
      :ok -> :ok
      {:error, reason} -> ExUnit.Assertions.flunk(reason)
    end
  end

  @doc """
  Asserts all invariants including comprehensive checks for testing.
  Calls `flunk/1` if any invariant fails.
  """
  def assert_all_invariants_comprehensive(index) do
    case check_all_invariants_comprehensive(index) do
      :ok -> :ok
      {:error, reason} -> ExUnit.Assertions.flunk(reason)
    end
  end

  @doc """
  Verifies that the index contains exactly the expected keys, no more, no less.
  Returns :ok if key sets match, or {:error, reason} describing the mismatch.
  """
  @spec check_key_completeness(Index.t(), [Bedrock.key()]) :: :ok | {:error, String.t()}
  def check_key_completeness(index, expected_keys) do
    actual_keys = extract_all_keys_from_index(index)
    expected_sorted = Enum.sort(expected_keys)
    actual_sorted = Enum.sort(actual_keys)

    cond do
      actual_sorted == expected_sorted -> :ok
      length(actual_keys) != length(expected_keys) -> key_count_mismatch_error(expected_keys, actual_keys)
      true -> analyze_key_differences(expected_sorted, actual_sorted)
    end
  end

  defp key_count_mismatch_error(expected_keys, actual_keys) do
    {:error, "Key count mismatch: expected #{length(expected_keys)} keys, found #{length(actual_keys)} keys"}
  end

  defp analyze_key_differences(expected_sorted, actual_sorted) do
    missing_keys = expected_sorted -- actual_sorted
    extra_keys = actual_sorted -- expected_sorted

    case {missing_keys, extra_keys} do
      {[], []} -> {:error, "Keys are present but not in expected order"}
      {missing, []} -> missing_keys_error(missing)
      {[], extra} -> extra_keys_error(extra)
      {missing, extra} -> mixed_keys_error(missing, extra)
    end
  end

  defp missing_keys_error(missing) do
    suffix = if length(missing) > 5, do: "...", else: ""
    {:error, "Missing keys: #{inspect(Enum.take(missing, 5), base: :hex)}#{suffix}"}
  end

  defp extra_keys_error(extra) do
    suffix = if length(extra) > 5, do: "...", else: ""
    {:error, "Extra keys: #{inspect(Enum.take(extra, 5), base: :hex)}#{suffix}"}
  end

  defp mixed_keys_error(missing, extra) do
    {:error,
     "Missing keys: #{inspect(Enum.take(missing, 3), base: :hex)}..., Extra keys: #{inspect(Enum.take(extra, 3), base: :hex)}..."}
  end

  @doc """
  Asserts that the index contains exactly the expected keys.
  Calls `flunk/1` if keys don't match exactly.
  """
  def assert_key_completeness(index, expected_keys) do
    case check_key_completeness(index, expected_keys) do
      :ok -> :ok
      {:error, reason} -> ExUnit.Assertions.flunk(reason)
    end
  end

  defp extract_all_keys_from_index(index) do
    case Map.get(index.page_map, 0) do
      nil -> []
      {page_0, next_id} -> extract_keys_from_page_0(index, page_0, next_id)
    end
  end

  defp extract_keys_from_page_0(index, page_0, next_id) do
    if Page.empty?(page_0) do
      extract_keys_from_empty_page_0(index, next_id)
    else
      collect_keys_from_chain(index, 0, [])
    end
  end

  defp extract_keys_from_empty_page_0(index, leftmost_id) do
    case leftmost_id do
      0 -> []
      _ -> collect_keys_from_chain(index, leftmost_id, [])
    end
  end

  @doc """
  Pretty prints all invariant violations for debugging.
  """
  def debug_invariants(index) do
    checks = [
      {"Page key ordering", &check_page_key_ordering/1},
      {"Tree ordering", &check_tree_ordering/1},
      {"Page chain integrity", &check_page_chain_integrity/1},
      {"Chain-tree consistency", &check_chain_tree_consistency/1},
      {"Index consistency", &check_index_consistency/1},
      {"Page size limits", &check_page_size_limits/1}
    ]

    IO.puts("=== INVARIANT CHECK RESULTS ===")

    Enum.each(checks, fn {name, check_fn} ->
      case check_fn.(index) do
        :ok ->
          IO.puts("✅ #{name}: PASS")

        {:error, reason} ->
          IO.puts("❌ #{name}: FAIL - #{reason}")
      end
    end)

    IO.puts("\n=== DEBUG INFO ===")
    debug_print_structure(index)
  end

  defp debug_print_structure(index) do
    IO.puts("Page map:")

    index.page_map
    |> Enum.sort_by(fn {page_id, _} -> page_id end)
    |> Enum.each(fn {page_id, {page, next_id}} ->
      start_key = Page.left_key(page)
      end_key = Page.right_key(page)
      key_count = Page.key_count(page)

      IO.puts(
        "  Page #{page_id}: #{key_count} keys, range #{inspect(start_key, base: :hex)}..#{inspect(end_key, base: :hex)}, next=#{next_id}"
      )
    end)

    tree_order = get_all_page_ids_from_tree(index.tree)
    IO.puts("Tree order: #{inspect(tree_order)}")

    chain_order = get_chain_order(index)
    IO.puts("Chain order: #{inspect(chain_order)}")
  end
end
