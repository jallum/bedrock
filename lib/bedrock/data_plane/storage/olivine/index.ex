defmodule Bedrock.DataPlane.Storage.Olivine.Index do
  @moduledoc """
  B-tree-like index structure for the Olivine storage driver.

  ## Structure

  The index consists of:
  - **Tree**: gb_trees keyed by page `last_key`, storing `page_id`
  - **Page Map**: Map of page_id → Page structs containing key-value pairs
  - **Page Chain**: Linked list of pages via `next_id` pointers, starting from page 0
  - **Gap-Free Design**: Pages cover entire keyspace with no gaps between them

  ## Critical Invariants

  1. **Page 0 Existence**: Page 0 must always exist as the leftmost page
  2. **Tree Ordering**: Pages in tree order have strictly ascending `last_key` values
  3. **Chain Integrity**: Page chain starts at 0, terminates at 0, covers all pages
  4. **Key Ordering**: Following page chain yields all keys in strictly ascending order
  5. **Non-overlapping**: Pages have non-overlapping key ranges
  6. **Page Keys**: Within each page, keys are sorted; `first_key <= last_key`

  ## Page Chain Reconstruction

  When pages are added or modified, the page chain is automatically rebuilt from tree
  ordering to maintain consistency. This ensures proper key ordering across page boundaries
  and prevents chain corruption during concurrent operations and page splits.

  ## Multi-Split Support

  Pages can be split into multiple pages when they exceed size limits. The original page
  ID is preserved for the first split to maintain consistency, especially for page 0.
  Chain pointers are updated to maintain traversal integrity.

  ## Algorithms

  ### Key Lookup
  1. Use `Tree.page_for_key(key)` to find page containing key
  2. Search within page for exact key match

  ### Mutation Application
  1. Use `Tree.page_for_key(key)` to find target page (no gaps exist)
  2. Apply operations to page, maintaining sorted order within page

  ### Page Splitting
  1. When page exceeds 256 keys, split at midpoint (or into multiple pages)
  2. Left half keeps original page_id (preserves page 0 as leftmost)
  3. Right halves get new page_ids
  4. Update tree entries and rebuild page chain from tree ordering
  5. Chain integrity is automatically maintained through reconstruction

  ### Range Clearing
  1. Find all pages intersecting the range using tree
  2. For single page: clear keys within range
  3. For multiple pages: delete middle pages, clear edges
  4. Chain integrity maintained through automatic reconstruction
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager

  # Page sizing constant
  @max_keys_per_page 256

  @doc "Returns the maximum number of keys allowed per page before splitting"
  def max_keys_per_page, do: @max_keys_per_page

  @type operation :: IndexManager.operation()

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(),
          page_map: map(),
          min_key: Bedrock.key(),
          max_key: Bedrock.key()
        }

  defstruct [
    :tree,
    :page_map,
    :min_key,
    :max_key
  ]

  @doc """
  Creates a new empty Index with an initial page covering the entire keyspace.
  """
  @spec new() :: t()
  def new do
    initial_page = Page.new(0, [])
    # Add page 0 to tree with empty key (covers beginning of keyspace)
    initial_tree = Tree.add_page_to_tree(:gb_trees.empty(), initial_page)
    initial_page_map = %{0 => {initial_page, 0}}

    %__MODULE__{
      tree: initial_tree,
      page_map: initial_page_map,
      min_key: <<0xFF, 0xFF>>,
      max_key: <<>>
    }
  end

  @spec locator_for_key(t(), Bedrock.key()) ::
          {:ok, Page.t(), Database.locator()} | {:error, :not_found}
  def locator_for_key(index, key) do
    page = page_for_key(index, key)

    case Page.locator_for_key(page, key) do
      {:ok, locator} -> {:ok, page, locator}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  @doc """
  Loads an Index from the database by traversing the page chain and building the tree structure.
  Returns {:ok, index, max_id, free_ids, total_key_count} or an error.
  """
  @spec load_from(Database.t()) ::
          {:ok, t(), Page.id(), [Page.id()], non_neg_integer()}
          | {:error, :missing_pages}
  def load_from({_data_db, index_db}) do
    index_db
    |> IndexDatabase.load_durable_version()
    |> case do
      {:error, :not_found} ->
        {:ok, new(), 0, [], 0}

      {:ok, durable_version} ->
        # Start with page 0 as the only needed page
        needed_page_ids = MapSet.new([0])

        # Load pages starting from the durable version
        case load_needed_pages(index_db, %{}, %{}, needed_page_ids, durable_version) do
          {:ok, final_page_map} ->
            build_index_from_page_map(final_page_map)

          {:error, :missing_pages} ->
            {:error, :missing_pages}
        end
    end
  end

  # Load needed pages iteratively from older version blocks
  # page_map: final result map (only needed pages)
  # all_pages_seen: cumulative view of all pages (newest version wins)
  @spec load_needed_pages(
          IndexDatabase.t(),
          page_map :: %{Page.id() => {Page.t(), Page.id()}},
          all_pages_seen :: %{Page.id() => {Page.t(), Page.id()}},
          needed_page_ids :: MapSet.t(Page.id()),
          Bedrock.version()
        ) :: {:ok, %{Page.id() => {Page.t(), Page.id()}}} | {:error, :missing_pages}
  defp load_needed_pages(index_db, page_map, all_pages_seen, needed_page_ids, current_version) do
    if MapSet.size(needed_page_ids) == 0 do
      # No more pages needed
      {:ok, page_map}
    else
      load_needed_pages_from_version(index_db, page_map, all_pages_seen, needed_page_ids, current_version)
    end
  end

  defp load_needed_pages_from_version(index_db, page_map, all_pages_seen, needed_page_ids, current_version) do
    case IndexDatabase.load_page_block(index_db, current_version) do
      {:ok, version_pages, next_version} ->
        # Merge this version block with all pages seen (older pages don't override newer ones)
        updated_all_pages = Map.merge(version_pages, all_pages_seen)

        # Process each page in this version block
        {updated_page_map, updated_needed} =
          process_version_pages(version_pages, page_map, needed_page_ids, updated_all_pages)

        load_needed_pages(index_db, updated_page_map, updated_all_pages, updated_needed, next_version)

      {:error, :not_found} ->
        {:error, :missing_pages}
    end
  end

  defp process_version_pages(version_pages, page_map, needed_page_ids, updated_all_pages) do
    Enum.reduce(version_pages, {page_map, needed_page_ids}, fn
      {page_id, {_page, _next_id}}, {acc_map, acc_needed} ->
        if MapSet.member?(acc_needed, page_id) do
          process_needed_page(page_id, acc_map, acc_needed, updated_all_pages)
        else
          # This page is not needed - ignore it
          {acc_map, acc_needed}
        end
    end)
  end

  defp process_needed_page(page_id, acc_map, acc_needed, updated_all_pages) do
    # This page is needed - use the newest version from updated_all_pages
    {resolved_page, resolved_next_id} = Map.get(updated_all_pages, page_id)
    new_map = Map.put(acc_map, page_id, {resolved_page, resolved_next_id})
    new_needed = MapSet.delete(acc_needed, page_id)

    # Add the page's next_id to needed if it's not already in the result map
    final_needed =
      if resolved_next_id != 0 and not Map.has_key?(new_map, resolved_next_id) do
        MapSet.put(new_needed, resolved_next_id)
      else
        new_needed
      end

    {new_map, final_needed}
  end

  # Build final index structure from complete page map
  @spec build_index_from_page_map(%{Page.id() => {Page.t(), Page.id()}}) ::
          {:ok, t(), Page.id(), [Page.id()], non_neg_integer()}
  defp build_index_from_page_map(page_map) do
    tree = Tree.from_page_map(page_map)
    page_ids = page_map |> Map.keys() |> MapSet.new()
    max_id = if MapSet.size(page_ids) > 0, do: Enum.max(page_ids), else: 0
    free_ids = calculate_free_ids(max_id, page_ids)

    initial_page_map =
      if :gb_trees.is_empty(tree) and max_id == 0 do
        empty_page = Page.new(0, [])
        %{0 => {empty_page, 0}}
      else
        page_map
      end

    # Calculate min/max keys from the tree
    {min_key, max_key} = calculate_key_bounds(tree, initial_page_map)

    # Count total keys
    total_key_count =
      Enum.sum_by(initial_page_map, fn {_, {page, _next_id}} -> Page.key_count(page) end)

    index = %__MODULE__{
      tree: tree,
      page_map: initial_page_map,
      min_key: min_key,
      max_key: max_key
    }

    {:ok, index, max_id, free_ids, total_key_count}
  end

  defp calculate_free_ids(0, _all_existing_page_ids), do: []

  defp calculate_free_ids(max_id, all_existing_page_ids) do
    0..max_id
    |> MapSet.new()
    |> MapSet.difference(all_existing_page_ids)
    |> Enum.sort()
  end

  @doc """
  Gets a page by its ID from the index.
  Raises if the page is not found.
  """
  @spec get_page!(t(), Page.id()) :: Page.t()
  def get_page!(%__MODULE__{page_map: page_map}, page_id) do
    {page, _next_id} = Map.fetch!(page_map, page_id)
    page
  end

  @doc """
  Gets a page and its cached next_id from the index.
  Returns {page_binary, next_id}.
  """
  @spec get_page_with_next_id!(t(), Page.id()) :: {Page.t(), Page.id()}
  def get_page_with_next_id!(%__MODULE__{page_map: page_map}, page_id) do
    Map.fetch!(page_map, page_id)
  end

  @doc """
  Finds the page containing the given key in this index.
  """
  @spec page_for_key(t(), Bedrock.key()) :: Page.t()
  def page_for_key(%__MODULE__{tree: tree, page_map: page_map}, key) do
    page_id = Tree.page_for_key(tree, key)
    {page, _next_id} = Map.fetch!(page_map, page_id)
    page
  end

  @doc """
  Finds all pages that contain keys within the given range in this index.
  Returns {:ok, [Page.t()]} with the list of pages (may be empty).
  """
  @spec pages_for_range(t(), Bedrock.key(), Bedrock.key()) :: {:ok, [Page.t()]}
  def pages_for_range(%__MODULE__{tree: tree, page_map: page_map}, start_key, end_key) do
    # Find the first page that could contain start_key
    first_page_id = Tree.page_for_key(tree, start_key)

    # Walk the page chain until we reach a page whose last_key >= end_key
    page_ids = collect_pages_in_range(page_map, first_page_id, end_key, [])

    pages =
      Enum.map(page_ids, fn page_id ->
        {page, _next_id} = Map.fetch!(page_map, page_id)
        page
      end)

    {:ok, pages}
  end

  defp collect_pages_in_range(page_map, page_id, end_key, collected_page_ids) do
    {page, next_id} = Map.fetch!(page_map, page_id)
    updated_collected = [page_id | collected_page_ids]

    last_key = Page.right_key(page)

    cond do
      last_key != nil and last_key >= end_key ->
        # This page covers the end of our range
        Enum.reverse(updated_collected)

      next_id == 0 ->
        # End of chain
        Enum.reverse(updated_collected)

      true ->
        # Continue to next page
        collect_pages_in_range(page_map, next_id, end_key, updated_collected)
    end
  end

  @doc """
  Removes a single page from the index.
  Updates both the tree structure and page_map.
  Returns the updated index.

  **Note**: Page 0 cannot be deleted as it must always exist as the leftmost page.
  Attempting to delete page 0 raises `ArgumentError`.
  """
  @spec delete_page(t(), Page.id()) :: t()
  def delete_page(_index, 0),
    do: raise(ArgumentError, "Cannot delete page 0 - it must always exist as the leftmost page")

  def delete_page(%__MODULE__{tree: tree, page_map: page_map} = index, page_id) do
    case Map.fetch(page_map, page_id) do
      {:ok, {page, _next_id}} ->
        updated_tree = Tree.remove_page_from_tree(tree, page)
        updated_page_map = Map.delete(page_map, page_id)
        %{index | tree: updated_tree, page_map: updated_page_map}

      :error ->
        index
    end
  end

  @doc """
  Splits a page into multiple pages when it contains too many keys.
  Ensures no page exceeds 256 keys while maintaining chain continuity.

  The original page ID is preserved for the first page in the chain.
  The last page points to the original page's next_id.
  """
  @spec multi_split_page(t(), Page.id(), Page.id(), Page.t(), [Page.id()]) :: t()
  def multi_split_page(index, original_page_id, original_next_id, updated_page, new_page_ids) do
    all_key_locators = Page.key_locators(updated_page)

    key_chunks = Enum.chunk_every(all_key_locators, @max_keys_per_page)

    all_page_ids = [original_page_id | new_page_ids]

    # Build proper chain: first -> second -> ... -> last -> original_next_id
    next_ids = new_page_ids ++ [original_next_id]

    new_page_tuples =
      key_chunks
      |> Enum.zip(all_page_ids)
      |> Enum.zip(next_ids)
      |> Enum.map(fn {{chunk_keys, page_id}, next_id} ->
        # next_id in binary doesn't matter, we use tuple
        page = Page.new(page_id, chunk_keys)
        {page, next_id}
      end)

    [first_page_tuple | remaining_page_tuples] = new_page_tuples

    {first_page, first_next_id} = first_page_tuple

    # Get the original page from page_map for the update operation
    {original_page, _} = Map.get(index.page_map, original_page_id)

    index_with_first_page_updated =
      index
      |> update_index_with_page(original_page, first_page, first_next_id)
      |> add_pages_batch(remaining_page_tuples)

    index_with_first_page_updated
  end

  # Unified helpers to eliminate tree/page_map update duplication

  @spec update_index_with_page(t(), Page.t(), Page.t(), Page.id()) :: t()
  defp update_index_with_page(%__MODULE__{tree: tree, page_map: page_map} = index, old_page, new_page, next_id) do
    updated_tree = Tree.update_page_in_tree(tree, old_page, new_page)
    updated_page_map = Map.put(page_map, Page.id(new_page), {new_page, next_id})
    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  @doc """
  Removes multiple pages from the index by their IDs.
  Updates both the tree structure and page_map.
  Returns the updated index.
  """
  @spec delete_pages(t(), [Page.id()]) :: t()
  def delete_pages(index, []), do: index

  def delete_pages(index, page_ids) do
    Enum.reduce(page_ids, index, fn page_id, index_acc ->
      index_acc
      |> delete_page_chain_update(page_id)
      |> delete_page_from_structures(page_id)
    end)
  end

  defp delete_page_from_structures(%__MODULE__{tree: tree, page_map: page_map} = index, page_id) do
    case Map.fetch(page_map, page_id) do
      {:ok, {page, _next_id}} ->
        updated_tree = Tree.remove_page_from_tree(tree, page)
        updated_page_map = Map.delete(page_map, page_id)
        %{index | tree: updated_tree, page_map: updated_page_map}

      :error ->
        index
    end
  end

  # Unified chain update helper - eliminates redundant logic and unnecessary Page.new calls
  @spec update_page_chain_pointer(t(), Page.id(), Page.id()) :: t()
  defp update_page_chain_pointer(%__MODULE__{page_map: page_map} = index, page_id, new_next_id) do
    case Map.get(page_map, page_id) do
      nil ->
        index

      {page, _old_next_id} ->
        updated_page_map = Map.put(page_map, page_id, {page, new_next_id})
        %{index | page_map: updated_page_map}
    end
  end

  @spec update_predecessor_chain_pointer(t(), Page.id(), Page.id()) :: t()
  defp update_predecessor_chain_pointer(%__MODULE__{tree: tree} = index, target_page_id, new_successor_id) do
    case find_predecessor_via_tree(tree, target_page_id) do
      nil ->
        # No predecessor found in tree - check if target is leftmost page (pointed to by page 0)
        handle_leftmost_page_deletion(index, target_page_id, new_successor_id)

      pred_id ->
        update_page_chain_pointer(index, pred_id, new_successor_id)
    end
  end

  # Handle deletion of leftmost page by updating page 0's pointer
  @spec handle_leftmost_page_deletion(t(), Page.id(), Page.id()) :: t()
  defp handle_leftmost_page_deletion(index, target_page_id, new_successor_id) do
    case Map.get(index.page_map, 0) do
      {_page_0, leftmost_id} when leftmost_id == target_page_id ->
        # Page 0 points to the target page - update it to point to successor
        update_page_chain_pointer(index, 0, new_successor_id)

      _ ->
        # Target page is not the leftmost - no predecessor to update
        index
    end
  end

  @spec delete_page_chain_update(t(), Page.id()) :: t()
  defp delete_page_chain_update(index, page_id) do
    {_page, successor_id} = Map.get(index.page_map, page_id)
    update_predecessor_chain_pointer(index, page_id, successor_id)
  end

  # Optimized batch addition - single tree operation for all pages
  @spec add_pages_batch(t(), [{Page.t(), Page.id()}]) :: t()
  defp add_pages_batch(%__MODULE__{tree: tree, page_map: page_map} = index, page_tuples) do
    # Extract pages for batch tree operation
    pages = Enum.map(page_tuples, fn {page, _next_id} -> page end)

    # Single batch tree update
    updated_tree = Enum.reduce(pages, tree, &Tree.add_page_to_tree(&2, &1))

    # Batch page_map update
    updated_page_map =
      Enum.reduce(page_tuples, page_map, fn {page, next_id}, acc_map ->
        Map.put(acc_map, Page.id(page), {page, next_id})
      end)

    %{index | tree: updated_tree, page_map: updated_page_map}
  end

  # Optimized tree traversal to find predecessor - single pass with state tracking
  @spec find_predecessor_via_tree(:gb_trees.tree(), Page.id()) :: Page.id() | nil
  defp find_predecessor_via_tree(tree, target_page_id) do
    iterator = :gb_trees.iterator(tree)
    find_predecessor_optimized(iterator, target_page_id, nil)
  end

  # Single pass through tree to find the page that comes just before target_page_id
  defp find_predecessor_optimized(iterator, target_page_id, last_page_id) do
    case :gb_trees.next(iterator) do
      {_last_key, page_id, next_iter} ->
        if page_id == target_page_id do
          # Found target page, return the previous page we saw
          last_page_id
        else
          # Keep track of this page as potential predecessor and continue
          find_predecessor_optimized(next_iter, target_page_id, page_id)
        end

      :none ->
        # Reached end without finding target page
        nil
    end
  end

  # Helper function to calculate min/max keys efficiently
  defp calculate_key_bounds(tree, page_map) do
    if :gb_trees.is_empty(tree) do
      {<<0xFF, 0xFF>>, <<>>}
    else
      # Get max from tree structure (O(log n) operation)
      {max_tree_key, _max_id} = :gb_trees.largest(tree)

      # For min_key, we need to find the actual smallest first_key across all pages
      # This is a one-time calculation during load
      min_key = find_minimum_first_key(page_map)

      {min_key, max_tree_key}
    end
  end

  # Find the minimum first_key across all pages (only used during load)
  defp find_minimum_first_key(page_map) when map_size(page_map) == 0, do: <<0xFF, 0xFF>>

  defp find_minimum_first_key(page_map) do
    page_map
    |> Enum.map(fn {_page_id, {page, _next_id}} -> Page.left_key(page) end)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> <<0xFF, 0xFF>>
      keys -> Enum.min(keys)
    end
  end

  @doc """
  Updates the index when a page is added, maintaining min/max key bounds.
  """
  @spec add_page(t(), Page.t()) :: t()
  def add_page(%__MODULE__{} = index, page) do
    updated_tree = Tree.add_page_to_tree(index.tree, page)

    # Update min/max keys based on the new page
    page_first_key = Page.left_key(page)
    page_last_key = Page.right_key(page)

    new_min_key =
      if page_first_key != nil and page_first_key < index.min_key do
        page_first_key
      else
        index.min_key
      end

    new_max_key =
      if page_last_key != nil and page_last_key > index.max_key do
        page_last_key
      else
        index.max_key
      end

    %{index | tree: updated_tree, min_key: new_min_key, max_key: new_max_key}
  end
end
