defmodule Bedrock.DataPlane.Storage.Olivine.IndexUpdate do
  @moduledoc """
  Tracks mutation state during index updates.
  Contains the base index plus mutation tracking fields.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree

  defmodule PageAllocator do
    @moduledoc """
    Manages page ID allocation during index updates.
    """

    @type t :: %__MODULE__{
            max_page_id: Page.id(),
            free_page_ids: [Page.id()]
          }

    defstruct [:max_page_id, :free_page_ids]

    @spec new(Page.id(), [Page.id()]) :: t()
    def new(max_page_id, free_page_ids) do
      %__MODULE__{max_page_id: max_page_id, free_page_ids: free_page_ids}
    end

    @spec allocate_id(t()) :: {Page.id(), t()}
    def allocate_id(%__MODULE__{free_page_ids: [id | rest]} = allocator) do
      {id, %{allocator | free_page_ids: rest}}
    end

    def allocate_id(%__MODULE__{free_page_ids: [], max_page_id: max_id} = allocator) do
      new_id = max_id + 1
      {new_id, %{allocator | max_page_id: new_id}}
    end

    @spec recycle_page_id(t(), Page.id()) :: t()
    def recycle_page_id(%__MODULE__{free_page_ids: free_ids} = allocator, page_id) do
      # Add to free list if not already present (maintain sorted order for efficiency)
      updated_free_ids = add_unique_page_id(free_ids, page_id)
      %{allocator | free_page_ids: updated_free_ids}
    end

    # Helper function for maintaining sorted unique list of free page IDs
    @spec add_unique_page_id([Page.id()], Page.id()) :: [Page.id()]
    def add_unique_page_id(id_list, id), do: add_unique_page_id(id_list, id, [])

    @spec add_unique_page_id([Page.id()], Page.id(), [Page.id()]) :: [Page.id()]
    defp add_unique_page_id([], id, acc), do: Enum.reverse(acc, [id])
    defp add_unique_page_id([h | t], id, acc) when id < h, do: Enum.reverse(acc, [id, h | t])
    defp add_unique_page_id([h | _t] = list, id, acc) when id == h, do: Enum.reverse(acc, list)
    defp add_unique_page_id([h | t], id, acc), do: add_unique_page_id(t, id, [h | acc])
  end

  @type t :: %__MODULE__{
          index: Index.t(),
          version: Bedrock.version(),
          page_allocator: PageAllocator.t(),
          modified_page_ids: [Page.id()],
          pending_operations: %{Page.id() => %{Bedrock.key() => {:set, Bedrock.version()} | :clear}}
        }

  defstruct [
    :index,
    :version,
    :page_allocator,
    :modified_page_ids,
    :pending_operations
  ]

  @doc """
  Creates an IndexUpdate for mutation tracking from an Index, version, and page allocator.
  """
  @spec new(Index.t(), Bedrock.version(), PageAllocator.t()) :: t()
  def new(%Index{} = index, version, page_allocator) do
    %__MODULE__{
      index: index,
      version: version,
      page_allocator: page_allocator,
      modified_page_ids: [],
      pending_operations: %{}
    }
  end

  @doc """
  Extracts the final Index from an IndexUpdate, discarding mutation tracking.
  """
  @spec to_index(t()) :: Index.t()
  def to_index(%__MODULE__{index: index}), do: index

  @doc """
  Extracts the PageAllocator from an IndexUpdate.
  """
  @spec to_page_allocator(t()) :: PageAllocator.t()
  def to_page_allocator(%__MODULE__{page_allocator: page_allocator}), do: page_allocator

  @doc """
  Gets the modified pages from the IndexUpdate as a list of pages.
  """
  @spec modified_pages(t()) :: [Page.t()]
  def modified_pages(%__MODULE__{index: index, modified_page_ids: modified_page_ids}) do
    Enum.map(modified_page_ids, fn page_id ->
      Map.fetch!(index.page_map, page_id)
    end)
  end

  @doc """
  Stores all modified pages from the IndexUpdate in the database.
  Returns the IndexUpdate for chaining.
  """
  @spec store_modified_pages(t(), Database.t()) :: t()
  def store_modified_pages(%__MODULE__{version: version} = index_update, database) do
    pages = modified_pages(index_update)
    :ok = Database.store_modified_pages(database, version, pages)
    index_update
  end

  @doc """
  Applies mutations to this IndexUpdate, returning the updated IndexUpdate.
  """
  @spec apply_mutations(t(), [Tx.mutation()], Database.t()) :: t()
  def apply_mutations(%__MODULE__{version: version} = update, mutations, database) do
    Enum.reduce(mutations, update, fn mutation, update_acc ->
      apply_single_mutation(mutation, version, update_acc, database)
    end)
  end

  @doc """
  Process all pending operations for each modified page using sorted merge.
  """
  @spec process_pending_operations(t()) :: t()
  def process_pending_operations(%{pending_operations: pending_operations} = index_update) do
    # Process each page that has pending operations using sorted merge
    Enum.reduce(pending_operations, index_update, fn {page_id, operations}, index_update ->
      process_page_operations(index_update, page_id, operations)
    end)
  end

  @spec apply_single_mutation(Tx.mutation(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_single_mutation(mutation, new_version, update_data, database) do
    case mutation do
      {:set, key, value} ->
        apply_set_mutation(key, value, new_version, update_data, database)

      {:clear, key} ->
        apply_clear_mutation(key, new_version, update_data)

      {:clear_range, start_key, end_key} ->
        apply_range_clear_mutation(start_key, end_key, new_version, update_data)
    end
  end

  @spec apply_set_mutation(binary(), binary(), Bedrock.version(), t(), Database.t()) :: t()
  defp apply_set_mutation(key, value, new_version, %__MODULE__{} = update_data, database) do
    target_page_id = Tree.page_for_insertion(update_data.index.tree, key)

    # Store the value in database (handles lookaside buffer internally)
    :ok = Database.store_value(database, key, new_version, value)

    # Add set operation to pending operations for this page (last-writer-wins)
    set_operation = {:set, new_version}

    updated_operations =
      Map.update(update_data.pending_operations, target_page_id, %{key => set_operation}, fn page_ops ->
        Map.put(page_ops, key, set_operation)
      end)

    %{update_data | pending_operations: updated_operations}
  end

  @spec apply_clear_mutation(binary(), Bedrock.version(), t()) :: t()
  defp apply_clear_mutation(key, _new_version, %__MODULE__{} = update_data) do
    case Tree.page_for_key(update_data.index.tree, key) do
      nil ->
        # Key doesn't exist, no operation needed
        update_data

      page_id ->
        # Add clear operation to pending operations for this page (last-writer-wins)
        clear_operation = :clear

        updated_operations =
          Map.update(update_data.pending_operations, page_id, %{key => clear_operation}, fn page_ops ->
            Map.put(page_ops, key, clear_operation)
          end)

        %{update_data | pending_operations: updated_operations}
    end
  end

  @spec apply_range_clear_mutation(binary(), binary(), Bedrock.version(), t()) :: t()
  defp apply_range_clear_mutation(start_key, end_key, _new_version, %__MODULE__{} = update_data) do
    overlapping_page_ids = Tree.page_ids_in_range(update_data.index.tree, start_key, end_key)

    case overlapping_page_ids do
      [] ->
        # No pages in range, nothing to do
        update_data

      [single_page_id] ->
        # Only one page overlaps, expand range to individual clear operations
        page = Map.fetch!(update_data.index.page_map, single_page_id)
        keys_to_clear = get_keys_in_range(page, start_key, end_key)

        # Add individual clear operations for each key in range
        updated_operations = add_clear_operations(update_data.pending_operations, single_page_id, keys_to_clear)

        %{update_data | pending_operations: updated_operations}

      multiple_page_ids ->
        # Multiple pages: first and last get individual clears for keys in range, middle pages get deleted
        first_page_id = List.first(multiple_page_ids)
        last_page_id = List.last(multiple_page_ids)
        middle_page_ids = multiple_page_ids |> Enum.drop(1) |> Enum.drop(-1)

        # Handle first page - get keys from start_key to end of page
        first_page = Map.fetch!(update_data.index.page_map, first_page_id)
        first_page_end_key = Page.right_key(first_page)
        first_clear_end = min(end_key, first_page_end_key || end_key)
        first_keys_to_clear = get_keys_in_range(first_page, start_key, first_clear_end)

        # Handle last page - get keys from start of page to end_key
        last_page = Map.fetch!(update_data.index.page_map, last_page_id)
        last_page_start_key = Page.left_key(last_page)
        last_clear_start = max(start_key, last_page_start_key || start_key)
        last_keys_to_clear = get_keys_in_range(last_page, last_clear_start, end_key)

        # Remove middle pages immediately and clean up their pending operations
        updated_tree =
          Enum.reduce(middle_page_ids, update_data.index.tree, fn page_id, tree_acc ->
            page = Map.fetch!(update_data.index.page_map, page_id)
            Tree.remove_page_from_tree(tree_acc, page)
          end)

        updated_page_map =
          Enum.reduce(middle_page_ids, update_data.index.page_map, fn page_id, map_acc ->
            Map.delete(map_acc, page_id)
          end)

        # Recycle the deleted middle page IDs
        updated_allocator =
          Enum.reduce(middle_page_ids, update_data.page_allocator, fn page_id, allocator ->
            PageAllocator.recycle_page_id(allocator, page_id)
          end)

        updated_operations =
          update_data.pending_operations
          |> Map.drop(middle_page_ids)
          |> add_clear_operations(first_page_id, first_keys_to_clear)
          |> add_clear_operations(last_page_id, last_keys_to_clear)

        updated_index = %{update_data.index | tree: updated_tree, page_map: updated_page_map}

        %{
          update_data
          | index: updated_index,
            page_allocator: updated_allocator,
            pending_operations: updated_operations
        }
    end
  end

  # Helper function to get keys within a range from a page
  @spec get_keys_in_range(Page.t(), Bedrock.key(), Bedrock.key()) :: [Bedrock.key()]
  defp get_keys_in_range(page, start_key, end_key) do
    page
    |> Page.key_versions()
    |> Enum.filter(fn {key, _version} -> key >= start_key and key <= end_key end)
    |> Enum.map(fn {key, _version} -> key end)
  end

  # Helper function to add clear operations for keys on a specific page
  defp add_clear_operations(operations, page_id, keys_to_clear) do
    Enum.reduce(keys_to_clear, operations, fn key, ops_acc ->
      Map.update(ops_acc, page_id, %{key => :clear}, &Map.put(&1, key, :clear))
    end)
  end

  # Helper functions for processing pending operations

  @spec process_page_operations(t(), Page.id(), %{Bedrock.key() => {:set, Bedrock.version()} | :clear}) :: t()
  defp process_page_operations(%__MODULE__{} = update_data, page_id, operations) do
    page_binary = Map.fetch!(update_data.index.page_map, page_id)

    # Apply operations and encode directly to binary (no intermediate struct)
    updated_page_binary = Page.apply_operations(page_binary, operations)

    # Handle page updates, splitting, or deletion
    cond do
      Page.empty?(updated_page_binary) ->
        # Page is now empty, remove it and recycle the page ID
        updated_index = %{
          update_data.index
          | tree: Tree.remove_page_from_tree(update_data.index.tree, page_binary),
            page_map: Map.delete(update_data.index.page_map, page_id)
        }

        updated_allocator = PageAllocator.recycle_page_id(update_data.page_allocator, page_id)

        %{
          update_data
          | index: updated_index,
            page_allocator: updated_allocator
        }

      Page.key_count(updated_page_binary) > 256 ->
        # Page needs splitting
        handle_page_split(update_data, page_id, page_binary, updated_page_binary)

      true ->
        # Normal page update
        updated_index = %{
          update_data.index
          | tree: Tree.update_page_in_tree(update_data.index.tree, page_binary, updated_page_binary),
            page_map: Map.put(update_data.index.page_map, page_id, updated_page_binary)
        }

        %{
          update_data
          | index: updated_index,
            modified_page_ids: add_unique_page_id(update_data.modified_page_ids, page_id)
        }
    end
  end

  # Helper function that delegates to PageAllocator's add_unique_page_id
  @spec add_unique_page_id([Page.id()], Page.id()) :: [Page.id()]
  defp add_unique_page_id(id_list, id) do
    PageAllocator.add_unique_page_id(id_list, id)
  end

  @spec handle_page_split(t(), Page.id(), Page.t(), Page.t()) :: t()
  defp handle_page_split(%__MODULE__{} = update_data, page_id, original_page_binary, updated_page_binary) do
    # Split the page using binary format
    key_count = Page.key_count(updated_page_binary)
    {new_page_id, updated_allocator} = PageAllocator.allocate_id(update_data.page_allocator)

    {left_page_binary, right_page_binary} = Page.split_page(updated_page_binary, div(key_count, 2), new_page_id)

    updated_tree =
      update_data.index.tree
      |> Tree.remove_page_from_tree(original_page_binary)
      |> Tree.add_page_to_tree(left_page_binary)
      |> Tree.add_page_to_tree(right_page_binary)

    updated_page_map =
      update_data.index.page_map
      |> Map.put(Page.id(left_page_binary), left_page_binary)
      |> Map.put(Page.id(right_page_binary), right_page_binary)
      |> then(fn map ->
        if not Page.has_id?(left_page_binary, page_id) and not Page.has_id?(right_page_binary, page_id) do
          Map.delete(map, page_id)
        else
          map
        end
      end)

    # If the original page ID is not reused in either split page, recycle it
    final_allocator =
      if not Page.has_id?(left_page_binary, page_id) and not Page.has_id?(right_page_binary, page_id) do
        PageAllocator.recycle_page_id(updated_allocator, page_id)
      else
        updated_allocator
      end

    updated_modified_page_ids =
      update_data.modified_page_ids
      |> add_unique_page_id(Page.id(left_page_binary))
      |> add_unique_page_id(Page.id(right_page_binary))

    updated_index = %{update_data.index | tree: updated_tree, page_map: updated_page_map}

    %{
      update_data
      | index: updated_index,
        page_allocator: final_allocator,
        modified_page_ids: updated_modified_page_ids
    }
  end
end
