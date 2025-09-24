defmodule Bedrock.DataPlane.Storage.Olivine.IndexUpdate do
  @moduledoc """
  Tracks mutation state during index updates.

  ## Mutation Processing

  Mutations are distributed to pages based on key ranges:
  1. **Key Distribution**: Use `Tree.page_for_key/2` to find target page
  2. **Batch Processing**: Group operations by page_id for efficiency
  3. **Page Operations**: Apply all operations to a page at once
  4. **Automatic Splitting**: Split pages exceeding 256 keys
  5. **Chain Maintenance**: Update page chains when pages are added/removed

  ## Process Flow

  1. `apply_set_mutation/5`: Determines target page, stores value, queues operation
  2. `apply_clear_mutation/3`: Queues clear operation for existing key
  3. `apply_range_clear_mutation/4`: Handles range clears across multiple pages
  4. `process_pending_operations/1`: Applies all queued operations
  5. `finish/1`: Returns final index and page allocator state

  ## Page 0 Protection

  Page 0 is never deleted, only updated. When page 0 becomes empty,
  it remains in the index to preserve the leftmost chain entry point.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.Internal.Atomics

  # Constants for range operations
  @max_key <<255, 255, 255, 255>>

  @type t :: %__MODULE__{
          index: Index.t(),
          version: Bedrock.version(),
          database: Database.t(),
          page_allocator: PageAllocator.t(),
          modified_page_ids: MapSet.t(Page.id()),
          pending_operations: %{Page.id() => %{Bedrock.key() => {:set, Bedrock.version()} | :clear}}
        }

  defstruct [
    :index,
    :version,
    :database,
    :page_allocator,
    :modified_page_ids,
    :pending_operations
  ]

  @doc """
  Creates an IndexUpdate for mutation tracking from an Index, version, and page allocator.
  """
  @spec new(Index.t(), Bedrock.version(), PageAllocator.t(), Database.t()) :: t()
  def new(%Index{} = index, version, page_allocator, database) do
    %__MODULE__{
      index: index,
      version: version,
      database: database,
      page_allocator: page_allocator,
      modified_page_ids: MapSet.new(),
      pending_operations: %{}
    }
  end

  @doc """
  Finishes the IndexUpdate, returning the final Index, Database and PageAllocator.
  """
  def finish(%__MODULE__{index: index, page_allocator: page_allocator, database: database, version: version}) do
    new_database = Database.close_version(database, version)
    {index, new_database, page_allocator}
  end

  @doc """
  Stores all modified pages from the IndexUpdate in the database.
  Returns the IndexUpdate for chaining.
  """
  @spec store_modified_pages(t()) :: t()
  def store_modified_pages(%__MODULE__{version: version} = index_update) do
    pages = Enum.map(index_update.modified_page_ids, &Index.get_page_with_next_id!(index_update.index, &1))
    :ok = Database.store_modified_pages(index_update.database, version, pages)
    index_update
  end

  @doc """
  Applies mutations to this IndexUpdate, returning the updated IndexUpdate.
  """
  @spec apply_mutations(t(), Enumerable.t(Tx.mutation())) :: t()
  def apply_mutations(%__MODULE__{version: version} = index_update, mutations) do
    Enum.reduce(mutations, index_update, fn mutation, tracker_acc ->
      apply_single_mutation(mutation, version, tracker_acc)
    end)
  end

  @doc """
  Process all pending operations for each modified page using sorted merge.
  """
  @spec process_pending_operations(t()) :: t()
  def process_pending_operations(%{pending_operations: pending_operations} = index_update) do
    Enum.reduce(pending_operations, index_update, fn {page_id, page_mutations}, tracker_acc ->
      apply_mutations_to_page(tracker_acc, page_id, page_mutations)
    end)
  end

  @spec apply_single_mutation(Tx.mutation(), Bedrock.version(), t()) :: t()
  defp apply_single_mutation(mutation, target_version, index_update) do
    case mutation do
      {:set, key, value} ->
        apply_set_mutation(key, value, target_version, index_update)

      {:clear, key} ->
        apply_clear_mutation(key, target_version, index_update)

      {:clear_range, start_key, end_key} ->
        apply_range_clear_mutation(start_key, end_key, target_version, index_update)

      {:atomic, op, key, value} ->
        apply_atomic_mutation(op, key, value, target_version, index_update)
    end
  end

  @spec apply_set_mutation(binary(), binary(), Bedrock.version(), t()) :: t()
  defp apply_set_mutation(key, value, target_version, %__MODULE__{} = index_update) do
    insertion_page = Tree.page_for_key(index_update.index.tree, key)
    {:ok, locator, database} = Database.store_value(index_update.database, key, target_version, value)
    set_operation = {:set, locator}

    updated_pending_operations =
      Map.update(index_update.pending_operations, insertion_page, %{key => set_operation}, fn page_mutations ->
        Map.put(page_mutations, key, set_operation)
      end)

    %{index_update | pending_operations: updated_pending_operations, database: database}
  end

  @spec apply_clear_mutation(binary(), Bedrock.version(), t()) :: t()
  defp apply_clear_mutation(key, _target_version, %__MODULE__{} = index_update) do
    containing_page_id = Tree.page_for_key(index_update.index.tree, key)

    updated_pending_operations =
      Map.update(index_update.pending_operations, containing_page_id, %{key => :clear}, fn page_mutations ->
        Map.put(page_mutations, key, :clear)
      end)

    %{index_update | pending_operations: updated_pending_operations}
  end

  @spec apply_range_clear_mutation(binary(), binary(), Bedrock.version(), t()) :: t()
  defp apply_range_clear_mutation(start_key, end_key, _target_version, %__MODULE__{} = index_update) do
    case collect_range_pages_via_chain_following(index_update.index, start_key, end_key) do
      [] ->
        index_update

      [single_page_id] ->
        page = Index.get_page!(index_update.index, single_page_id)
        keys_to_clear = extract_keys_in_range(page, start_key, end_key)

        %{
          index_update
          | pending_operations:
              add_clear_operations_for_keys(index_update.pending_operations, single_page_id, keys_to_clear)
        }

      [first_page_id | remaining_page_ids] ->
        {middle_page_ids, [last_page_id]} = Enum.split(remaining_page_ids, -1)

        # NEVER delete page 0 - it should always be preserved even if it's in a range
        filtered_middle_page_ids = Enum.reject(middle_page_ids, &(&1 == 0))

        first_page = Index.get_page!(index_update.index, first_page_id)
        last_page = Index.get_page!(index_update.index, last_page_id)

        # For multi-page ranges, we need to be careful to only clear keys that actually fall within the range
        first_keys_to_clear =
          extract_keys_in_range(
            first_page,
            max(start_key, Page.left_key(first_page) || <<>>),
            min(end_key, Page.right_key(first_page) || @max_key)
          )

        last_keys_to_clear =
          extract_keys_in_range(
            last_page,
            max(start_key, Page.left_key(last_page) || <<>>),
            min(end_key, Page.right_key(last_page) || @max_key)
          )

        # Remove pending operations only for pages that will actually be deleted
        base_operations =
          index_update.pending_operations
          |> Map.drop(filtered_middle_page_ids)
          |> add_clear_operations_for_keys(first_page_id, first_keys_to_clear)
          |> add_clear_operations_for_keys(last_page_id, last_keys_to_clear)

        final_operations =
          if 0 in middle_page_ids do
            page_0 = Index.get_page!(index_update.index, 0)
            page_0_keys_to_clear = extract_keys_in_range(page_0, start_key, end_key)
            add_clear_operations_for_keys(base_operations, 0, page_0_keys_to_clear)
          else
            base_operations
          end

        %{
          index_update
          | index: Index.delete_pages(index_update.index, filtered_middle_page_ids),
            page_allocator: PageAllocator.recycle_page_ids(index_update.page_allocator, filtered_middle_page_ids),
            pending_operations: final_operations
        }
    end
  end

  @spec collect_range_pages_via_chain_following(Index.t(), binary(), binary()) :: [Page.id()]
  defp collect_range_pages_via_chain_following(index, start_key, end_key) do
    first_page_id = Tree.page_for_key(index.tree, start_key)
    follow_chain_collecting_range_pages(index.page_map, first_page_id, start_key, end_key, [])
  end

  defp follow_chain_collecting_range_pages(page_map, current_page_id, start_key, end_key, collected_page_ids) do
    case Map.get(page_map, current_page_id) do
      nil ->
        Enum.reverse(collected_page_ids)

      {current_page, next_id} ->
        process_page_in_range(page_map, current_page, next_id, start_key, end_key, collected_page_ids)
    end
  end

  defp process_page_in_range(page_map, current_page, next_id, start_key, end_key, collected_page_ids) do
    page_first_key = Page.left_key(current_page)
    page_last_key = Page.right_key(current_page)

    cond do
      page_entirely_before_range?(page_last_key, start_key) ->
        continue_to_next_page(page_map, next_id, start_key, end_key, collected_page_ids)

      page_entirely_after_range?(page_first_key, end_key) ->
        Enum.reverse(collected_page_ids)

      true ->
        include_page_and_continue(page_map, current_page, next_id, start_key, end_key, collected_page_ids)
    end
  end

  defp page_entirely_before_range?(page_last_key, start_key) do
    page_last_key != nil and page_last_key < start_key
  end

  defp page_entirely_after_range?(page_first_key, end_key) do
    page_first_key != nil and page_first_key > end_key
  end

  defp continue_to_next_page(page_map, next_id, start_key, end_key, collected_page_ids) do
    if next_id == 0 do
      Enum.reverse(collected_page_ids)
    else
      follow_chain_collecting_range_pages(page_map, next_id, start_key, end_key, collected_page_ids)
    end
  end

  defp include_page_and_continue(page_map, current_page, next_id, start_key, end_key, collected_page_ids) do
    current_page_id = Page.id(current_page)
    updated_collection = [current_page_id | collected_page_ids]

    if next_id == 0 do
      Enum.reverse(updated_collection)
    else
      follow_chain_collecting_range_pages(page_map, next_id, start_key, end_key, updated_collection)
    end
  end

  @spec apply_atomic_mutation(op :: atom(), binary(), binary(), Bedrock.version(), t()) :: t()
  defp apply_atomic_mutation(op, key, value, target_version, %__MODULE__{} = index_update) do
    current_value = get_current_value_for_atomic_op(index_update, key, target_version)

    new_value =
      case op do
        :add -> Atomics.add(current_value, value)
        :min -> Atomics.min(current_value, value)
        :max -> Atomics.max(current_value, value)
        _ -> raise ArgumentError, "Unsupported atomic operation: #{inspect(op)}"
      end

    apply_set_mutation(key, new_value, target_version, index_update)
  end

  @spec extract_keys_in_range(Page.t(), Bedrock.key(), Bedrock.key()) :: [Bedrock.key()]
  defp extract_keys_in_range(page, start_key, end_key) do
    page
    |> Page.key_locators()
    |> Enum.filter(fn {key, _version} -> key >= start_key and key <= end_key end)
    |> Enum.map(fn {key, _version} -> key end)
  end

  defp add_clear_operations_for_keys(pending_operations, _page_id, []), do: pending_operations

  defp add_clear_operations_for_keys(pending_operations, page_id, keys_to_clear) do
    Enum.reduce(keys_to_clear, pending_operations, fn key, operations_acc ->
      Map.update(operations_acc, page_id, %{key => :clear}, &Map.put(&1, key, :clear))
    end)
  end

  @spec apply_mutations_to_page(t(), Page.id(), %{Bedrock.key() => {:set, Bedrock.version()} | :clear}) :: t()
  defp apply_mutations_to_page(%__MODULE__{} = index_update, page_id, page_mutations) do
    page = Index.get_page!(index_update.index, page_id)
    updated_page = Page.apply_operations(page, page_mutations)
    key_count = Page.key_count(updated_page)

    cond do
      key_count == 0 ->
        handle_empty_page(index_update, page_id, page, updated_page)

      key_count > Index.max_keys_per_page() ->
        handle_oversized_page(index_update, page_id, updated_page, key_count)

      true ->
        handle_normal_page(index_update, page_id, page, updated_page)
    end
  end

  defp handle_empty_page(index_update, page_id, _page, updated_page) do
    if page_id == 0 do
      # Inline update_page logic - page 0 becoming empty doesn't change tree structure
      {_old_page, next_id} = Map.get(index_update.index.page_map, page_id)
      updated_page_map = Map.put(index_update.index.page_map, page_id, {updated_page, next_id})
      %{index_update | index: %{index_update.index | page_map: updated_page_map}}
    else
      %{
        index_update
        | index: Index.delete_pages(index_update.index, [page_id]),
          page_allocator: PageAllocator.recycle_page_id(index_update.page_allocator, page_id)
      }
    end
  end

  defp handle_oversized_page(index_update, page_id, updated_page, key_count) do
    additional_pages_needed = div(key_count - 1, Index.max_keys_per_page())

    {new_page_ids, allocator_after_allocation} =
      PageAllocator.allocate_ids(index_update.page_allocator, additional_pages_needed)

    {_original_page, original_next_id} = Map.get(index_update.index.page_map, page_id)

    index_after_split =
      Index.multi_split_page(index_update.index, page_id, original_next_id, updated_page, new_page_ids)

    all_modified_page_ids = [page_id | new_page_ids]

    %{
      index_update
      | index: index_after_split,
        page_allocator: allocator_after_allocation,
        modified_page_ids: MapSet.union(index_update.modified_page_ids, MapSet.new(all_modified_page_ids))
    }
  end

  defp handle_normal_page(index_update, page_id, page, updated_page) do
    # Inline update_page logic with boundary change optimization
    {_old_page, next_id} = Map.get(index_update.index.page_map, page_id)

    updated_tree =
      if page_boundaries_changed?(page, updated_page) do
        Tree.update_page_in_tree(index_update.index.tree, page, updated_page)
      else
        index_update.index.tree
      end

    updated_page_map = Map.put(index_update.index.page_map, page_id, {updated_page, next_id})
    updated_index = %{index_update.index | tree: updated_tree, page_map: updated_page_map}

    %{
      index_update
      | index: updated_index,
        modified_page_ids: MapSet.put(index_update.modified_page_ids, page_id)
    }
  end

  @spec get_current_value_for_atomic_op(t(), Bedrock.key(), Bedrock.version()) :: binary()
  defp get_current_value_for_atomic_op(index_update, key, _target_version) do
    case Index.locator_for_key(index_update.index, key) do
      {:ok, _page, locator} ->
        case Database.load_value(index_update.database, locator) do
          {:ok, value} ->
            value

          {:error, :not_found} ->
            <<>>
        end

      {:error, :not_found} ->
        <<>>
    end
  rescue
    _ -> <<>>
  end

  # Check if page boundaries (first_key/last_key) changed
  defp page_boundaries_changed?(old_page, new_page),
    do: Page.left_key(old_page) != Page.left_key(new_page) or Page.right_key(old_page) != Page.right_key(new_page)
end
