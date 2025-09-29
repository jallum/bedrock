defmodule Bedrock.DataPlane.Storage.Olivine.IndexManager do
  @moduledoc """
  Page management core for the Olivine storage driver.

  Implements Phase 2.1 of the Olivine implementation plan:
  - 5-second sliding time window for version retention
  - Version advancement and window expiry
  - Page eviction when versions exit window with efficient page collection
  - Version filtering for queries
  - Binary page encoding/decoding with 32-byte header format
  - Page creation and key lookup within pages
  - Simple median split algorithm (256 key threshold)
  - Page ID allocation with max_id tracking

  The output queue stores modified pages alongside version metadata to enable
  efficient collection during eviction without redundant filtering operations.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Storage.Olivine.Telemetry
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector

  @type page_id :: Page.id()
  @type page :: Page.t()

  @type loader_fn :: (Bedrock.key(), Bedrock.version() -> {:ok, Bedrock.value()} | {:error, :not_found})

  @type operation :: {:set, Bedrock.version()} | :clear

  @type modified_pages :: %{Page.id() => {Page.t(), Page.id()}}
  @type version_data :: {Index.t(), modified_pages()}
  @type version_update_data :: IndexUpdate.t()
  @type version_list :: [{Bedrock.version(), version_data()}]
  @opaque t :: %__MODULE__{
            versions: version_list(),
            current_version: Bedrock.version(),
            window_size_in_microseconds: pos_integer(),
            id_allocator: IdAllocator.t(),
            output_queue: :queue.queue(),
            last_version_ended_at_offset: non_neg_integer(),
            window_lag_time_μs: pos_integer(),
            n_keys: non_neg_integer()
          }
  defstruct [
    :versions,
    :current_version,
    :window_size_in_microseconds,
    :id_allocator,
    output_queue: :queue.new(),
    last_version_ended_at_offset: 0,
    window_lag_time_μs: 5_000_000,
    n_keys: 0
  ]

  @spec new() :: t()
  def new do
    %__MODULE__{
      versions: [{Version.zero(), {Index.new(), %{}}}],
      current_version: Version.zero(),
      window_size_in_microseconds: 5_000_000,
      id_allocator: IdAllocator.new(0, []),
      n_keys: 0
    }
  end

  @spec recover_from_database(database :: Database.t()) ::
          {:ok, t()} | {:error, :missing_pages}
  def recover_from_database({_data_db, _index_db} = database) do
    # Get the durable version from the database
    durable_version = Database.durable_version(database)

    # Load the index structure from the database
    case Index.load_from(database) do
      {:ok, initial_index, max_id, free_ids, n_keys} ->
        index_manager = %__MODULE__{
          versions: [{durable_version, {initial_index, %{}}}],
          current_version: durable_version,
          window_size_in_microseconds: 5_000_000,
          id_allocator: IdAllocator.new(max_id, free_ids),
          n_keys: n_keys
        }

        {:ok, index_manager}

      {:error, :missing_pages} ->
        {:error, :missing_pages}
    end
  end

  @spec page_for_key(t(), key :: Bedrock.key(), Bedrock.version()) ::
          {:ok, Page.t()}
          | {:error, :not_found}
          | {:error, :version_too_new}
  def page_for_key(index_manager, _key, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def page_for_key(index_manager, key, version) when is_binary(key) do
    index_manager.versions
    |> index_for_version(version)
    |> case do
      nil ->
        {:error, :version_too_old}

      index ->
        {:ok, Index.page_for_key(index, key)}
    end
  end

  @spec page_for_key(t(), KeySelector.t(), Bedrock.version()) ::
          {:ok, resolved_key :: binary(), Page.t()}
          | {:partial, keys_available :: non_neg_integer()}
          | {:error, :not_found | :version_too_new | :version_too_old}
  def page_for_key(index_manager, %KeySelector{} = _key_selector, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def page_for_key(index_manager, %KeySelector{} = key_selector, version) do
    case index_for_version(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        resolve_key_selector_in_index(index, key_selector)
    end
  end

  @spec pages_for_range(t(), start_key :: Bedrock.key(), end_key :: Bedrock.key(), Bedrock.version()) ::
          {:ok, [Page.t()]}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
  def pages_for_range(index_manager, _start_key, _end_key, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def pages_for_range(index_manager, start_key, end_key, version) when is_binary(start_key) and is_binary(end_key) do
    case index_for_version(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        Index.pages_for_range(index, start_key, end_key)
    end
  end

  @spec pages_for_range(t(), KeySelector.t(), KeySelector.t(), Bedrock.version()) ::
          {:ok, {resolved_start :: binary(), resolved_end :: binary()}, [Page.t()]}
          | {:error, :version_too_new | :version_too_old | :invalid_range}
  def pages_for_range(index_manager, %KeySelector{} = _start_selector, %KeySelector{} = _end_selector, version)
      when index_manager.current_version < version,
      do: {:error, :version_too_new}

  def pages_for_range(index_manager, %KeySelector{} = start_selector, %KeySelector{} = end_selector, version) do
    case index_for_version(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        resolve_range_selectors_in_index(index, start_selector, end_selector)
    end
  end

  @spec apply_transactions(index_manager :: t(), encoded_transactions :: [binary()], database :: Database.t()) ::
          {t(), Database.t()}
  def apply_transactions(index_manager, [], database), do: {index_manager, database}

  def apply_transactions(index_manager, transactions, database) when is_list(transactions) do
    Enum.reduce(transactions, {index_manager, database}, fn transaction, {index_manager, database} ->
      apply_transaction(index_manager, transaction, database)
    end)
  end

  # Transaction Processing Functions (Phase 3.1)

  @doc """
  Applies a single transaction binary to the version manager.
  Creates a new version and applies all mutations in the transaction.
  Uses a two-pass approach: first collect all instructions, then process each page.
  """
  @spec apply_transaction(t(), binary(), Database.t()) :: {t(), Database.t()}
  def apply_transaction(
        %{versions: [{_version, {current_index, _prev_modified}} | _]} = index_manager,
        transaction,
        database
      ) do
    commit_version = Transaction.commit_version!(transaction)

    update =
      current_index
      |> IndexUpdate.new(commit_version, index_manager.id_allocator, database)
      |> IndexUpdate.apply_mutations(Transaction.mutations!(transaction))
      |> IndexUpdate.process_pending_operations()

    {new_index, new_database, new_id_allocator, modified_pages} = IndexUpdate.finish(update)

    %{keys_added: keys_added, keys_removed: keys_removed, keys_changed: keys_changed} = update
    new_n_keys = index_manager.n_keys + keys_added - keys_removed

    {updated_data_db, _} = new_database
    this_version_ended_at_offset = updated_data_db.file_offset
    size_in_bytes = this_version_ended_at_offset - index_manager.last_version_ended_at_offset

    # Store modified pages directly in output queue for efficient collection during eviction.
    # This eliminates the need to filter versions later in the persistence flow.
    new_queue =
      :queue.in(
        {commit_version, this_version_ended_at_offset, size_in_bytes, modified_pages},
        index_manager.output_queue
      )

    Telemetry.trace_index_update_complete(keys_added, keys_removed, keys_changed, new_n_keys)

    {%{
       index_manager
       | versions: [{commit_version, {new_index, modified_pages}} | index_manager.versions],
         current_version: commit_version,
         id_allocator: new_id_allocator,
         output_queue: new_queue,
         last_version_ended_at_offset: this_version_ended_at_offset,
         n_keys: new_n_keys
     }, new_database}
  end

  # Helper Functions

  @spec info(index_manager :: t(), atom()) :: term()
  def info(index_manager, stat) do
    case stat do
      :n_keys ->
        index_manager.n_keys

      # Size tracking will be implemented in a future phase.
      # This will require summing the byte size of all pages and
      # values across versions, including lookaside buffer data.
      :size_in_bytes ->
        0

      # Utilization tracking will be implemented in a future phase.
      # This will provide metrics on storage efficiency, including
      # page fill ratios and memory usage patterns.
      :utilization ->
        0.0

      :key_ranges ->
        get_key_ranges(index_manager)

      :max_id ->
        index_manager.id_allocator.max_id

      :free_ids ->
        index_manager.id_allocator.free_ids

      _ ->
        :undefined
    end
  end

  @spec get_key_ranges(t()) :: [{Bedrock.key(), Bedrock.key()}]
  defp get_key_ranges(%{versions: [{_, {current_index, _}} | _]}), do: [{current_index.min_key, current_index.max_key}]
  defp get_key_ranges(%{versions: []}), do: []

  @spec index_for_version(version_list(), Bedrock.version()) :: Index.t() | nil
  defp index_for_version(versions, target), do: find_target(versions, target)

  defp find_target([{version, _version_data} | rest], target) when target < version, do: find_target(rest, target)
  defp find_target([], _target), do: nil
  defp find_target([{_version, {index, _modified_pages}} | _rest], _target), do: index

  # KeySelector Resolution Helper Functions

  @spec resolve_key_selector_in_index(Index.t(), KeySelector.t()) ::
          {:ok, resolved_key :: binary(), Page.t()}
          | {:partial, keys_available :: non_neg_integer()}
          | {:error, :not_found}
  defp resolve_key_selector_in_index(
         index,
         %KeySelector{key: ref_key, or_equal: or_equal, offset: offset} = key_selector
       ) do
    page = Index.page_for_key(index, ref_key)

    case resolve_key_selector_in_page(page, ref_key, or_equal, offset) do
      {:ok, resolved_key, page} ->
        {:ok, resolved_key, page}

      {:partial, keys_available} ->
        handle_cross_page_continuation(index, page, key_selector, keys_available)
    end
  end

  defp handle_cross_page_continuation(index, page, key_selector, keys_available) do
    # If no keys were available and we have a negative offset, we've gone beyond the start of keyspace
    if keys_available == 0 and key_selector.offset < 0 do
      {:error, :not_found}
    else
      case calculate_cross_page_continuation(index, page, key_selector, keys_available) do
        {:ok, continuation_selector} ->
          resolve_key_selector_in_index(index, continuation_selector)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec resolve_range_selectors_in_index(Index.t(), KeySelector.t(), KeySelector.t()) ::
          {:ok, {resolved_start :: binary(), resolved_end :: binary()}, [Page.t()]}
          | {:error, :invalid_range | :not_found}
  defp resolve_range_selectors_in_index(index, start_selector, end_selector) do
    with {:ok, resolved_start, _start_page} <- resolve_key_selector_in_index(index, start_selector),
         {:ok, resolved_end, _end_page} <- resolve_key_selector_in_index(index, end_selector) do
      if resolved_start <= resolved_end do
        {:ok, pages} = Index.pages_for_range(index, resolved_start, resolved_end)
        {:ok, {resolved_start, resolved_end}, pages}
      else
        {:error, :invalid_range}
      end
    end
  end

  @spec resolve_key_selector_in_page(Page.t(), binary(), boolean(), integer()) ::
          {:ok, resolved_key :: binary(), Page.t()}
          | {:partial, keys_available :: non_neg_integer()}
          | {:error, :not_found}
  defp resolve_key_selector_in_page(page, ref_key, or_equal, offset) do
    # Extract header info once for efficient binary operations
    <<_id::unsigned-big-32, key_count::unsigned-big-16, _offset::unsigned-big-32, _reserved::unsigned-big-48,
      entries::binary>> = page

    # Use optimized binary search that stops early when key > ref_key
    case Page.search_entries_with_position(entries, key_count, ref_key) do
      {:found, pos} ->
        target_pos = calculate_target_position_found(pos, or_equal, offset)
        resolve_at_position_optimized(entries, target_pos, key_count, page)

      {:not_found, insertion_pos} ->
        target_pos = calculate_target_position_not_found(insertion_pos, or_equal, offset)
        resolve_at_position_optimized(entries, target_pos, key_count, page)
    end
  end

  # Helper functions for calculating target positions
  defp calculate_target_position_found(pos, or_equal, offset) do
    if or_equal do
      pos + offset
    else
      # For greater_than, we need the next position, then apply offset
      pos + 1 + offset
    end
  end

  defp calculate_target_position_not_found(insertion_pos, or_equal, offset) do
    if offset >= 0 do
      # Forward selector: insertion_pos is correct starting point
      insertion_pos + offset
    else
      # Backward selector: want position before insertion point
      if or_equal do
        # last_less_or_equal: position before insertion point
        insertion_pos - 1 + offset
      else
        # last_less_than: same as less_or_equal when key not found
        insertion_pos - 1 + offset
      end
    end
  end

  defp resolve_at_position_optimized(entries, pos, key_count, page) when pos >= 0 and pos < key_count do
    case Page.decode_entry_at_position(entries, pos, key_count) do
      {:ok, {key, _version}} -> {:ok, key, page}
      :out_of_bounds -> {:partial, key_count}
    end
  end

  defp resolve_at_position_optimized(_entries, pos, _key_count, _page) when pos < 0, do: {:partial, 0}
  defp resolve_at_position_optimized(_entries, _pos, key_count, _page), do: {:partial, key_count}

  # Cross-page continuation helper functions

  @spec calculate_cross_page_continuation(Index.t(), Page.t(), KeySelector.t(), non_neg_integer()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp calculate_cross_page_continuation(index, current_page, key_selector, keys_available) do
    if key_selector.offset >= 0 do
      calculate_forward_page_continuation(index, current_page, key_selector, keys_available)
    else
      calculate_backward_page_continuation(index, current_page, key_selector, keys_available)
    end
  end

  @spec calculate_forward_page_continuation(Index.t(), Page.t(), KeySelector.t(), non_neg_integer()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp calculate_forward_page_continuation(index, current_page, key_selector, keys_available) do
    # Get the cached next_id from the page_map instead of parsing the page binary
    {_page, next_id} = Index.get_page_with_next_id!(index, Page.id(current_page))

    case next_id do
      0 ->
        # No next page - we've hit the end of the index
        {:error, :not_found}

      next_page_id ->
        next_page = Index.get_page!(index, next_page_id)

        # Calculate remaining offset after consuming available keys in current page
        remaining_offset = key_selector.offset - keys_available

        case Page.left_key(next_page) do
          nil ->
            # Empty next page, continue with empty key to trigger gap resolution
            continuation_selector = %KeySelector{
              key: "",
              or_equal: true,
              offset: remaining_offset
            }

            {:ok, continuation_selector}

          first_key_of_next_page ->
            # Create continuation KeySelector at the start of next page
            continuation_selector = %KeySelector{
              key: first_key_of_next_page,
              or_equal: true,
              offset: remaining_offset
            }

            {:ok, continuation_selector}
        end
    end
  end

  @spec calculate_backward_page_continuation(Index.t(), Page.t(), KeySelector.t(), non_neg_integer()) ::
          {:ok, KeySelector.t()} | {:error, :not_found}
  defp calculate_backward_page_continuation(index, current_page, key_selector, keys_available) do
    # Find the page whose next_id points to current page (previous page)
    case find_previous_page(index, Page.id(current_page)) do
      {:ok, previous_page} ->
        case Page.right_key(previous_page) do
          nil ->
            # Empty previous page, this shouldn't happen in a well-formed index
            {:error, :not_found}

          last_key_of_prev_page ->
            # Calculate remaining offset after consuming available keys in current page
            # For backward traversal, we add the consumed keys to make the offset more negative
            remaining_offset = key_selector.offset + keys_available

            # Create continuation KeySelector at the end of previous page
            continuation_selector = %KeySelector{
              key: last_key_of_prev_page,
              or_equal: true,
              offset: remaining_offset
            }

            {:ok, continuation_selector}
        end

      {:error, :not_found} ->
        # No previous page - we've hit the beginning of the index
        {:error, :not_found}
    end
  end

  @spec find_previous_page(Index.t(), Page.id()) :: {:ok, Page.t()} | {:error, :not_found}
  defp find_previous_page(%Index{page_map: page_map}, target_page_id) do
    # Search through all pages to find the one whose next_id points to our target
    page_map
    |> Enum.find_value(fn {_page_id, {page, next_id}} ->
      if next_id == target_page_id, do: page
    end)
    |> case do
      nil -> {:error, :not_found}
      page -> {:ok, page}
    end
  end

  # Buffer tracking queue functions

  @doc """
  Advances the window by determining what to evict and updating both buffer tracking and hot set.
  This is the complete window advancement operation that combines:
  1. Calculating window edge (newest version in buffer - 5 seconds)
  2. Determining eviction batch based on size and time constraints, collecting modified pages
  3. Trimming hot set to match eviction point

  Returns either {:no_eviction, updated_manager} or {:evict, evicted_count, updated_manager, collected_pages, eviction_version}.
  The collected_pages contain all modified pages from evicted versions for efficient persistence.
  """
  @spec advance_window(t(), pos_integer()) ::
          {:no_eviction, t()}
          | {:evict, non_neg_integer(), t(), [any()], Bedrock.version()}
  def advance_window(index_manager, max_eviction_size_bytes) do
    with {:ok, window_edge} <- get_window_edge(index_manager),
         {:ok, evicted_count, new_output_queue, collected_pages, eviction_version} <-
           determine_eviction_batch(index_manager, max_eviction_size_bytes, window_edge) do
      new_versions = split_versions(index_manager.versions, eviction_version, [])
      new_index_manager = %{index_manager | output_queue: new_output_queue, versions: new_versions}
      {:evict, evicted_count, new_index_manager, collected_pages, eviction_version}
    else
      :no_eviction -> {:no_eviction, index_manager}
    end
  end

  # This version is still in window, keep it and continue
  defp split_versions([{version, _data} = entry | rest], target, kept_versions) when version >= target,
    do: split_versions(rest, target, [entry | kept_versions])

  defp split_versions([], _target, kept_versions), do: Enum.reverse(kept_versions)

  # This version is outside window, split here
  # All remaining versions (including this one) should be evicted
  defp split_versions(_all_versions, _target, kept_versions), do: Enum.reverse(kept_versions)

  # Gets the window edge by using the current_version as the reference point.
  # This provides a stable reference that advances with transaction processing
  # rather than causing massive evictions during transaction bursts.
  defp get_window_edge(index_manager) do
    case :queue.peek_r(index_manager.output_queue) do
      {:value, _} ->
        # Use current_version (the latest committed version) as the reference point
        # This prevents the bug where using the newest buffered version causes
        # the entire buffer to be evicted during rapid transaction processing
        try do
          {:ok, Version.subtract(index_manager.current_version, index_manager.window_lag_time_μs)}
        rescue
          # Underflow - return no window edge (zero version)
          ArgumentError -> :no_eviction
        end

      :empty ->
        :no_eviction
    end
  end

  # Determines which versions to evict based on size limits and window edge.
  # Returns count, collected pages, eviction version, and updated queue for efficiency.
  defp determine_eviction_batch(index_manager, max_size_bytes, window_edge_version) do
    index_manager.output_queue
    |> pull_from_output_queue(max_size_bytes, window_edge_version)
    |> case do
      {0, _, _, _} ->
        :no_eviction

      {count, collected_pages, eviction_version, new_queue} ->
        {:ok, count, new_queue, collected_pages, eviction_version}
    end
  end

  # Pull versions from oldest end of output queue until size limit or window edge.
  # Collects modified pages and counts evicted versions for efficient processing.
  defp pull_from_output_queue(
         queue,
         max_size,
         window_edge,
         count \\ 0,
         current_size \\ 0,
         pages_acc \\ [],
         last_version \\ nil
       ) do
    case :queue.peek(queue) do
      {:value, {version, _, size, modified_pages}}
      when version <= window_edge and current_size + size < max_size ->
        {_, new_queue} = :queue.out(queue)

        pull_from_output_queue(
          new_queue,
          max_size,
          window_edge,
          count + 1,
          current_size + size,
          [modified_pages | pages_acc],
          version
        )

      # Stop if this version is newer than the window edge (should not be evicted)
      _ ->
        {count, Enum.reverse(pages_acc), last_version, queue}
    end
  end
end
