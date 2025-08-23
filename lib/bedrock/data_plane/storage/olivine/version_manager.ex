defmodule Bedrock.DataPlane.Storage.Olivine.VersionManager do
  @moduledoc """
  Page management core for the Olivine storage driver.

  Implements Phase 2.1 of the Olivine implementation plan:
  - 5-second sliding time window for version retention
  - Version advancement and window expiry
  - Page eviction when versions exit window
  - Version filtering for queries
  - Binary page encoding/decoding with 32-byte header format
  - Page creation and key lookup within pages
  - Simple median split algorithm (256 key threshold)
  - Page ID allocation with max_page_id tracking

  ## Binary Page Format

  Pages are encoded as binary data with the following structure:

  ```
  32-byte header (all big-endian):
  <<PageId:64/big,           # 8 bytes
    NextPageId:64/big,       # 8 bytes
    KeyCount:16/big,         # 2 bytes (supports up to 65535 keys)
    LastKeyOffset:32/big,    # 4 bytes - byte offset to start of last key
    Reserved:80/big,         # 10 bytes
    % Interleaved entries (repeated KeyCount times):
    Version:64/big,          # 8 bytes
    KeyLength:16/big,        # 2 bytes
    Key/binary>>             # KeyLength bytes
  ```

  Keys and versions are stored as interleaved pairs for better cache locality.
  LastKeyOffset points to the start of the last key (after its version and length prefix),
  allowing O(1) access to the last key by reading from that offset to end of binary.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Page
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Tree
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @type page_id :: Page.id()
  @type page :: Page.t()

  @type loader_fn :: (Bedrock.key(), Bedrock.version() -> {:ok, Bedrock.value()} | {:error, :not_found})

  @type operation :: {:set, Bedrock.version()} | :clear

  defmodule VersionData do
    @moduledoc false
    alias Bedrock.DataPlane.Storage.Olivine.VersionManager

    @type operation :: VersionManager.operation()

    @type t :: %__MODULE__{
            tree: :gb_trees.tree(),
            page_map: map(),
            deleted_page_ids: [Page.id()],
            modified_page_ids: [Page.id()],
            pending_operations: %{Page.id() => %{Bedrock.key() => operation()}}
          }

    defstruct [
      :tree,
      :page_map,
      :deleted_page_ids,
      :modified_page_ids,
      :pending_operations
    ]
  end

  @type version_data :: VersionData.t()

  @opaque t :: %__MODULE__{
            versions: [{Bedrock.version(), version_data()}],
            current_version: Bedrock.version(),
            durable_version: Bedrock.version(),
            window_size_in_microseconds: pos_integer(),
            lookaside_buffer: :ets.tab(),
            max_page_id: page_id(),
            free_page_ids: [page_id()]
          }
  defstruct [
    :versions,
    :current_version,
    :durable_version,
    :window_size_in_microseconds,
    :lookaside_buffer,
    :max_page_id,
    :free_page_ids
  ]

  @spec new() :: t()
  def new do
    lookaside_buffer = :ets.new(:olivine_lookaside, [:ordered_set, :protected, {:read_concurrency, true}])

    initial_page_binary = Page.new(0, [])
    initial_tree = :gb_trees.empty()
    initial_page_map = %{0 => initial_page_binary}

    initial_version_data = %VersionData{
      tree: initial_tree,
      page_map: initial_page_map,
      deleted_page_ids: [],
      modified_page_ids: [],
      pending_operations: %{}
    }

    %__MODULE__{
      versions: [{Version.zero(), initial_version_data}],
      current_version: Version.zero(),
      durable_version: Version.zero(),
      window_size_in_microseconds: 5_000_000,
      lookaside_buffer: lookaside_buffer,
      max_page_id: 0,
      free_page_ids: []
    }
  end

  @spec recover_from_database(database :: Database.t()) ::
          {:ok, t()} | {:error, :corrupted_page | :broken_chain | :cycle_detected}
  def recover_from_database(database) do
    lookaside_buffer = :ets.new(:olivine_lookaside, [:ordered_set, :protected, {:read_concurrency, true}])

    # Load the durable version from the database, or use zero version if none exists
    durable_version =
      case Database.load_durable_version(database) do
        {:ok, version} -> version
        {:error, :not_found} -> Version.zero()
      end

    # Traverse pages from page 0 to rebuild page structure and calculate metadata
    case load_and_rebuild_pages(database) do
      {:ok, tree, max_page_id, free_page_ids} ->
        # Create initial page_map - if empty database, include page 0 like new() does
        initial_page_map =
          if :gb_trees.is_empty(tree) and max_page_id == 0 do
            %{0 => Page.new(0, [])}
          else
            %{}
          end

        # Properly reconstruct the last durable version as both current_version and top of versions stack
        initial_version_data = %VersionData{
          tree: tree,
          page_map: initial_page_map,
          deleted_page_ids: [],
          modified_page_ids: [],
          pending_operations: %{}
        }

        version_manager = %__MODULE__{
          versions: [{durable_version, initial_version_data}],
          current_version: durable_version,
          durable_version: durable_version,
          window_size_in_microseconds: 5_000_000,
          lookaside_buffer: lookaside_buffer,
          max_page_id: max_page_id,
          free_page_ids: free_page_ids
        }

        {:ok, version_manager}

      {:error, reason} when reason in [:corrupted_page, :broken_chain, :cycle_detected] ->
        # Clean up the ETS table before returning error
        :ets.delete(lookaside_buffer)
        {:error, reason}
    end
  end

  defp load_and_rebuild_pages(database) do
    case load_page_chain(database, 0, %{}) do
      {:ok, page_map} ->
        tree = Tree.from_page_map(page_map)
        page_ids = page_map |> Map.keys() |> MapSet.new()
        max_page_id = max(0, Enum.max(page_ids))
        free_page_ids = calculate_free_page_ids(max_page_id, page_ids)

        {:ok, tree, max_page_id, free_page_ids}

      {:error, :no_chain} ->
        {:ok, :gb_trees.empty(), 0, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_page_chain(_database, page_id, page_map) when is_map_key(page_map, page_id), do: {:error, :cycle_detected}

  defp load_page_chain(database, page_id, page_map) do
    case Database.load_page(database, page_id) do
      {:ok, page_binary} ->
        # Basic validation of binary page format
        case page_binary do
          <<_id::32, _next_id::32, _key_count::16, _last_key_offset::32, _reserved::16, _entries::binary>> ->
            page_map
            |> Map.put(page_id, page_binary)
            |> continue_page_chain(database, page_binary)

          _ ->
            {:error, :corrupted_page}
        end

      {:error, :not_found} when page_id == 0 ->
        {:error, :no_chain}

      {:error, :not_found} ->
        {:error, :broken_chain}
    end
  end

  defp continue_page_chain(page_map, database, page_binary) do
    case Page.next_id(page_binary) do
      0 -> {:ok, page_map}
      next_id -> load_page_chain(database, next_id, page_map)
    end
  end

  defp calculate_free_page_ids(0, _all_existing_page_ids), do: []

  defp calculate_free_page_ids(max_page_id, all_existing_page_ids) do
    0..max_page_id
    |> MapSet.new()
    |> MapSet.difference(all_existing_page_ids)
    |> Enum.sort()
  end

  defp add_unique_page_id(id_list, id, acc \\ [])
  defp add_unique_page_id([], id, acc), do: Enum.reverse(acc, [id])
  defp add_unique_page_id([h | t], id, acc) when id < h, do: Enum.reverse(acc, [id, h | t])
  defp add_unique_page_id([h | _t] = list, id, acc) when id == h, do: Enum.reverse(acc, list)
  defp add_unique_page_id([h | t], id, acc), do: add_unique_page_id(t, id, [h | acc])

  @spec close(version_manager :: t()) :: :ok
  def close(version_manager) do
    :ets.delete(version_manager.lookaside_buffer)
    :ok
  end

  @spec fetch_value(t(), Bedrock.key(), Bedrock.version()) ::
          {:ok, Bedrock.value()} | {:error, :not_found | :check_database}
  def fetch_value(version_manager, key, version) do
    if version > version_manager.durable_version do
      # Look up in ETS lookaside buffer for recent versions
      case :ets.lookup(version_manager.lookaside_buffer, {version, key}) do
        [{_key_version, value}] -> {:ok, value}
        [] -> {:error, :not_found}
      end
    else
      # Signal that this should be loaded from database instead
      {:error, :check_database}
    end
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire VersionManager.
  """
  @spec value_loader(t()) :: (Bedrock.key(), Bedrock.version() ->
                                {:ok, Bedrock.value()} | {:error, :not_found | :check_database})
  def value_loader(version_manager) do
    # Capture only the ETS reference and durable version
    lookaside_buffer = version_manager.lookaside_buffer
    durable_version = version_manager.durable_version

    fn
      key, version when version > durable_version ->
        case :ets.lookup(lookaside_buffer, {version, key}) do
          [{_key_version, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end

      _, _ ->
        {:error, :check_database}
    end
  end

  @spec fetch_page_for_key(version_manager :: t(), key :: Bedrock.key(), version :: Bedrock.version()) ::
          {:ok, Page.t()} | {:error, :not_found | :version_too_old | :version_too_new}
  def fetch_page_for_key(version_manager, _key, version) when version < version_manager.durable_version,
    do: {:error, :version_too_old}

  def fetch_page_for_key(version_manager, _key, version) when version_manager.current_version < version,
    do: {:error, :version_too_new}

  def fetch_page_for_key(version_manager, key, version) do
    with %VersionData{tree: tree, page_map: page_map} <- find_best_version_for_fetch(version_manager.versions, version),
         page_id when is_integer(page_id) <- Tree.page_for_key(tree, key) do
      {:ok, Map.fetch!(page_map, page_id)}
    else
      nil -> {:error, :not_found}
    end
  end

  @spec fetch_pages_for_range(
          version_manager :: t(),
          start_key :: Bedrock.key(),
          end_key :: Bedrock.key(),
          version :: Bedrock.version()
        ) ::
          {:ok, [Page.t()]} | {:error, :version_too_old | :version_too_new}
  def fetch_pages_for_range(version_manager, _start_key, _end_key, version)
      when version < version_manager.durable_version,
      do: {:error, :version_too_old}

  def fetch_pages_for_range(version_manager, _start_key, _end_key, version)
      when version_manager.current_version < version,
      do: {:error, :version_too_new}

  def fetch_pages_for_range(version_manager, start_key, end_key, version) do
    case find_best_version_for_fetch(version_manager.versions, version) do
      nil ->
        {:ok, []}

      %VersionData{tree: tree, page_map: page_map} ->
        pages =
          tree
          |> Tree.page_ids_in_range(start_key, end_key)
          |> Enum.map(&Map.fetch!(page_map, &1))

        {:ok, pages}
    end
  end

  @spec apply_transactions(version_manager :: t(), encoded_transactions :: [binary()]) :: t()
  def apply_transactions(version_manager, []), do: version_manager

  def apply_transactions(version_manager, transactions) when is_list(transactions),
    do: Enum.reduce(transactions, version_manager, &apply_single_transaction/2)

  # Transaction Processing Functions (Phase 3.1)

  @doc """
  Applies a single transaction binary to the version manager.
  Creates a new version and applies all mutations in the transaction.
  """
  @spec apply_single_transaction(binary(), t()) :: t()
  def apply_single_transaction(transaction_binary, version_manager) do
    commit_version = Transaction.extract_commit_version!(transaction_binary)

    transaction_binary
    |> Transaction.stream_mutations!()
    |> apply_mutations_to_version(commit_version, version_manager)
  end

  @doc """
  Applies mutations to create a new version state in the version manager.
  Uses a two-pass approach: first collect all instructions, then process each page.
  """
  @spec apply_mutations_to_version(Enumerable.t(), Bedrock.version(), t()) :: t()
  def apply_mutations_to_version(mutations, new_version, version_manager) do
    current_page_data = get_current_page_data(version_manager)

    # Pass 1: Distribute instructions across pages
    page_data_with_instructions =
      Enum.reduce(mutations, current_page_data, fn mutation, page_data_acc ->
        apply_single_mutation(version_manager, mutation, new_version, page_data_acc)
      end)

    # Pass 2: Process all pending operations for each modified page
    final_page_data = process_all_pending_operations(version_manager, page_data_with_instructions, new_version)

    updated_versions = [{new_version, final_page_data} | version_manager.versions]

    %{version_manager | versions: updated_versions, current_version: new_version}
  end

  @spec apply_single_mutation(t(), Tx.mutation(), Bedrock.version(), version_data()) :: version_data()
  def apply_single_mutation(version_manager, mutation, new_version, page_data) do
    case mutation do
      {:set, key, value} ->
        apply_set_mutation(version_manager, key, value, new_version, page_data)

      {:clear, key} ->
        apply_clear_mutation(version_manager, key, new_version, page_data)

      {:clear_range, start_key, end_key} ->
        apply_range_clear_mutation(version_manager, start_key, end_key, new_version, page_data)
    end
  end

  @spec apply_set_mutation(t(), binary(), binary(), Bedrock.version(), version_data()) :: version_data()
  defp apply_set_mutation(version_manager, key, value, new_version, %VersionData{} = page_data) do
    target_page_id = Tree.page_for_insertion(page_data.tree, key)

    # Store the value in lookaside buffer immediately
    true = :ets.insert_new(version_manager.lookaside_buffer, {{new_version, key}, value})

    # Add set operation to pending operations for this page (last-writer-wins)
    set_operation = {:set, new_version}

    updated_operations =
      Map.update(page_data.pending_operations, target_page_id, %{key => set_operation}, fn page_ops ->
        Map.put(page_ops, key, set_operation)
      end)

    %{page_data | pending_operations: updated_operations}
  end

  @spec apply_clear_mutation(t(), binary(), Bedrock.version(), version_data()) :: version_data()
  defp apply_clear_mutation(_version_manager, key, _new_version, %VersionData{} = page_data) do
    case Tree.page_for_key(page_data.tree, key) do
      nil ->
        # Key doesn't exist, no operation needed
        page_data

      page_id ->
        # Add clear operation to pending operations for this page (last-writer-wins)
        clear_operation = :clear

        updated_operations =
          Map.update(page_data.pending_operations, page_id, %{key => clear_operation}, fn page_ops ->
            Map.put(page_ops, key, clear_operation)
          end)

        %{page_data | pending_operations: updated_operations}
    end
  end

  @spec apply_range_clear_mutation(t(), binary(), binary(), Bedrock.version(), version_data()) :: version_data()
  defp apply_range_clear_mutation(_version_manager, start_key, end_key, _new_version, %VersionData{} = page_data) do
    overlapping_page_ids = Tree.page_ids_in_range(page_data.tree, start_key, end_key)

    case overlapping_page_ids do
      [] ->
        # No pages in range, nothing to do
        page_data

      [single_page_id] ->
        # Only one page overlaps, expand range to individual clear operations
        page = Map.fetch!(page_data.page_map, single_page_id)
        keys_to_clear = get_keys_in_range(page, start_key, end_key)

        # Add individual clear operations for each key in range
        updated_operations = add_clear_operations(page_data.pending_operations, single_page_id, keys_to_clear)

        %{page_data | pending_operations: updated_operations}

      multiple_page_ids ->
        # Multiple pages: first and last get individual clears for keys in range, middle pages get deleted
        first_page_id = List.first(multiple_page_ids)
        last_page_id = List.last(multiple_page_ids)
        middle_page_ids = multiple_page_ids |> Enum.drop(1) |> Enum.drop(-1)

        # Handle first page - get keys from start_key to end of page
        first_page = Map.fetch!(page_data.page_map, first_page_id)
        first_page_end_key = Page.right_key(first_page)
        first_clear_end = min(end_key, first_page_end_key || end_key)
        first_keys_to_clear = get_keys_in_range(first_page, start_key, first_clear_end)

        # Handle last page - get keys from start of page to end_key
        last_page = Map.fetch!(page_data.page_map, last_page_id)
        last_page_start_key = Page.left_key(last_page)
        last_clear_start = max(start_key, last_page_start_key || start_key)
        last_keys_to_clear = get_keys_in_range(last_page, last_clear_start, end_key)

        # Remove middle pages immediately and clean up their pending operations
        updated_tree =
          Enum.reduce(middle_page_ids, page_data.tree, fn page_id, tree_acc ->
            page = Map.fetch!(page_data.page_map, page_id)
            Tree.remove_page_from_tree(tree_acc, page)
          end)

        updated_page_map =
          Enum.reduce(middle_page_ids, page_data.page_map, fn page_id, map_acc ->
            Map.delete(map_acc, page_id)
          end)

        # Since both lists are sorted, we can merge efficiently
        updated_deleted_page_ids = merge_sorted_unique(page_data.deleted_page_ids, middle_page_ids)

        updated_operations =
          page_data.pending_operations
          |> Map.drop(middle_page_ids)
          |> add_clear_operations(first_page_id, first_keys_to_clear)
          |> add_clear_operations(last_page_id, last_keys_to_clear)

        %{
          page_data
          | tree: updated_tree,
            page_map: updated_page_map,
            deleted_page_ids: updated_deleted_page_ids,
            pending_operations: updated_operations
        }
    end
  end

  # Helper function to get keys within a range from a page
  @spec get_keys_in_range(Page.t(), Bedrock.key(), Bedrock.key()) :: [Bedrock.key()]
  defp get_keys_in_range(page_binary, start_key, end_key) do
    page_binary
    |> Page.key_versions()
    |> Enum.filter(fn {key, _version} -> key >= start_key and key <= end_key end)
    |> Enum.map(fn {key, _version} -> key end)
  end

  @spec process_all_pending_operations(t(), version_data(), Bedrock.version()) :: version_data()
  defp process_all_pending_operations(version_manager, %VersionData{} = page_data, new_version) do
    # Process each page that has pending operations using sorted merge
    page_data.pending_operations
    |> Enum.reduce(page_data, fn {page_id, operations}, page_data_acc ->
      process_page_operations(version_manager, page_data_acc, page_id, operations, new_version)
    end)
    |> clear_all_pending_operations()
  end

  @spec process_page_operations(t(), version_data(), page_id(), %{Bedrock.key() => operation()}, Bedrock.version()) ::
          version_data()
  defp process_page_operations(version_manager, %VersionData{} = page_data, page_id, operations, _new_version) do
    page_binary = Map.fetch!(page_data.page_map, page_id)

    # Apply operations and encode directly to binary (no intermediate struct)
    updated_page_binary = Page.apply_operations(page_binary, operations)

    # Handle page updates, splitting, or deletion
    cond do
      Page.empty?(updated_page_binary) ->
        # Page is now empty, remove it
        %{
          page_data
          | tree: Tree.remove_page_from_tree(page_data.tree, page_binary),
            page_map: Map.delete(page_data.page_map, page_id),
            deleted_page_ids: add_unique_page_id(page_data.deleted_page_ids, page_id)
        }

      Page.key_count(updated_page_binary) > 256 ->
        # Page needs splitting
        handle_page_split(version_manager, page_data, page_id, page_binary, updated_page_binary)

      true ->
        # Normal page update
        %{
          page_data
          | tree: Tree.update_page_in_tree(page_data.tree, page_binary, updated_page_binary),
            page_map: Map.put(page_data.page_map, page_id, updated_page_binary),
            modified_page_ids: add_unique_page_id(page_data.modified_page_ids, page_id)
        }
    end
  end

  @spec handle_page_split(t(), version_data(), page_id(), binary(), binary()) :: version_data()
  defp handle_page_split(
         version_manager,
         %VersionData{} = page_data,
         page_id,
         original_page_binary,
         updated_page_binary
       ) do
    # Split the page directly using binary format
    {{left_page_binary, right_page_binary}, _updated_vm} = split_page_binary(updated_page_binary, version_manager)

    updated_tree =
      page_data.tree
      |> Tree.remove_page_from_tree(original_page_binary)
      |> Tree.add_page_to_tree(left_page_binary)
      |> Tree.add_page_to_tree(right_page_binary)

    updated_page_map =
      page_data.page_map
      |> Map.put(Page.id(left_page_binary), left_page_binary)
      |> Map.put(Page.id(right_page_binary), right_page_binary)
      |> then(fn map ->
        if not Page.has_id?(left_page_binary, page_id) and not Page.has_id?(right_page_binary, page_id) do
          Map.delete(map, page_id)
        else
          map
        end
      end)

    split_deleted_pages =
      if not Page.has_id?(left_page_binary, page_id) and not Page.has_id?(right_page_binary, page_id) do
        add_unique_page_id(page_data.deleted_page_ids, page_id)
      else
        page_data.deleted_page_ids
      end

    updated_modified_page_ids =
      page_data.modified_page_ids
      |> add_unique_page_id(Page.id(left_page_binary))
      |> add_unique_page_id(Page.id(right_page_binary))

    %{
      page_data
      | tree: updated_tree,
        page_map: updated_page_map,
        deleted_page_ids: split_deleted_pages,
        modified_page_ids: updated_modified_page_ids
    }
  end

  @spec clear_all_pending_operations(version_data()) :: version_data()
  defp clear_all_pending_operations(%VersionData{} = page_data) do
    %{page_data | pending_operations: %{}}
  end

  # Helper Functions

  @spec last_committed_version(version_manager :: t()) :: Bedrock.version()
  def last_committed_version(version_manager), do: version_manager.current_version

  @spec last_durable_version(version_manager :: t()) :: Bedrock.version()
  def last_durable_version(version_manager), do: version_manager.durable_version

  @spec oldest_durable_version(version_manager :: t()) :: Bedrock.version()
  def oldest_durable_version(version_manager), do: version_manager.durable_version

  @spec purge_transactions_newer_than(version_manager :: t(), version :: Bedrock.version()) :: :ok
  def purge_transactions_newer_than(_version_manager, _version) do
    :ok
  end

  @spec info(version_manager :: t(), atom()) :: term()
  def info(version_manager, stat) do
    case stat do
      # Key count tracking will be implemented in a future phase.
      # This will require maintaining counters of unique keys across
      # all versions and pages in the version manager.
      :n_keys -> 0
      # Size tracking will be implemented in a future phase.
      # This will require summing the byte size of all pages and
      # values across versions, including lookaside buffer data.
      :size_in_bytes -> 0
      # Utilization tracking will be implemented in a future phase.
      # This will provide metrics on storage efficiency, including
      # page fill ratios and memory usage patterns.
      :utilization -> 0.0
      # Key range tracking will be implemented in a future phase.
      # This will maintain metadata about the range of keys managed
      # by this version manager for partition coordination.
      :key_ranges -> []
      :max_page_id -> version_manager.max_page_id
      :free_page_ids -> version_manager.free_page_ids
      _ -> :undefined
    end
  end

  @spec persist_values_to_database(version_manager :: t(), Database.t(), [
          {Bedrock.key(), Bedrock.version(), Bedrock.value()}
        ]) :: :ok | {:error, term()}
  def persist_values_to_database(_version_manager, database, key_value_version_tuples) do
    key_value_tuples =
      Enum.map(key_value_version_tuples, fn {key, _version, value} ->
        {key, value}
      end)

    Database.batch_store_values(database, key_value_tuples)
  end

  @doc """
  Persists a complete version to storage, including all pages and ETS values.
  Uses pipeline operations for clear data flow and guard clauses for intelligent routing.
  Applies consolidation principles learned during optimization session.
  """
  @spec persist_version_to_storage(version_manager :: t(), Database.t(), {Bedrock.version(), version_data()}) ::
          :ok | {:error, term()}
  def persist_version_to_storage(
        version_manager,
        database,
        {version, %VersionData{tree: tree, page_map: page_map, modified_page_ids: modified_page_ids}}
      ) do
    modified_pages_result =
      modified_page_ids
      |> Enum.map(&Map.fetch!(page_map, &1))
      |> persist_pages_batch(database)

    ets_values_result =
      version_manager.lookaside_buffer
      |> extract_version_values(version)
      |> persist_values_batch(database)

    tree_result = persist_tree_metadata(database, tree, version)

    with :ok <- modified_pages_result,
         :ok <- ets_values_result do
      tree_result
    end
  end

  # Helper functions using consolidation principles
  @spec persist_pages_batch([Page.t()], Database.t()) :: :ok | {:error, term()}
  defp persist_pages_batch(pages, database) do
    Enum.reduce_while(pages, :ok, fn page_binary, :ok ->
      case Database.store_page(database, Page.id(page_binary), page_binary) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @spec extract_version_values(:ets.tid(), Bedrock.version()) :: [{Bedrock.key(), Bedrock.version(), Bedrock.value()}]
  defp extract_version_values(ets_table, version) do
    match_pattern = {{version, :"$1"}, :"$2"}

    ets_table
    |> :ets.match(match_pattern)
    |> Enum.map(fn [key, value] -> {key, version, value} end)
  end

  @spec persist_values_batch([{Bedrock.key(), Bedrock.version(), Bedrock.value()}], Database.t()) ::
          :ok | {:error, term()}
  defp persist_values_batch([], _database), do: :ok

  defp persist_values_batch(values, database) do
    key_value_tuples =
      Enum.map(values, fn {key, _version, value} ->
        {key, value}
      end)

    Database.batch_store_values(database, key_value_tuples)
  end

  @spec persist_tree_metadata(Database.t(), :gb_trees.tree(), Bedrock.version()) :: :ok | {:error, term()}
  defp persist_tree_metadata(_database, _tree, _version) do
    # For now, tree metadata persistence is not needed
    # This provides extension point for future tree-based recovery optimizations
    :ok
  end

  @spec advance_window_with_persistence(version_manager :: t(), Database.t(), window_data :: term()) :: {:ok, t()}
  def advance_window_with_persistence(version_manager, database, _window_data) do
    # Real durability implementation: persist versions that will be evicted
    window_start_version = calculate_window_start(version_manager)

    version_manager.versions
    |> split_versions_at_window(window_start_version)
    |> case do
      {_versions_to_keep, []} ->
        {:ok, version_manager}

      {versions_to_keep, versions_to_evict} ->
        new_durable_version = elem(List.first(versions_to_evict), 0)

        {all_pages, all_values} = collect_persistence_data(version_manager, versions_to_evict)

        with :ok <- Database.batch_persist_all(database, all_pages, all_values, new_durable_version),
             :ok <- Database.sync(database) do
          :ok = cleanup_lookaside_buffer_for_versions(version_manager, versions_to_evict)

          updated_vm = %{version_manager | versions: versions_to_keep, durable_version: new_durable_version}

          {:ok, updated_vm}
        end
    end
  end

  # Optimized helper function to collect all persistence data in one pass
  @spec collect_persistence_data(t(), [{Bedrock.version(), version_data()}]) ::
          {[{Database.page_id(), binary()}], [{Bedrock.key(), Bedrock.value()}]}
  defp collect_persistence_data(version_manager, versions_to_evict) do
    {pages_acc, values_acc} =
      Enum.reduce(versions_to_evict, {[], []}, fn {version,
                                                   %VersionData{
                                                     page_map: page_map,
                                                     modified_page_ids: modified_page_ids
                                                   }},
                                                  {pages_acc, values_acc} ->
        version_pages =
          Enum.map(modified_page_ids, fn page_id ->
            page_binary = Map.fetch!(page_map, page_id)
            {page_id, page_binary}
          end)

        version_values = extract_version_values_optimized(version_manager.lookaside_buffer, version)

        {pages_acc ++ version_pages, values_acc ++ version_values}
      end)

    {pages_acc, values_acc}
  end

  # Optimized extraction that directly produces {key, value} tuples (no intermediate 3-tuples)
  @spec extract_version_values_optimized(:ets.tid(), Bedrock.version()) :: [{Bedrock.key(), Bedrock.value()}]
  defp extract_version_values_optimized(ets_table, version) do
    match_pattern = {{version, :"$1"}, :"$2"}

    ets_table
    |> :ets.match(match_pattern)
    |> Enum.map(fn [key, value] -> {key, value} end)
  end

  # Helper function using consolidation principles
  @spec cleanup_lookaside_buffer_for_versions(t(), [{Bedrock.version(), version_data()}]) :: :ok
  defp cleanup_lookaside_buffer_for_versions(version_manager, versions_to_evict) do
    Enum.each(versions_to_evict, fn {version, _} ->
      match_pattern = {{version, :_}, :_}
      :ets.match_delete(version_manager.lookaside_buffer, match_pattern)
    end)

    :ok
  end

  @spec next_id(version_manager :: t()) :: {page_id(), t()}
  def next_id(version_manager) do
    case version_manager.free_page_ids do
      [page_id | rest] ->
        {page_id, %{version_manager | free_page_ids: rest}}

      [] ->
        new_page_id = version_manager.max_page_id + 1
        {new_page_id, %{version_manager | max_page_id: new_page_id}}
    end
  end

  # Version Window Management Functions (Phase 2.1)

  @doc """
  Calculates the start of the sliding window based on the current version.
  The window is defined relative to the current (highest applied) version.
  For now, we keep all versions - proper windowing can be implemented later
  without breaking the version abstraction.
  """
  @spec calculate_window_start(t()) :: Bedrock.version()
  # Calculate window start based on current version (timestamp) minus window size
  # Versions are microsecond timestamps, so we subtract window_size_in_microseconds
  def calculate_window_start(version_manager) do
    Version.subtract(version_manager.current_version, version_manager.window_size_in_microseconds)
  rescue
    ArgumentError ->
      # Underflow - return zero version
      Version.zero()
  end

  @doc """
  Checks if a version falls within the sliding time window.
  Uses direct binary comparison since versions are lexicographically ordered.
  """
  @spec version_in_window?(Bedrock.version(), Bedrock.version()) :: boolean()
  def version_in_window?(version, window_start_version) do
    version >= window_start_version
  end

  @doc """
  Efficiently splits the versions list at the window boundary.
  Returns {versions_to_keep, versions_to_evict}.
  Since versions list is ordered descending (newest first), we can split at the cutoff point.
  """
  @spec split_versions_at_window([{Bedrock.version(), version_data()}], Bedrock.version()) ::
          {versions_to_keep :: [{Bedrock.version(), version_data()}],
           versions_to_evict :: [{Bedrock.version(), version_data()}]}
  def split_versions_at_window(versions, window_start_version) do
    split_versions_at_window(versions, window_start_version, [])
  end

  # Optimized version splitting using ordered list traversal
  defp split_versions_at_window([], _window_start_version, kept_versions) do
    # No more versions to check, all remaining versions are kept
    {Enum.reverse(kept_versions), []}
  end

  defp split_versions_at_window(
         [{version, _data} = version_entry | rest] = all_versions,
         window_start_version,
         kept_versions
       ) do
    if version_in_window?(version, window_start_version) do
      # This version is still in window, keep it and continue
      split_versions_at_window(rest, window_start_version, [version_entry | kept_versions])
    else
      # This version is outside window, split here
      # All remaining versions (including this one) should be evicted
      {Enum.reverse(kept_versions), all_versions}
    end
  end

  @doc """
  Advances the version manager to a new version with window management.
  - Updates current_version to the new version
  - Evicts expired versions outside the 5-second window
  - Updates durable_version to be the oldest version in the window

  Note: Only transaction application should add entries to the versions list.
  This function only manages version advancement and window eviction.
  """
  @spec advance_version(t(), Bedrock.version()) :: t()
  def advance_version(version_manager, new_version) do
    window_start_version = calculate_window_start(version_manager)

    {versions_to_keep, versions_to_evict} = split_versions_at_window(version_manager.versions, window_start_version)

    # Note: advance_version only manages in-memory version eviction
    # Persistence should be handled by advance_window_with_persistence before calling this function
    # This separation allows for proper error handling and transaction semantics

    new_durable_version =
      case versions_to_evict do
        [] -> version_manager.durable_version
        [{first_evicted_version, _} | _] -> first_evicted_version
      end

    updated_version_manager = %{
      version_manager
      | versions: versions_to_keep,
        current_version: new_version,
        durable_version: new_durable_version
    }

    if new_durable_version != version_manager.durable_version do
      :ok = cleanup_lookaside_buffer(updated_version_manager, new_durable_version)
    end

    updated_version_manager
  end

  # Helper Functions for MVCC Value Retrieval (Phase 3.2)

  @doc """
  Finds the best version data for fetch operations using MVCC semantics.
  Returns the latest version that is <= the target version.
  """
  @spec find_best_version_for_fetch(
          [{Bedrock.version(), version_data()}],
          Bedrock.version()
        ) ::
          version_data() | nil
  def find_best_version_for_fetch(versions, target_version) do
    find_first_valid_version(versions, target_version)
  end

  defp find_first_valid_version([], _target_version), do: nil

  defp find_first_valid_version([{version, data} | rest], target_version) do
    if target_version < version do
      find_first_valid_version(rest, target_version)
    else
      data
    end
  end

  @spec split_page_with_tree_update(page :: Page.t(), version_manager :: t()) ::
          {{Page.t(), Page.t()}, t()} | {:error, :no_split_needed}
  def split_page_with_tree_update(page, version_manager) do
    case split_page_simple(page, version_manager) do
      {:error, :no_split_needed} = error ->
        error

      {{left_page, right_page}, updated_vm} ->
        case updated_vm.versions do
          [] ->
            {{left_page, right_page}, updated_vm}

          [{current_version, %VersionData{} = version_data} | rest_versions] ->
            tree =
              version_data.tree
              |> Tree.remove_page_from_tree(page)
              |> Tree.add_page_to_tree(left_page)
              |> Tree.add_page_to_tree(right_page)

            updated_version_data = %{version_data | tree: tree}
            updated_versions = [{current_version, updated_version_data} | rest_versions]

            final_vm = %{updated_vm | versions: updated_versions}

            {{left_page, right_page}, final_vm}
        end
    end
  end

  @doc """
  Gets the current tree from the version manager.
  Always returns the tree from the top of the versions list.
  The versions list should always be available and prepared in recover_from_database.
  """
  @spec get_current_tree(t()) :: :gb_trees.tree()
  def get_current_tree(%__MODULE__{versions: []}),
    do: raise("Invalid state: version manager has no versions - this indicates a bug in recovery or initialization")

  def get_current_tree(%__MODULE__{versions: [{_version, %VersionData{tree: tree}} | _]}), do: tree

  @doc """
  Removes all entries for a specific version from the lookaside buffer.
  This is typically called after successfully flushing a version to persistent storage.
  """
  @spec remove_version_entries(t(), Bedrock.version()) :: :ok
  def remove_version_entries(version_manager, target_version) do
    buffer = version_manager.lookaside_buffer
    match_pattern = {{target_version, :_}, :_}
    :ets.match_delete(buffer, match_pattern)
    :ok
  end

  @doc """
  Efficiently removes all entries for versions older than or equal to the durable version.
  This is a more efficient way to clean up the lookaside buffer when advancing the durable version.
  Uses a single match_delete operation to remove all obsolete entries at once.
  """
  @spec cleanup_lookaside_buffer(t(), Bedrock.version()) :: :ok
  def cleanup_lookaside_buffer(version_manager, durable_version) do
    buffer = version_manager.lookaside_buffer
    match_pattern = {{:"$1", :_}, :_}
    guard_condition = {:"=<", :"$1", durable_version}
    match_spec = [{match_pattern, [guard_condition], [true]}]
    :ets.select_delete(buffer, match_spec)
    :ok
  end

  @doc """
  Gets the current complete page_data tuple from the version manager.
  Always returns the page_data from the top of the versions list.
  The versions list should always be available and prepared in recover_from_database.
  """
  @spec get_current_page_data(t()) :: version_data()
  def get_current_page_data(version_manager) do
    case version_manager.versions do
      [] ->
        raise "Invalid state: version manager has no versions - this indicates a bug in recovery or initialization"

      [{_version, page_data} | _] ->
        page_data
    end
  end

  @doc """
  Retrieves all entries for a specific version from the lookaside buffer.
  Returns a list of {key, value} tuples for the given version.
  This function is primarily used for testing lookaside buffer functionality.
  """
  @spec get_version_entries(t(), Bedrock.version()) :: [{Bedrock.key(), Bedrock.value()}]
  def get_version_entries(version_manager, version) do
    match_pattern = {{version, :"$1"}, :"$2"}

    version_manager.lookaside_buffer
    |> :ets.match(match_pattern)
    |> Enum.map(fn [key, value] -> {key, value} end)
  end

  @doc """
  Retrieves entries for a range of versions from the lookaside buffer.
  Returns a list of {version, key, value} tuples for versions within the specified range (inclusive).
  This function is primarily used for testing lookaside buffer functionality.
  """
  @spec get_version_range_entries(t(), Bedrock.version(), Bedrock.version()) ::
          [{Bedrock.version(), Bedrock.key(), Bedrock.value()}]
  def get_version_range_entries(version_manager, start_version, end_version) do
    match_pattern = {{:"$1", :"$2"}, :"$3"}
    guard_condition = {:andalso, {:>=, :"$1", start_version}, {:"=<", :"$1", end_version}}
    match_spec = [{match_pattern, [guard_condition], [{{:"$1", :"$2", :"$3"}}]}]

    :ets.select(version_manager.lookaside_buffer, match_spec)
  end

  # Page management functions moved from Page module

  @spec split_page_simple(Page.t(), t()) :: {{Page.t(), Page.t()}, t()} | {:error, :no_split_needed}
  defp split_page_simple(page, version_manager) when length(page.key_versions) > 256 do
    key_versions = page.key_versions
    mid_point = div(length(key_versions), 2)

    {left_key_versions, right_key_versions} = Enum.split(key_versions, mid_point)

    updated_vm = %{version_manager | max_page_id: max(version_manager.max_page_id, page.id)}
    {right_id, vm1} = next_id_from_vm(updated_vm)

    left_id = page.id

    left_page = Page.new(left_id, left_key_versions, right_id)
    right_page = Page.new(right_id, right_key_versions, page.next_id)

    {{left_page, right_page}, vm1}
  end

  defp split_page_simple(_page, _version_manager), do: {:error, :no_split_needed}

  # Binary-optimized page splitting using Page.split_page/3
  @spec split_page_binary(binary(), t()) :: {{binary(), binary()}, t()} | {:error, :no_split_needed}
  defp split_page_binary(page_binary, version_manager) do
    case Page.key_count(page_binary) do
      key_count when key_count > 256 ->
        {new_page_id, version_manager} = next_id_from_vm(version_manager)

        {Page.split_page(page_binary, div(key_count, 2), new_page_id), version_manager}

      _ ->
        {:error, :no_split_needed}
    end
  end

  defp next_id_from_vm(version_manager) do
    case version_manager.free_page_ids do
      [id | rest] ->
        {id, %{version_manager | free_page_ids: rest}}

      [] ->
        new_id = version_manager.max_page_id + 1
        {new_id, %{version_manager | max_page_id: new_id}}
    end
  end

  # Helper function to add clear operations for keys on a specific page
  defp add_clear_operations(operations, page_id, keys_to_clear) do
    Enum.reduce(keys_to_clear, operations, fn key, ops_acc ->
      Map.update(ops_acc, page_id, %{key => :clear}, &Map.put(&1, key, :clear))
    end)
  end

  defp merge_sorted_unique(list1, list2), do: merge_sorted_unique(list1, list2, [])

  defp merge_sorted_unique([h1 | t1], [h2 | t2], acc) when h1 < h2, do: merge_sorted_unique(t1, [h2 | t2], [h1 | acc])
  defp merge_sorted_unique([h1 | t1], [h2 | t2], acc) when h1 > h2, do: merge_sorted_unique([h1 | t1], t2, [h2 | acc])
  defp merge_sorted_unique([h | t1], [h | t2], acc), do: merge_sorted_unique(t1, t2, [h | acc])
  defp merge_sorted_unique([], list2, acc), do: Enum.reverse(acc, list2)
  defp merge_sorted_unique(list1, [], acc), do: Enum.reverse(acc, list1)
end
