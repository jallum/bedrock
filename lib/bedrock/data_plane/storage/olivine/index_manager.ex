defmodule Bedrock.DataPlane.Storage.Olivine.IndexManager do
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

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @type page_id :: Page.id()
  @type page :: Page.t()

  @type loader_fn :: (Bedrock.key(), Bedrock.version() -> {:ok, Bedrock.value()} | {:error, :not_found})

  @type operation :: {:set, Bedrock.version()} | :clear

  @type version_data :: Index.t()
  @type version_update_data :: IndexUpdate.t()

  @opaque t :: %__MODULE__{
            versions: [{Bedrock.version(), version_data()}],
            current_version: Bedrock.version(),
            window_size_in_microseconds: pos_integer(),
            page_allocator: PageAllocator.t()
          }
  defstruct [
    :versions,
    :current_version,
    :window_size_in_microseconds,
    :page_allocator
  ]

  @spec new() :: t()
  def new do
    %__MODULE__{
      versions: [{Version.zero(), Index.new()}],
      current_version: Version.zero(),
      window_size_in_microseconds: 5_000_000,
      page_allocator: PageAllocator.new(0, [])
    }
  end

  @spec recover_from_database(database :: Database.t()) ::
          {:ok, t()} | {:error, :corrupted_page | :broken_chain | :cycle_detected}
  def recover_from_database(database) do
    # Get the durable version from the database
    {:ok, durable_version} = Database.load_durable_version(database)

    # Load the index structure from the database
    case Index.load_from(database) do
      {:ok, initial_index, max_page_id, free_page_ids} ->
        index_manager = %__MODULE__{
          versions: [{durable_version, initial_index}],
          current_version: durable_version,
          window_size_in_microseconds: 5_000_000,
          page_allocator: PageAllocator.new(max_page_id, free_page_ids)
        }

        {:ok, index_manager}

      {:error, reason} when reason in [:corrupted_page, :broken_chain, :cycle_detected, :no_chain] ->
        {:error, reason}
    end
  end

  @spec page_for_key(t(), key :: Bedrock.key(), Bedrock.version()) ::
          {:ok, Page.t()}
          | {:error, :not_found}
          | {:error, :version_too_new}
  def page_for_key(index_manager, _key, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def page_for_key(index_manager, key, version) do
    index_manager.versions
    |> find_best_index_for_fetch(version)
    |> case do
      nil ->
        {:error, :version_too_old}

      index ->
        Index.page_for_key(index, key)
    end
  end

  @spec pages_for_range(t(), start_key :: Bedrock.key(), end_key :: Bedrock.key(), Bedrock.version()) ::
          {:ok, [Page.t()]}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
  def pages_for_range(index_manager, _start_key, _end_key, version) when index_manager.current_version < version,
    do: {:error, :version_too_new}

  def pages_for_range(index_manager, start_key, end_key, version) do
    case find_best_index_for_fetch(index_manager.versions, version) do
      nil ->
        {:error, :version_too_old}

      index ->
        Index.pages_for_range(index, start_key, end_key)
    end
  end

  @spec apply_transactions(index_manager :: t(), encoded_transactions :: [binary()], database :: Database.t()) :: t()
  def apply_transactions(index_manager, [], _database), do: index_manager

  def apply_transactions(index_manager, transactions, database) when is_list(transactions) do
    Enum.reduce(transactions, index_manager, fn transaction, index_manager ->
      apply_transaction(index_manager, transaction, database)
    end)
  end

  # Transaction Processing Functions (Phase 3.1)

  @doc """
  Applies a single transaction binary to the version manager.
  Creates a new version and applies all mutations in the transaction.
  Uses a two-pass approach: first collect all instructions, then process each page.
  """
  @spec apply_transaction(t(), binary(), Database.t()) :: t()
  def apply_transaction(%{versions: [{_version, current_index} | _]} = index_manager, transaction, database) do
    commit_version = Transaction.extract_commit_version!(transaction)

    {new_index, updated_page_allocator} =
      current_index
      |> IndexUpdate.new(commit_version, index_manager.page_allocator)
      |> IndexUpdate.apply_mutations(Transaction.stream_mutations!(transaction), database)
      |> IndexUpdate.process_pending_operations()
      |> IndexUpdate.store_modified_pages(database)
      |> IndexUpdate.finish()

    %{
      index_manager
      | versions: [{commit_version, new_index} | index_manager.versions],
        current_version: commit_version,
        page_allocator: updated_page_allocator
    }
  end

  # Helper Functions

  @spec last_committed_version(index_manager :: t()) :: Bedrock.version()
  def last_committed_version(index_manager), do: index_manager.current_version

  # These functions are deprecated - durable version is now managed by Database
  @spec last_durable_version(index_manager :: t()) :: nil
  def last_durable_version(_index_manager), do: nil

  @spec oldest_durable_version(index_manager :: t()) :: nil
  def oldest_durable_version(_index_manager), do: nil

  @spec purge_transactions_newer_than(index_manager :: t(), version :: Bedrock.version()) :: :ok
  def purge_transactions_newer_than(_index_manager, _version) do
    :ok
  end

  @spec info(index_manager :: t(), atom()) :: term()
  def info(index_manager, stat) do
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
      :max_page_id -> index_manager.page_allocator.max_page_id
      :free_page_ids -> index_manager.page_allocator.free_page_ids
      _ -> :undefined
    end
  end

  @doc """
  Determines what needs to happen for window advancement.
  Returns either :no_eviction or {:evict, new_durable_version, evicted_versions, updated_vm}.
  VersionManager handles all the version management logic internally.
  """
  @spec prepare_window_advancement(t()) ::
          :no_eviction
          | {:evict, new_durable_version :: Bedrock.version(), evicted_versions :: [Bedrock.version()],
             updated_vm :: t()}
  def prepare_window_advancement(index_manager) do
    window_start_version = calculate_window_start(index_manager)

    index_manager.versions
    |> split_versions_at_window(window_start_version)
    |> case do
      {_versions_to_keep, []} ->
        :no_eviction

      {versions_to_keep, versions_to_evict} ->
        new_durable_version = elem(List.first(versions_to_evict), 0)
        evicted_versions = Enum.map(versions_to_evict, fn {version, _} -> version end)

        {:evict, new_durable_version, evicted_versions, %{index_manager | versions: versions_to_keep}}
    end
  end

  @doc """
  Calculates the start of the sliding window based on the current version.
  The window is defined relative to the current (highest applied) version.
  For now, we keep all versions - proper windowing can be implemented later
  without breaking the version abstraction.
  """
  @spec calculate_window_start(t()) :: Bedrock.version()
  # Calculate window start based on current version (timestamp) minus window size
  # Versions are microsecond timestamps, so we subtract window_size_in_microseconds
  def calculate_window_start(index_manager) do
    Version.subtract(index_manager.current_version, index_manager.window_size_in_microseconds)
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
  def advance_version(index_manager, new_version) do
    window_start_version = calculate_window_start(index_manager)

    {versions_to_keep, _versions_to_evict} = split_versions_at_window(index_manager.versions, window_start_version)

    # Note: advance_version only manages in-memory version eviction
    # Persistence should be handled by advance_window_with_persistence before calling this function
    # This separation allows for proper error handling and transaction semantics

    %{index_manager | versions: versions_to_keep, current_version: new_version}
  end

  # Helper Functions for MVCC Value Retrieval (Phase 3.2)

  @doc """
  Finds the best version data for fetch operations using MVCC semantics.
  Returns the latest version that is <= the target version.
  """
  @spec find_best_index_for_fetch(
          [{Bedrock.version(), version_data()}],
          Bedrock.version()
        ) ::
          version_data() | nil
  def find_best_index_for_fetch(versions, target_version) do
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
end
