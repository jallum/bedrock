defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  alias Bedrock.DataPlane.Version

  @type page_id :: non_neg_integer()

  @opaque t :: %__MODULE__{
            dets_storage: :dets.tab_name(),
            window_size_in_microseconds: pos_integer(),
            lookaside_buffer: :ets.tab(),
            durable_version: Bedrock.version()
          }
  defstruct dets_storage: nil,
            window_size_in_microseconds: 5_000_000,
            lookaside_buffer: nil,
            durable_version: nil

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), window_in_ms :: pos_integer()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, window_in_ms \\ 5_000) when is_atom(otp_name) do
    storage_opts = [
      {:type, :set},
      {:access, :read_write},
      {:auto_save, :infinity},
      {:estimated_no_objects, 1_000_000}
    ]

    case :dets.open_file(otp_name, [{:file, String.to_charlist(file_path)} | storage_opts]) do
      {:ok, dets_table} ->
        lookaside_buffer = :ets.new(:lookaside_buffer, [:ordered_set, :protected, {:read_concurrency, true}])

        durable_version =
          case load_durable_version_internal(dets_table) do
            {:ok, version} -> version
            {:error, :not_found} -> Version.zero()
          end

        {:ok,
         %__MODULE__{
           dets_storage: dets_table,
           window_size_in_microseconds: window_in_ms * 1_000,
           lookaside_buffer: lookaside_buffer,
           durable_version: durable_version
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec close(t()) :: :ok
  def close(database) do
    try do
      :ets.delete(database.lookaside_buffer)
    catch
      _, _ -> :ok
    end

    try do
      :dets.sync(database.dets_storage)
    catch
      _, _ -> :ok
    end

    try do
      :dets.close(database.dets_storage)
    catch
      _, _ -> :ok
    end

    :ok
  end

  @spec store_page(t(), page_id :: page_id(), page_binary :: binary()) :: :ok | {:error, term()}
  def store_page(database, page_id, page_binary) do
    case :dets.insert(database.dets_storage, {page_id, page_binary}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_page(t(), page_id :: page_id()) :: {:ok, binary()} | {:error, :not_found}
  def load_page(database, page_id) do
    case :dets.lookup(database.dets_storage, page_id) do
      [{^page_id, page_binary}] -> {:ok, page_binary}
      [] -> {:error, :not_found}
    end
  end

  @spec store_value(t(), key :: Bedrock.key(), value :: Bedrock.value()) ::
          :ok | {:error, term()}
  def store_value(database, key, value) do
    case :dets.insert(database.dets_storage, {key, value}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_value(t(), key :: Bedrock.key()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(database, key) do
    case :dets.lookup(database.dets_storage, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Unified value fetch that handles both lookaside buffer and DETS storage.
  Routes between hot (ETS) and cold (DETS) storage based on version vs durable_version.
  """
  @spec fetch_value(t(), key :: Bedrock.key(), version :: Bedrock.version()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  def fetch_value(database, key, version) do
    if version > database.durable_version do
      case :ets.lookup(database.lookaside_buffer, {version, key}) do
        [{_key_version, value}] -> {:ok, value}
        [] -> {:error, :not_found}
      end
    else
      load_value(database, key)
    end
  end

  @doc """
  Store a value in the lookaside buffer for the given version and key.
  This is used during transaction application for values within the window.
  """
  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) ::
          :ok | {:error, term()}
  def store_value(database, key, version, value) do
    if :ets.insert_new(database.lookaside_buffer, {{version, key}, value}) do
      :ok
    else
      {:error, :already_exists}
    end
  end

  @doc """
  Store a page in the lookaside buffer for the given version and page_id.
  This is used during transaction application for modified pages within the window.
  """
  @spec store_page_version(t(), page_id(), version :: Bedrock.version(), page_binary :: binary()) ::
          :ok | {:error, term()}
  def store_page_version(database, page_id, version, page_binary) do
    if :ets.insert_new(database.lookaside_buffer, {{version, {:page, page_id}}, page_binary}) do
      :ok
    else
      {:error, :already_exists}
    end
  end

  @doc """
  Batch store values and pages in the lookaside buffer for a given version.
  This enables atomic writes during transaction application.
  """
  @spec batch_store_version_data(
          t(),
          version :: Bedrock.version(),
          values :: [{Bedrock.key(), Bedrock.value()}],
          pages :: [{page_id(), binary()}]
        ) :: :ok | {:error, term()}
  def batch_store_version_data(database, version, values, pages) do
    value_entries = Enum.map(values, fn {key, value} -> {{version, key}, value} end)
    page_entries = Enum.map(pages, fn {page_id, page_binary} -> {{version, {:page, page_id}}, page_binary} end)

    all_entries = value_entries ++ page_entries

    if :ets.insert_new(database.lookaside_buffer, all_entries) do
      :ok
    else
      {:error, :insert_failed}
    end
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec value_loader(t()) ::
          (Bedrock.key(), Bedrock.version() ->
             {:ok, Bedrock.value()}
             | {:error, :not_found}
             | {:error, :shutting_down})
  def value_loader(database) do
    dets_storage = database.dets_storage
    lookaside_buffer = database.lookaside_buffer
    durable_version = database.durable_version

    fn
      key, version when version > durable_version ->
        case :ets.lookup(lookaside_buffer, {version, key}) do
          [{_key_version, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end

      key, _version ->
        case :dets.lookup(dets_storage, key) do
          [{^key, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end
    end
  end

  @spec get_all_page_ids(t()) :: [page_id()]
  def get_all_page_ids(database) do
    :dets.foldl(
      fn
        {page_id, _page_binary}, acc when is_integer(page_id) ->
          [page_id | acc]

        {key, _value}, acc when is_binary(key) ->
          acc
      end,
      [],
      database.dets_storage
    )
  end

  @spec batch_store_values(t(), [{Bedrock.key(), Bedrock.value()}]) ::
          :ok | {:error, term()}
  def batch_store_values(database, key_value_tuples) do
    entries =
      Enum.map(key_value_tuples, fn {key, value} ->
        {key, value}
      end)

    case :dets.insert(database.dets_storage, entries) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec batch_persist_all(
          t(),
          pages :: [{page_id(), binary()}],
          values :: [{Bedrock.key(), Bedrock.value()}],
          durable_version :: Bedrock.version()
        ) :: :ok | {:error, term()}
  def batch_persist_all(database, pages, values, durable_version) do
    all_entries = pages ++ values ++ [{:durable_version, durable_version}]

    case :dets.insert(database.dets_storage, all_entries) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec store_durable_version(t(), version :: Bedrock.version()) ::
          {:ok, t()} | {:error, term()}
  def store_durable_version(database, version) do
    case :dets.insert(database.dets_storage, {:durable_version, version}) do
      :ok ->
        updated_database = %{database | durable_version: version}
        {:ok, updated_database}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Internal function for loading durable version during initialization
  @spec load_durable_version_internal(:dets.tab_name()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  defp load_durable_version_internal(dets_storage) do
    case :dets.lookup(dets_storage, :durable_version) do
      [{:durable_version, version}] -> {:ok, version}
      [] -> {:error, :not_found}
    end
  end

  @spec load_durable_version(t()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_durable_version(database) do
    {:ok, database.durable_version}
  end

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) ::
          any() | :undefined
  def info(database, stat) do
    case stat do
      :n_keys ->
        :dets.info(database.dets_storage, :no_objects) || 0

      :size_in_bytes ->
        :dets.info(database.dets_storage, :file_size) || 0

      :utilization ->
        calculate_utilization(database.dets_storage)

      # Key range tracking will be implemented in a future phase.
      # This will require maintaining metadata about the range of keys
      # stored in the database, supporting efficient range queries and
      # partition management across distributed storage nodes.
      :key_ranges ->
        []

      _ ->
        :undefined
    end
  end

  defp calculate_utilization(dets_storage) do
    case :dets.info(dets_storage, :no_objects) do
      nil -> 0.0
      0 -> 0.0
      objects -> calculate_utilization_ratio(objects, dets_storage)
    end
  end

  defp calculate_utilization_ratio(objects, dets_storage) do
    file_size = :dets.info(dets_storage, :file_size) || 1
    min(1.0, objects / max(1, file_size / 1000))
  end

  @spec sync(t()) :: :ok
  def sync(database) do
    :dets.sync(database.dets_storage)
    :ok
  catch
    _, _ -> :ok
  end

  @doc """
  Advances the durable version with full persistence handling.
  This handles the complete persistence process:
  - Extracts data for the specified versions from lookaside buffer
  - Persists all values and pages atomically to DETS
  - Syncs to disk
  - Cleans up lookaside buffer
  - Updates durable version
  """
  @spec advance_durable_version(t(), version :: Bedrock.version(), versions_to_persist :: [Bedrock.version()]) ::
          {:ok, t()} | {:error, term()}
  def advance_durable_version(database, new_durable_version, versions_to_persist) do
    # Extract all values and pages from lookaside buffer for specified versions
    {all_values, all_pages} = extract_persistence_data(database, versions_to_persist)

    with :ok <- batch_persist_all(database, all_pages, all_values, new_durable_version),
         :ok <- sync(database) do
      # Clean up the lookaside buffer for persisted versions
      :ok = cleanup_lookaside_buffer(database, new_durable_version)

      # Update the durable version
      updated_database = %{database | durable_version: new_durable_version}
      {:ok, updated_database}
    end
  end

  @doc """
  Efficiently removes all entries for versions older than or equal to the durable version.
  This is a more efficient way to clean up the lookaside buffer when advancing the durable version.
  Uses a single select_delete operation to remove all obsolete entries at once.
  """
  @spec cleanup_lookaside_buffer(t(), version :: Bedrock.version()) :: :ok
  def cleanup_lookaside_buffer(database, durable_version) do
    match_spec = [{{{:"$1", :_}, :_}, [{:"=<", :"$1", durable_version}], [true]}]
    :ets.select_delete(database.lookaside_buffer, match_spec)
    :ok
  end

  @doc """
  Extract all values and pages from the lookaside buffer for versions up to the durable version.
  Returns separate lists for values and pages ready for DETS persistence.
  This greatly simplifies the flush operation.
  """
  @spec extract_persistence_data(t(), versions :: [Bedrock.version()]) ::
          {values :: [{Bedrock.key(), Bedrock.value()}], pages :: [{page_id(), binary()}]}
  def extract_persistence_data(database, versions) do
    # Simple approach: iterate through all entries and filter
    all_entries = :ets.tab2list(database.lookaside_buffer)
    version_set = MapSet.new(versions)

    # Filter entries that match our versions and separate values and pages
    Enum.reduce(all_entries, {[], []}, fn
      {{version, {:page, page_id}}, page_binary}, {values, pages} ->
        if MapSet.member?(version_set, version) do
          {values, [{page_id, page_binary} | pages]}
        else
          {values, pages}
        end

      {{version, key}, value}, {values, pages} when is_binary(key) ->
        if MapSet.member?(version_set, version) do
          {[{key, value} | values], pages}
        else
          {values, pages}
        end
    end)
  end
end
