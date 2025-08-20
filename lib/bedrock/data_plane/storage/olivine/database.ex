defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  @type page_id :: non_neg_integer()

  @opaque t :: %__MODULE__{
            dets_storage: :dets.tab_name(),
            window_size_in_microseconds: pos_integer()
          }
  defstruct dets_storage: nil,
            # 5000ms * 1000
            window_size_in_microseconds: 5_000_000

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
        {:ok,
         %__MODULE__{
           dets_storage: dets_table,
           window_size_in_microseconds: window_in_ms * 1_000
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec close(database :: t()) :: :ok
  def close(database) do
    # Simply try to close, ignore any errors
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

  @spec store_page(database :: t(), page_id :: page_id(), page_binary :: binary()) :: :ok | {:error, term()}
  def store_page(database, page_id, page_binary) do
    case :dets.insert(database.dets_storage, {page_id, page_binary}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_page(database :: t(), page_id :: page_id()) :: {:ok, binary()} | {:error, :not_found}
  def load_page(database, page_id) do
    case :dets.lookup(database.dets_storage, page_id) do
      [{^page_id, page_binary}] -> {:ok, page_binary}
      [] -> {:error, :not_found}
    end
  end

  @spec store_value(database :: t(), key :: Bedrock.key(), value :: Bedrock.value()) ::
          :ok | {:error, term()}
  def store_value(database, key, value) do
    case :dets.insert(database.dets_storage, {key, value}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_value(database :: t(), key :: Bedrock.key()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(database, key) do
    case :dets.lookup(database.dets_storage, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  @spec get_all_page_ids(database :: t()) :: [page_id()]
  def get_all_page_ids(database) do
    :dets.foldl(
      fn
        {page_id, _page_binary}, acc when is_integer(page_id) ->
          [page_id | acc]

        {key, _value}, acc when is_binary(key) ->
          # Skip value entries
          acc
      end,
      [],
      database.dets_storage
    )
  end

  @spec batch_store_values(database :: t(), [{Bedrock.key(), Bedrock.value()}]) ::
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
          database :: t(),
          pages :: [{page_id(), binary()}],
          values :: [{Bedrock.key(), Bedrock.value()}],
          durable_version :: Bedrock.version()
        ) :: :ok | {:error, term()}
  def batch_persist_all(database, pages, values, durable_version) do
    # Combine all DETS entries into a single transaction:
    # - Pages: {page_id, page_binary}
    # - Values: {key, value}
    # - Durable version: {:durable_version, version}

    page_entries = Enum.map(pages, fn {page_id, page_binary} -> {page_id, page_binary} end)
    value_entries = Enum.map(values, fn {key, value} -> {key, value} end)
    version_entry = {:durable_version, durable_version}

    all_entries = page_entries ++ value_entries ++ [version_entry]

    case :dets.insert(database.dets_storage, all_entries) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec store_durable_version(database :: t(), version :: Bedrock.version()) ::
          :ok | {:error, term()}
  def store_durable_version(database, version) do
    case :dets.insert(database.dets_storage, {:durable_version, version}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_durable_version(database :: t()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_durable_version(database) do
    case :dets.lookup(database.dets_storage, :durable_version) do
      [{:durable_version, version}] -> {:ok, version}
      [] -> {:error, :not_found}
    end
  end

  @spec info(database :: t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) ::
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
    # Simple utilization estimate
    file_size = :dets.info(dets_storage, :file_size) || 1
    min(1.0, objects / max(1, file_size / 1000))
  end

  @spec sync(database :: t()) :: :ok
  def sync(database) do
    :dets.sync(database.dets_storage)
    :ok
  catch
    # Handle any sync errors gracefully
    _, _ -> :ok
  end
end
