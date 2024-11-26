defmodule Bedrock.DataPlane.Storage.Basalt.PersistentKeyValues do
  @moduledoc """
  """

  alias Bedrock.DataPlane.Log.Transaction

  @opaque t :: :dets.tab_name()

  @doc """
  Opens a persistent key-value store.
  """
  @spec open(atom(), String.t()) :: {:ok, t()} | {:error, term()}
  def open(name, file_path) when is_atom(name) do
    :dets.open_file(name,
      access: :read_write,
      auto_save: :infinity,
      type: :set,
      file: file_path |> String.to_charlist()
    )
  end

  @doc """
  Closes a persistent key-value store.
  """
  @spec close(t()) :: :ok
  def close(dets),
    do: :dets.close(dets)

  @doc """
  Returns the last version of the key-value store.
  """
  @spec oldest_version(t()) :: Bedrock.version()
  def oldest_version(pkv) do
    fetch(pkv, :oldest_version)
    |> case do
      {:error, :not_found} -> 0
      {:ok, version} -> version
    end
  end

  @doc """
  Returns the last version of the key-value store.
  """
  @spec last_version(t()) :: Bedrock.version()
  def last_version(pkv) do
    fetch(pkv, :last_version)
    |> case do
      {:error, :not_found} -> 0
      {:ok, version} -> version
    end
  end

  @doc """
  Apply a transaction to the key-value store, atomically. The transaction must
  be applied in order.
  """
  @spec apply_transaction(pkv :: t(), Transaction.t()) ::
          :ok
          | {:error, :version_too_new}
          | {:error, :version_too_old}
  def apply_transaction(pkv, transaction) do
    version = Transaction.version(transaction)
    last_version = last_version(pkv)

    with :ok <- check_version(version, last_version),
         writes <- Transaction.key_values(transaction) |> Enum.to_list(),
         :ok <- :dets.insert(pkv, [{:last_version, version} | writes]) do
      :dets.sync(pkv)
    end
  end

  defp check_version(version, last_version) when version >= last_version, do: :ok
  defp check_version(_, _), do: {:error, :version_too_old}

  @doc """
  Attempt to find the value for the given key in the key-value store. Returns
  `nil` if the key is not found.
  """
  @spec fetch(pkv :: t(), key :: term()) :: {:ok, term()} | {:error, :not_found}
  def fetch(pkv, key) do
    pkv
    |> :dets.lookup(key)
    |> case do
      [] -> {:error, :not_found}
      [{_, value}] -> {:ok, value}
    end
  end

  @doc """
  Interrogate the key-value store for specific metadata. Supported queries are:

  * `:n_keys` - the number of keys in the store
  * `:size_in_bytes` - the size of the store in bytes
  * `:utilization` - the utilization of the database (as a percentage, expressed
    as a float between 0.0 and 1.0)
  * `:key_ranges` - the key ranges for which this store is responsible
  """
  @spec info(pkv :: t(), :n_keys | :size_in_bytes | :utilization | :key_ranges) ::
          any() | :undefined
  def info(pkv, :n_keys) do
    # We don't count the :last_version key
    pkv
    |> :dets.info(:no_objects)
    |> case do
      0 -> 0
      n_keys -> n_keys - 1
    end
  end

  def info(pkv, :key_ranges) do
    pkv
    |> :dets.lookup(:key_ranges)
    |> case do
      key_ranges when is_list(key_ranges) -> key_ranges
      _ -> []
    end
  end

  def info(pkv, :utilization) do
    pkv
    |> :dets.info(:no_slots)
    |> case do
      {min, used, max} -> Float.ceil((used - min) / max, 1)
      :undefined -> :undefined
    end
  end

  def info(pkv, :size_in_bytes), do: pkv |> :dets.info(:file_size)

  def info(_pkv, _query), do: :undefined

  @doc """
  Prune the key-value store of any keys that have a `nil` value.
  """
  @spec prune(pkv :: t()) :: {:ok, n_pruned :: non_neg_integer()}
  def prune(pkv) do
    n_pruned = :dets.select_delete(pkv, [{{:_, :"$1"}, [{:is_nil}], [true]}])
    {:ok, n_pruned}
  end

  @doc """
  Return a stream of all keys in the key-value store. The keys are not
  guaranteed to be in any particular order.
  """
  @spec stream_keys(pkv :: t()) :: Enumerable.t()
  def stream_keys(pkv) do
    Stream.resource(
      fn -> :dets.first(pkv) end,
      fn
        :"$end_of_table" -> {:halt, :ok}
        key -> {[key], :dets.next(pkv, key)}
      end,
      fn _ -> :ok end
    )
    |> Stream.filter(&is_binary/1)
  end
end
