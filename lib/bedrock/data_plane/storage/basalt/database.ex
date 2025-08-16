defmodule Bedrock.DataPlane.Storage.Basalt.Database do
  @moduledoc false

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Storage.Basalt.Keyspace
  alias Bedrock.DataPlane.Storage.Basalt.MultiVersionConcurrencyControl, as: MVCC
  alias Bedrock.DataPlane.Storage.Basalt.PersistentKeyValues
  alias Bedrock.DataPlane.Version

  @opaque t :: %__MODULE__{
            mvcc: MVCC.t(),
            keyspace: Keyspace.t(),
            pkv: PersistentKeyValues.t(),
            window_size_in_microseconds: pos_integer()
          }
  defstruct mvcc: nil,
            keyspace: nil,
            pkv: nil,
            # 5000ms * 1000
            window_size_in_microseconds: 5_000_000

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), window_in_ms :: pos_integer()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, window_in_ms \\ 5_000) when is_atom(otp_name) do
    with {:ok, pkv} <- PersistentKeyValues.open(:"#{otp_name}_pkv", file_path),
         last_durable_version <- PersistentKeyValues.last_version(pkv),
         mvcc <- MVCC.new(:"#{otp_name}_mvcc", last_durable_version),
         keyspace <- Keyspace.new(:"#{otp_name}_keyspace"),
         :ok <- load_keys_into_keyspace(pkv, keyspace) do
      {:ok,
       %__MODULE__{
         mvcc: mvcc,
         keyspace: keyspace,
         pkv: pkv,
         window_size_in_microseconds: window_in_ms * 1_000
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  @spec close(database :: t()) :: :ok
  def close(database) do
    with :ok <- ensure_durability_to_latest_version(database),
         :ok <- PersistentKeyValues.close(database.pkv),
         :ok <- Keyspace.close(database.keyspace),
         :ok <- MVCC.close(database.mvcc) do
    end

    :ok
  end

  @spec oldest_durable_version(database :: t()) :: Bedrock.version()
  def oldest_durable_version(database), do: database.pkv |> PersistentKeyValues.oldest_version()

  @spec last_durable_version(database :: t()) :: Bedrock.version()
  def last_durable_version(database), do: database.pkv |> PersistentKeyValues.last_version()

  @spec load_keys_into_keyspace(PersistentKeyValues.t(), Keyspace.t()) :: :ok
  def load_keys_into_keyspace(pkv, keyspace) do
    PersistentKeyValues.stream_keys(pkv)
    |> Stream.chunk_every(1_000)
    |> Stream.map(fn keys -> :ok = Keyspace.insert_many(keyspace, keys) end)
    |> Stream.run()
  end

  @spec apply_transactions(database :: t(), transactions :: [BedrockTransaction.encoded()]) ::
          Bedrock.version()
  def apply_transactions(database, encoded_transactions),
    do: MVCC.apply_transactions!(database.mvcc, encoded_transactions)

  @spec last_committed_version(t()) :: Bedrock.version()
  def last_committed_version(database),
    do: MVCC.newest_version(database.mvcc)

  @spec fetch(database :: t(), Bedrock.key(), Bedrock.version()) ::
          {:ok, Bedrock.value()}
          | {:error,
             :not_found
             | :key_out_of_range}
  @spec fetch(t(), Bedrock.key(), Bedrock.version()) :: Bedrock.value() | :not_found
  def fetch(%{key_range: {min_key, max_key}}, key, _version)
      when key < min_key or key >= max_key,
      do: {:error, :key_out_of_range}

  @spec fetch(t(), Bedrock.key(), Bedrock.version()) :: Bedrock.value() | :not_found
  def fetch(database, key, version) do
    MVCC.fetch(database.mvcc, key, version)
    |> case do
      {:error, :not_found} ->
        cond do
          not Keyspace.key_exists?(database.keyspace, key) ->
            {:error, :not_found}

          Version.older?(version, MVCC.oldest_version(database.mvcc)) ->
            {:error, :version_too_old}

          true ->
            fetch_from_persistence_and_write_back_to_mvcc(database, key, version)
        end

      result ->
        result
    end
  end

  @spec fetch_from_persistence_and_write_back_to_mvcc(t(), Bedrock.key(), Bedrock.version()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  defp fetch_from_persistence_and_write_back_to_mvcc(database, key, version) do
    PersistentKeyValues.fetch(database.pkv, key)
    |> case do
      {:ok, value} = result ->
        :ok = MVCC.insert_read(database.mvcc, key, version, value)
        result

      {:error, :not_found} = result ->
        result
    end
  end

  @spec purge_transactions_newer_than(t(), Bedrock.version()) :: :ok
  def purge_transactions_newer_than(database, version) do
    :ok = MVCC.purge_keys_newer_than_version(database.mvcc, version)
  end

  @doc """
  Returns information about the database. The following statistics are
  available:

  * `:n_keys` - the number of keys in the store
  * `:size_in_bytes` - the size of the database in bytes
  * `:utilization` - the utilization of the database (as a percentage, expressed
    as a float between 0.0 and 1.0)
  """
  @spec info(database :: t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) ::
          any() | :undefined
  @spec info(t(), atom()) :: term()
  def info(database, stat),
    do: database.pkv |> PersistentKeyValues.info(stat)

  @doc """
  Ensures that the database is durable up to the latest version.
  """
  @spec ensure_durability_to_latest_version(db :: t()) :: :ok
  def ensure_durability_to_latest_version(db),
    do: ensure_durability_to_version(db, MVCC.newest_version(db.mvcc))

  @doc """
  Ensures durability for versions outside the time-based window. Flushes
  transactions older than the trailing edge (latest - window_size) to disk.
  """
  @spec ensure_durability_within_window(database :: t()) :: :ok
  def ensure_durability_within_window(database) do
    newest_version_int =
      database.mvcc
      |> MVCC.newest_version()
      |> Version.to_integer()

    trailing_edge =
      max(0, newest_version_int - database.window_size_in_microseconds)
      |> Version.from_integer()

    if Version.older?(database.mvcc |> MVCC.oldest_version(), trailing_edge) do
      ensure_durability_to_version(database, trailing_edge)
    else
      :ok
    end
  end

  @doc """
  Ensures that the database is durable up to the given version. This is done by
  applying all transactions up to the given version to the the underlying
  persistent key value store. Versions of values older than the given version
  are pruned from the store.
  """
  @spec ensure_durability_to_version(db :: t(), Bedrock.version()) :: :ok
  def ensure_durability_to_version(db, version) do
    if Version.first?(version) do
      :ok
    else
      MVCC.transaction_at_version(db.mvcc, version)
      |> then(fn transaction ->
        :ok = PersistentKeyValues.apply_transaction(db.pkv, transaction)
        :ok = Keyspace.apply_transaction(db.keyspace, transaction)
        {:ok, _n_purged} = MVCC.purge_keys_older_than_version(db.mvcc, version)
        :ok
      end)
    end
  end
end
