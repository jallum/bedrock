defmodule Bedrock.DataPlane.Storage.Basalt.MultiVersionConcurrencyControl do
  @moduledoc """
  Multi-Version Concurrency Control (MVCC) is a concurrency control method that
  allows for multiple versions of a key to exist in the same table. This module
  provides an implementation of MVCC for Basalt.
  """

  alias Bedrock.DataPlane.Log.Transaction

  @opaque t :: :ets.table()

  @spec new(otp_name :: atom(), Bedrock.version()) :: t()
  def new(otp_name, version) when is_atom(otp_name) do
    with mvcc <-
           :ets.new(otp_name, [
             :ordered_set,
             :public,
             read_concurrency: true,
             write_concurrency: true
           ]),
         true <-
           :ets.insert(mvcc, [
             {:newest_version, version},
             {:oldest_version, version}
           ]) do
      mvcc
    end
  end

  @spec close(pkv :: t()) :: :ok
  def close(mvcc) do
    :ets.delete(mvcc)
    :ok
  end

  @doc """
  Apply a series of transactions to the given table and return the id of the
  last transaction applied. Transaction IDs must be ever-increasing. Though
  each transaction is applied atomically, it's possible that reads will be
  interleaved between them.

  If any of the transactions fails to apply, an exception will be raised.
  """
  @spec apply_transactions!(
          mvcc :: t(),
          transactions :: [Transaction.t()]
        ) ::
          Bedrock.version()
  def apply_transactions!(mvcc, transactions) do
    latest_version = mvcc |> newest_version()

    transactions
    |> Enum.reduce(latest_version, fn
      {version, _kv_pairs}, latest_version
      when version <= latest_version and latest_version != 0 ->
        raise "Transactions must be applied in order (new #{version}, old #{latest_version})"

      {version, _kv_pairs} = transaction, _latest_version ->
        :ok = apply_one_transaction!(mvcc, transaction)
        version
    end)
  end

  @doc """
  Apply a single transaction to the given table, atomically. Returns `:ok` if
  the transaction was applied successfully.
  """
  @spec apply_one_transaction!(mvcc :: t(), Transaction.t()) :: :ok
  def apply_one_transaction!(mvcc, {version, kv_pairs}) do
    :ets.insert(
      mvcc,
      [
        {:newest_version, version}
        | kv_pairs
          |> Enum.map(fn
            {key, value} -> {versioned_key(key, version), value}
          end)
      ]
    )

    :ok
  end

  @doc """
  Store a key/value pair in the table, at the given version. If the key already
  exists, then nothing will happen. This value will be returned by subsequent
  calls to lookup/3, but will _never_ be returned as part of a snapshot. This is
  useful for caching values that are (possibly expensive) to retrieve from
  permanent storage.

  No error checking is performed on the version.
  """
  @spec insert_read(mvcc :: t(), Bedrock.key(), Bedrock.version(), Bedrock.value() | nil) ::
          :ok
  def insert_read(mvcc, key, version, value) when is_binary(value) or is_nil(value) do
    :ets.insert_new(mvcc, {versioned_key(key, version), {value}})
    :ok
  end

  @doc """
  Lookup the value for the given key/version. The *exact* version, or the next-
  oldest will be returned. Values for the key newer than the given version will
  not be considered. If no suitable keys are found, then {:error, :not_found}
  will be returned.

  This is useful for providing a consistent view of the data at a given point
  in the transaction timeline.
  """
  @spec fetch(mvcc :: t(), Bedrock.key(), Bedrock.version()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  def fetch(mvcc, key, version) do
    mvcc
    |> :ets.select_reverse(match_value_for_key_with_version_lte(key, version), 1)
    |> case do
      {[match], _continuation} ->
        match
        |> value_from_ets_row()
        |> to_fetch_result()

      :"$end_of_table" ->
        {:error, :not_found}
    end
  end

  defp match_value_for_key_with_version_lte(key, version),
    do: [{{{:"$1", :"$2"}, :"$3"}, [{:"=:=", key, :"$1"}, {:"=<", :"$2", version}], [:"$3"]}]

  defp match_rows_with_with_version_gt(version),
    do: [{{{:_, :"$2"}, :_}, [{:>=, :"$2", version}], [true]}]

  defp value_from_ets_row({value}), do: value
  defp value_from_ets_row(value), do: value

  defp to_fetch_result(nil), do: {:error, :not_found}
  defp to_fetch_result(value), do: {:ok, value}

  @doc """
  Get the last transaction version performed on the table. If no transaction
  has been performed then nil is returned.
  """
  @spec newest_version(mvcc :: t()) :: Bedrock.version() | nil
  def newest_version(mvcc) do
    :ets.lookup(mvcc, :newest_version)
    |> case do
      [{_, version}] -> version
      [] -> nil
    end
  end

  @doc """
  Get the oldest possible transaction that can be read by the system. All
  transactions prior to this will have been coalesced.
  """
  @spec oldest_version(mvcc :: t()) :: Bedrock.version() | nil
  def oldest_version(mvcc) do
    :ets.lookup(mvcc, :oldest_version)
    |> case do
      [{_, version}] -> version
      [] -> nil
    end
  end

  @doc """
  Build a new transaction that encompasses only the latest writes for each key in
  the table, using the latest version as the cutoff. Since the latest
  transaction version is updated atomically alongside the transaction values,
  it's guaranteed that the generated transaction will include all writes that
  have been applied up until that point, and none that have been applied after.

  Returns a transaction tuple. If no transactions have been performed then nil
  is returned.
  """
  @spec transaction_at_version(
          mvcc :: t(),
          version ::
            :latest
            | Bedrock.version()
        ) :: Transaction.t() | nil
  def transaction_at_version(mvcc, :latest) do
    newest_version(mvcc)
    |> case do
      nil -> nil
      version -> transaction_at_version(mvcc, version)
    end
  end

  def transaction_at_version(mvcc, version) do
    {_, snapshot} =
      :ets.foldr(
        fn
          {{key, key_version}, value}, {last_key, kv}
          when not is_tuple(value) and key_version <= version and key != last_key ->
            {key, Map.put(kv, key, value)}

          _, acc ->
            acc
        end,
        {nil, %{}},
        mvcc
      )

    {version, snapshot}
  end

  @spec purge_keys_newer_than_version(mvcc :: t(), Bedrock.version()) :: :ok
  def purge_keys_newer_than_version(mvcc, version) do
    :ets.select_delete(mvcc, match_rows_with_with_version_gt(version))
    :ets.insert(mvcc, [{:newest_version, version}])
    :ok
  end

  @doc """
  Purge all keys/versions (and values) that are older than the given version.
  """
  @spec purge_keys_older_than_version(mvcc :: t(), Bedrock.version()) ::
          {:ok, n_purged :: pos_integer()}
  def purge_keys_older_than_version(mvcc, version) do
    :ets.insert(mvcc, [{:oldest_version, version}])
    n_purged = :ets.select_delete(mvcc, match_version_lt(version))
    {:ok, n_purged}
  end

  defp match_version_lt(version),
    do: [{{{:_, :"$1"}, :_}, [{:<, :"$1", version}], [true]}]

  defp versioned_key(key, version), do: {key, version}
end
