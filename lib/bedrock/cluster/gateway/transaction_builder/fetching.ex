defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Fetching do
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  import Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions, only: [next_read_version: 1]

  @doc false
  @spec do_fetch(State.t(), key :: binary()) :: {State.t(), term()}
  def do_fetch(t, key) do
    {:ok, encoded_key} = t.key_codec.encode_key(key)

    with :error <- Map.fetch(t.writes, encoded_key),
         :error <- Map.fetch(t.reads, encoded_key),
         :error <- fetch_from_stack(encoded_key, t.stack),
         {:ok, t, encoded_value} <- fetch_from_storage(t, encoded_key),
         {:ok, value} <- t.value_codec.decode_value(encoded_value) do
      {%{t | reads: Map.put(t.reads, encoded_key, value)}, {:ok, value}}
    else
      {:ok, _value} = ok -> {t, ok}
      :error -> {%{t | reads: Map.put(t.reads, encoded_key, :error)}, :error}
      error -> {t, error}
    end
  end

  @spec fetch_from_stack(Bedrock.key(), [{reads :: map(), writes :: map()}]) ::
          :error | {State.t(), binary()}
  def fetch_from_stack(_, []), do: :error

  def fetch_from_stack(key, [{reads, writes} | stack]) do
    with :error <- Map.fetch(writes, key),
         :error <- Map.fetch(reads, key) do
      fetch_from_stack(key, stack)
    end
  end

  @spec fetch_from_storage(State.t(), key :: binary()) :: {:ok, State.t(), binary()} | :error
  def fetch_from_storage(%{read_version: nil} = t, key) do
    with {:ok, read_version, read_version_lease_expiration_in_ms} <- next_read_version(t) do
      read_version_lease_expiration =
        :erlang.monotonic_time(:millisecond) + read_version_lease_expiration_in_ms

      t
      |> Map.put(:read_version, read_version)
      |> Map.put(:read_version_lease_expiration, read_version_lease_expiration)
      |> fetch_from_storage(key)
    end
  end

  @spec fetch_from_storage(State.t(), key :: binary()) :: {:ok, State.t(), binary()} | :error
  def fetch_from_storage(t, key) do
    fastest_storage_server_for_key(t.fastest_storage_servers, key)
    |> case do
      nil ->
        storage_servers_for_key(t.storage_table, key)
        |> case do
          [] ->
            raise "No storage server or team found for key: #{inspect(key)}"

          storage_servers when is_list(storage_servers) ->
            fetch_from_fastest_storage_server(t, storage_servers, key)
        end

      storage_server ->
        Storage.fetch(storage_server, key, t.read_version, timeout: t.fetch_timeout_in_ms)
        |> case do
          {:ok, value} -> {:ok, t, value}
          error -> error
        end
    end
  end

  @spec fetch_from_fastest_storage_server(
          State.t(),
          storage_servers :: [{Bedrock.key_range(), pid()}],
          key :: binary()
        ) ::
          {:ok, State.t(), binary()} | :error
  def fetch_from_fastest_storage_server(t, storage_servers, key) do
    storage_servers
    |> horse_race_storage_servers_for_key(t.read_version, key, t.fetch_timeout_in_ms)
    |> case do
      {:ok, key_range, storage_server, value} ->
        {:ok, t |> Map.update!(:fastest_storage_servers, &Map.put(&1, key_range, storage_server)),
         value}

      error ->
        error
    end
  end

  @spec fastest_storage_server_for_key(%{Bedrock.key_range() => pid()}, key :: binary()) ::
          pid() | nil
  def fastest_storage_server_for_key(storage_servers, key) do
    Enum.find_value(storage_servers, fn
      {{min_key, max_key_exclusive}, storage_server}
      when min_key <= key and (key < max_key_exclusive or :end == max_key_exclusive) ->
        storage_server

      _ ->
        nil
    end)
  end

  @doc """
  Retrieves the set of storage servers responsible for a given key from the ETS
  table that is maintained by the Gateway.
  """
  @spec storage_servers_for_key(:ets.table(), key :: binary()) :: [{Bedrock.key_range(), pid()}]
  def storage_servers_for_key(storage_table, key) do
    :ets.select(storage_table, [
      {
        {{:"$1", :"$2"}, :"$3", :"$4", :"$5"},
        [
          {:and, {:"=<", :"$1", key}, {:or, {:<, key, :"$2"}, {:==, :end, :"$2"}}}
        ],
        [{{{{:"$1", :"$2"}}, :"$5"}}]
      }
    ])
  end

  @doc """
  Performs a "horse race" across multiple storage servers to fetch the value
  for a given key. All of the storage servers are queried in parallel, and the
  first successful response is returned. If none of the servers return a value
  within the specified timeout, `:error` is returned.
  """
  @spec horse_race_storage_servers_for_key(
          storage_servers :: [{Bedrock.key_range(), pid()}],
          read_version :: non_neg_integer(),
          key :: binary(),
          fetch_timeout_in_ms :: pos_integer()
        ) :: {:ok, Bedrock.key_range(), pid(), binary()} | :error
  def horse_race_storage_servers_for_key([], _, _, _), do: :error

  def horse_race_storage_servers_for_key(
        storage_servers,
        read_version,
        key,
        fetch_timeout_in_ms
      ) do
    storage_servers
    |> Task.async_stream(
      fn {key_range, storage_server} ->
        Storage.fetch(storage_server, key, read_version, timeout: fetch_timeout_in_ms)
        |> case do
          {:ok, value} -> {key_range, storage_server, value}
          error -> error
        end
      end,
      ordered: false,
      timeout: fetch_timeout_in_ms
    )
    |> Enum.find_value(fn
      {:ok, {:error, :version_too_old} = error} -> error
      {:ok, {:error, :not_found} = error} -> error
      {:ok, {key_range, storage_server, value}} -> {:ok, key_range, storage_server, value}
      _ -> nil
    end) || :error
  end
end
