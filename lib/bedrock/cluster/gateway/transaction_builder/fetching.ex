defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Fetching do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Internal.Time

  import Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions, only: [next_read_version: 1]

  @type next_read_version_fn() :: (State.t() ->
                                     {:ok, Bedrock.version(), Bedrock.interval_in_ms()}
                                     | {:error, atom()})
  @type time_fn() :: (-> integer())
  @type storage_fetch_fn() :: (pid(), binary(), Bedrock.version(), keyword() ->
                                 {:ok, binary()} | {:error, atom()})
  @type async_stream_fn() :: (list(), function(), keyword() -> Enumerable.t())
  @type horse_race_fn() :: ([{Bedrock.key_range(), pid()}],
                            Bedrock.version(),
                            binary(),
                            pos_integer(),
                            keyword() ->
                              {:ok, Bedrock.key_range(), pid(), binary()}
                              | {:error, atom()}
                              | :error)

  @doc false
  @spec do_fetch(State.t(), key :: binary()) ::
          {State.t(),
           {:ok, Bedrock.value()}
           | :error
           | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :timeout}}
  @spec do_fetch(
          State.t(),
          key :: binary(),
          opts :: [
            next_read_version_fn: next_read_version_fn(),
            time_fn: time_fn(),
            storage_fetch_fn: storage_fetch_fn(),
            horse_race_fn: horse_race_fn()
          ]
        ) ::
          {State.t(),
           {:ok, Bedrock.value()}
           | :error
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def do_fetch(t, key, opts \\ []) do
    {:ok, encoded_key} = t.key_codec.encode_key(key)
    # Create a fetch function that matches Tx.get expectations: (key, state) -> {result, state}
    fetch_fn = fn k, state ->
      case fetch_from_storage(state, k, opts) do
        {:ok, new_state, value} -> {{:ok, value}, new_state}
        {:error, reason} -> {{:error, reason}, state}
        :error -> {:error, state}
      end
    end

    {tx, result, t} = Tx.get(t.tx, encoded_key, fetch_fn, t)
    {%{t | tx: tx}, result}
  end

  @spec fetch_from_storage(
          State.t(),
          key :: binary(),
          opts :: [
            next_read_version_fn: next_read_version_fn(),
            time_fn: time_fn(),
            storage_fetch_fn: storage_fetch_fn()
          ]
        ) ::
          {:ok, State.t(), binary()}
          | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :timeout}
          | :error
  def fetch_from_storage(%{read_version: nil} = t, key, opts) do
    next_read_version_fn = Keyword.get(opts, :next_read_version_fn, &next_read_version/1)
    time_fn = Keyword.get(opts, :time_fn, &Time.monotonic_now_in_ms/0)

    case next_read_version_fn.(t) do
      {:ok, read_version, read_version_lease_expiration_in_ms} ->
        read_version_lease_expiration =
          time_fn.() + read_version_lease_expiration_in_ms

        t
        |> Map.put(:read_version, read_version)
        |> Map.put(:read_version_lease_expiration, read_version_lease_expiration)
        |> fetch_from_storage(key, opts)

      {:error, :unavailable} ->
        raise "No read version available for fetching key: #{inspect(key)}"
    end
  end

  def fetch_from_storage(t, key, opts) do
    storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)

    fastest_storage_server_for_key(t.fastest_storage_servers, key)
    |> case do
      nil ->
        storage_servers_for_key(t.transaction_system_layout, key)
        |> case do
          [] ->
            raise "No storage server or team found for key: #{inspect(key)}"

          storage_servers when is_list(storage_servers) ->
            fetch_from_fastest_storage_server(t, storage_servers, key, opts)
        end

      storage_server ->
        storage_fetch_fn.(storage_server, key, t.read_version, timeout: t.fetch_timeout_in_ms)
        |> case do
          {:ok, value} -> {:ok, t, value}
          error -> error
        end
    end
  end

  @spec fetch_from_fastest_storage_server(
          State.t(),
          storage_servers :: [{Bedrock.key_range(), pid()}],
          key :: binary(),
          opts :: [
            horse_race_fn: horse_race_fn()
          ]
        ) ::
          {:ok, State.t(), binary()}
          | {:error, atom()}
          | :error
  def fetch_from_fastest_storage_server(t, storage_servers, key, opts) do
    horse_race_fn = Keyword.get(opts, :horse_race_fn, &horse_race_storage_servers_for_key/5)

    storage_servers
    |> horse_race_fn.(t.read_version, key, t.fetch_timeout_in_ms, opts)
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
  Retrieves the set of storage servers responsible for a given key from the
  Transaction System Layout.
  """
  @spec storage_servers_for_key(
          Bedrock.ControlPlane.Config.TransactionSystemLayout.t(),
          key :: binary()
        ) ::
          [{Bedrock.key_range(), pid()}]
  def storage_servers_for_key(transaction_system_layout, key) do
    transaction_system_layout.storage_teams
    |> Enum.filter(fn %{key_range: {min_key, max_key_exclusive}} ->
      min_key <= key and (key < max_key_exclusive or max_key_exclusive == :end)
    end)
    |> Enum.flat_map(fn %{key_range: key_range, storage_ids: storage_ids} ->
      storage_ids
      |> Enum.map(fn storage_id ->
        storage_server = get_storage_server_pid(transaction_system_layout, storage_id)
        {key_range, storage_server}
      end)
      |> Enum.filter(fn {_key_range, pid} -> not is_nil(pid) end)
    end)
  end

  @spec get_storage_server_pid(
          Bedrock.ControlPlane.Config.TransactionSystemLayout.t(),
          String.t()
        ) :: pid() | nil
  defp get_storage_server_pid(transaction_system_layout, storage_id) do
    transaction_system_layout.services
    |> Map.get(storage_id)
    |> case do
      %{kind: :storage, status: {:up, pid}} -> pid
      _ -> nil
    end
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
          fetch_timeout_in_ms :: pos_integer(),
          opts :: [
            async_stream_fn: async_stream_fn(),
            storage_fetch_fn: storage_fetch_fn()
          ]
        ) :: {:ok, Bedrock.key_range(), pid(), binary()} | {:error, atom()} | :error
  def horse_race_storage_servers_for_key([], _, _, _, _), do: :error

  def horse_race_storage_servers_for_key(
        storage_servers,
        read_version,
        key,
        fetch_timeout_in_ms,
        opts
      ) do
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)

    storage_servers
    |> async_stream_fn.(
      fn {key_range, storage_server} ->
        storage_fetch_fn.(storage_server, key, read_version, timeout: fetch_timeout_in_ms)
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
