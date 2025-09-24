defmodule Bedrock.DataPlane.Storage.Olivine.Logic do
  @moduledoc false

  import Bedrock.DataPlane.Storage.Olivine.State,
    only: [update_mode: 2, update_director_and_epoch: 3, reset_puller: 1, put_puller: 2]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Pulling
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.WaitingList
  alias Bedrock.KeySelector
  alias Bedrock.Service.Worker

  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t(), db_opts :: keyword()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  def startup(otp_name, foreman, id, path, db_opts \\ []) do
    with :ok <- ensure_directory_exists(path),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets"), db_opts),
         {:ok, index_manager} <- IndexManager.recover_from_database(database) do
      {:ok,
       %State{
         path: path,
         otp_name: otp_name,
         id: id,
         foreman: foreman,
         database: database,
         index_manager: index_manager
       }}
    end
  end

  @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  @spec shutdown(State.t()) :: :ok
  def shutdown(%State{} = t) do
    stop_pulling(t)
    notify_waitlist_shutdown(t.waiting_fetches)
    :ok = Database.close(t.database)
  end

  @spec lock_for_recovery(State.t(), Director.ref(), Bedrock.epoch()) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists | String.t()}
  def lock_for_recovery(t, _, epoch) when not is_nil(t.epoch) and epoch < t.epoch, do: {:error, :newer_epoch_exists}

  def lock_for_recovery(t, director, epoch) do
    t
    |> update_mode(:locked)
    |> update_director_and_epoch(director, epoch)
    |> stop_pulling()
    |> then(&{:ok, &1})
  end

  @spec stop_pulling(State.t()) :: State.t()
  def stop_pulling(%{pull_task: nil} = t), do: t

  def stop_pulling(%{pull_task: puller} = t) do
    Pulling.stop(puller)
    reset_puller(t)
  end

  @spec unlock_after_recovery(State.t(), Bedrock.version(), TransactionSystemLayout.t()) ::
          {:ok, State.t()}
  def unlock_after_recovery(t, durable_version, %{logs: logs, services: services}) do
    t = stop_pulling(t)
    main_process_pid = self()

    apply_and_notify_fn = fn transactions ->
      send(main_process_pid, {:apply_transactions, transactions})
      last_transaction = List.last(transactions)
      Transaction.commit_version!(last_transaction)
    end

    database = t.database

    puller =
      Pulling.start_pulling(
        durable_version,
        t.id,
        logs,
        services,
        apply_and_notify_fn,
        fn -> Database.load_current_durable_version(database) end
      )

    t
    |> update_mode(:running)
    |> put_puller(puller)
    |> then(&{:ok, &1})
  end

  @doc """
  Fetch function with optional async callback support.

  When reply_fn is provided in opts, returns :ok immediately and delivers results via callback.
  When reply_fn is not provided, returns results synchronously (primarily for testing).
  """
  def get(state, key_or_selector, version, opts \\ [])

  @spec get(State.t(), Bedrock.key(), Bedrock.version(), opts :: [reply_fn: function()]) ::
          {:ok, binary()}
          | {:ok, pid()}
          | {:error, :not_found}
          | {:error, :version_too_old}
          | {:error, :version_too_new}
  def get(%State{} = t, key, version, opts) when is_binary(key) do
    t.index_manager
    |> IndexManager.page_for_key(key, version)
    |> case do
      {:ok, page} ->
        load_fn = Database.value_loader(t.database)

        do_now_or_async_with_reply(opts[:reply_fn], fn ->
          fetch_from_page(page, key, load_fn)
        end)

      error ->
        error
    end
  end

  @spec get(State.t(), KeySelector.t(), Bedrock.version(), opts :: [reply_fn: function()]) ::
          {:ok, {resolved_key :: binary(), value :: binary()}}
          | {:ok, pid()}
          | {:error, :not_found}
          | {:error, :version_too_old}
          | {:error, :version_too_new}
  def get(%State{} = t, %KeySelector{} = key_selector, version, opts) do
    case IndexManager.page_for_key(t.index_manager, key_selector, version) do
      {:ok, resolved_key, page} ->
        load_fn = Database.value_loader(t.database)
        fetch_task = fn -> fetch_resolved_key_selector(page, resolved_key, load_fn) end
        do_now_or_async_with_reply(opts[:reply_fn], fetch_task)

      error ->
        error
    end
  end

  defp fetch_resolved_key_selector(page, resolved_key, load_fn) do
    case fetch_from_page(page, resolved_key, load_fn) do
      {:ok, value} -> {:ok, {resolved_key, value}}
      error -> error
    end
  end

  defp fetch_from_page(page, key, load_fn) do
    case Page.locator_for_key(page, key) do
      {:ok, locator} -> load_fn.(locator)
      error -> error
    end
  end

  @doc """
  Range fetch function with optional async callback support.

  When reply_fn is provided in opts, returns :ok immediately and delivers results via callback.
  When reply_fn is not provided, returns results synchronously (primarily for testing).
  """
  def get_range(t, start_key_or_selector, end_key_or_selector, version, opts \\ [])

  @spec get_range(
          State.t(),
          Bedrock.key(),
          Bedrock.key(),
          Bedrock.version(),
          opts :: [reply_fn: function(), limit: pos_integer()]
        ) ::
          {:ok, {[{Bedrock.key(), Bedrock.value()}], more :: boolean()}}
          | {:ok, pid()}
          | {:error, :version_too_old}
          | {:error, :version_too_new}
  def get_range(t, start_key, end_key, version, opts) when is_binary(start_key) and is_binary(end_key) do
    t.index_manager
    |> IndexManager.pages_for_range(start_key, end_key, version)
    |> case do
      {:ok, pages} ->
        load_many_fn = Database.many_value_loader(t.database)
        limit = opts[:limit]

        do_now_or_async_with_reply(opts[:reply_fn], fn ->
          range_fetch_from_pages(pages, start_key, end_key, limit, load_many_fn)
        end)

      error ->
        error
    end
  end

  @spec get_range(
          State.t(),
          KeySelector.t(),
          KeySelector.t(),
          Bedrock.version(),
          opts :: [reply_fn: function(), limit: pos_integer()]
        ) ::
          {:ok, {[{Bedrock.key(), Bedrock.value()}], more :: boolean()}}
          | {:ok, pid()}
          | {:error, :version_too_old}
          | {:error, :version_too_new}
          | {:error, :not_found}
          | {:error, :invalid_range}
  def get_range(t, %KeySelector{} = start_selector, %KeySelector{} = end_selector, version, opts) do
    case IndexManager.pages_for_range(t.index_manager, start_selector, end_selector, version) do
      {:ok, {resolved_start_key, resolved_end_key}, pages} ->
        load_many_fn = Database.many_value_loader(t.database)
        limit = opts[:limit]

        do_now_or_async_with_reply(opts[:reply_fn], fn ->
          range_fetch_from_pages(pages, resolved_start_key, resolved_end_key, limit, load_many_fn)
        end)

      error ->
        error
    end
  end

  defp range_fetch_from_pages(pages, start_key, end_key, limit, load_many_fn) do
    {keys_and_locators, has_more} =
      pages
      |> Page.stream_key_locators_in_range(start_key, end_key)
      |> Stream.with_index()
      |> Enum.reduce_while({[], false}, fn
        {key_and_locator, index}, {acc, _} when index < limit ->
          {:cont, {[key_and_locator | acc], false}}

        _, {acc, _} ->
          {:halt, {acc, true}}
      end)

    {:ok, values_by_locator} =
      keys_and_locators
      |> Enum.map(fn {_key, locator} -> locator end)
      |> load_many_fn.()

    results =
      keys_and_locators
      |> Enum.reverse()
      |> Enum.map(fn {key, locator} -> {key, Map.get(values_by_locator, locator)} end)

    {:ok, {results, has_more}}
  end

  @doc """
  Adds a fetch request to the waitlist.

  Used by the Server when a version_too_new error occurs and wait_ms is specified.
  """
  @spec add_to_waitlist(State.t(), term(), Bedrock.version(), function(), pos_integer()) :: State.t()
  def add_to_waitlist(%State{} = t, fetch_request, version, reply_fn, timeout_in_ms) do
    {new_waiting, _timeout} = WaitingList.insert(t.waiting_fetches, version, fetch_request, reply_fn, timeout_in_ms)
    %{t | waiting_fetches: new_waiting}
  end

  @spec notify_waiting_fetches(State.t(), Bedrock.version()) :: {State.t(), [pid()]}
  def notify_waiting_fetches(%State{} = t, applied_version) do
    {new_waiting, fetches_to_run} = WaitingList.remove_all(t.waiting_fetches, applied_version)

    {updated_state, spawned_pids} =
      Enum.reduce(fetches_to_run, {%{t | waiting_fetches: new_waiting}, []}, fn
        {_deadline, reply_fn, fetch_request}, {acc_state, pids_acc} ->
          case run_fetch_and_notify(acc_state, reply_fn, fetch_request) do
            {:ok, pid} -> {acc_state, [pid | pids_acc]}
            :ok -> {acc_state, pids_acc}
          end
      end)

    {updated_state, spawned_pids}
  end

  defp run_fetch_and_notify(t, reply_fn, {key, version}) when is_binary(key) do
    case get(t, key, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  defp run_fetch_and_notify(t, reply_fn, {start_key, end_key, version})
       when is_binary(start_key) and is_binary(end_key) do
    case get_range(t, start_key, end_key, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  defp run_fetch_and_notify(t, reply_fn, {%KeySelector{} = key_selector, version}) do
    case get(t, key_selector, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  defp run_fetch_and_notify(t, reply_fn, {%KeySelector{} = start_selector, %KeySelector{} = end_selector, version}) do
    case get_range(t, start_selector, end_selector, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  @spec info(State.t(), Storage.fact_name() | [Storage.fact_name()]) ::
          {:ok, term() | %{Storage.fact_name() => term()}} | {:error, :unsupported_info}
  def info(%State{} = t, fact_name) when is_atom(fact_name), do: {:ok, gather_info(fact_name, t)}

  def info(%State{} = t, fact_names) when is_list(fact_names) do
    {:ok,
     fact_names
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
     end)
     |> Map.new()}
  end

  defp supported_info, do: ~w[
      durable_version
      oldest_durable_version
      id
      pid
      path
      key_ranges
      kind
      n_keys
      otp_name
      size_in_bytes
      supported_info
      utilization
    ]a

  defp gather_info(:oldest_durable_version, t) do
    {:ok, version} = Database.load_durable_version(t.database)
    version
  end

  defp gather_info(:durable_version, t) do
    {:ok, version} = Database.load_durable_version(t.database)
    version
  end

  defp gather_info(:id, t), do: t.id
  defp gather_info(:key_ranges, t), do: IndexManager.info(t.index_manager, :key_ranges)
  defp gather_info(:kind, _t), do: :storage
  defp gather_info(:n_keys, t), do: IndexManager.info(t.index_manager, :n_keys)
  defp gather_info(:otp_name, t), do: t.otp_name
  defp gather_info(:path, t), do: t.path
  defp gather_info(:pid, _t), do: self()
  defp gather_info(:size_in_bytes, t), do: IndexManager.info(t.index_manager, :size_in_bytes)
  defp gather_info(:supported_info, _t), do: supported_info()
  defp gather_info(:utilization, t), do: IndexManager.info(t.index_manager, :utilization)
  defp gather_info(_unsupported, _t), do: {:error, :unsupported_info}

  # Window advancement constants
  # 5 seconds
  @window_lag_microseconds 5_000_000

  defp max_eviction_size, do: 1 * 1024 * 1024

  @doc """
  Performs size-controlled window advancement based on buffer tracking queue.
  Uses two-queue architecture:
  1. Calculate window edge from newest version in buffer
  2. Take eviction batch (up to max_eviction_size or window edge) from buffer tracking queue
  3. Advance window to exact eviction point
  """
  @spec advance_window(State.t()) :: {:ok, State.t()} | {:error, term()}
  def advance_window(%State{} = state) do
    case Database.get_newest_version_in_buffer(state.database) do
      nil ->
        # No data in buffer, nothing to evict
        {:ok, state}

      newest_version ->
        # Calculate window edge (5 seconds ago from newest version)
        window_edge_version = calculate_window_edge(newest_version)

        # Take eviction batch limited by size and window edge
        {eviction_batch, updated_database} =
          Database.determine_eviction_batch(state.database, max_eviction_size(), window_edge_version)

        state_after_eviction_queue = %{state | database: updated_database}

        case eviction_batch do
          [] ->
            # No data to evict
            {:ok, state_after_eviction_queue}

          batch ->
            # Get the latest version that should be evicted
            {eviction_version, high_water_mark, _} = List.last(batch)

            # Calculate lag: how far behind we are from ideal window edge
            lag_microseconds = calculate_lag_microseconds(window_edge_version, eviction_version)

            # Trace eviction details
            Telemetry.trace_window_advancement_evicting(
              state.id,
              eviction_version,
              length(batch),
              window_edge_version,
              lag_microseconds
            )

            # Advance window to exact eviction point
            advance_window_to_eviction_point(state_after_eviction_queue, eviction_version, high_water_mark, state.id)
        end
    end
  end

  defp advance_window_to_eviction_point(state, eviction_version, high_water_mark, state_id) do
    case IndexManager.prepare_window_advancement(state.index_manager, eviction_version) do
      :no_eviction ->
        Telemetry.trace_window_advancement_no_eviction(state.id)
        {:ok, state}

      {:evict, new_durable_version, evicted_versions, index_manager} ->
        with {:ok, database} <-
               Database.advance_durable_version(
                 state.database,
                 new_durable_version,
                 evicted_versions,
                 high_water_mark
               ) do
          final_state = %{state | index_manager: index_manager, database: database}
          Telemetry.trace_window_advancement_complete(state_id, eviction_version)
          {:ok, final_state}
        end
    end
  end

  defp calculate_window_edge(newest_version) do
    # Subtract window lag time from newest version
    Version.subtract(newest_version, @window_lag_microseconds)
  rescue
    ArgumentError ->
      # Underflow - return zero version
      Version.zero()
  end

  defp calculate_lag_microseconds(window_edge_version, eviction_version) do
    # Convert versions to integers for arithmetic (they're 8-byte big-endian timestamps)
    <<window_edge_int::unsigned-big-64>> = window_edge_version
    <<eviction_int::unsigned-big-64>> = eviction_version

    # Lag is how far behind the eviction point is from the ideal window edge
    max(0, window_edge_int - eviction_int)
  rescue
    _ -> 0
  end

  defp notify_waitlist_shutdown(waiting_fetches) do
    waiting_fetches
    |> Enum.flat_map(fn {_version, entries} -> entries end)
    |> Enum.each(fn {_deadline, reply_fn, _fetch_request} ->
      reply_fn.({:error, :shutting_down})
    end)
  end

  @doc """
  Apply a batch of transactions to the storage state.
  This is used for incremental processing to avoid large DETS writes.
  Buffer tracking is now handled directly by IndexManager.apply_transactions.
  """
  @spec apply_transaction_batch(State.t(), [binary()]) :: {:ok, State.t(), Bedrock.version()}
  def apply_transaction_batch(%State{} = t, encoded_transactions) do
    # Apply transactions and get updated index manager and database (with buffer tracking)
    {updated_index_manager, updated_database} =
      IndexManager.apply_transactions(t.index_manager, encoded_transactions, t.database)

    version = updated_index_manager.current_version

    # Update state with both updated index manager and database
    updated_state = %{t | index_manager: updated_index_manager, database: updated_database}
    {:ok, updated_state, version}
  end

  #

  defp do_now_or_async_with_reply(nil, fun), do: fun.()
  defp do_now_or_async_with_reply(reply_fn, fun), do: Task.start(fn -> reply_fn.(fun.()) end)
end
