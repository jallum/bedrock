defmodule Bedrock.DataPlane.Storage.Olivine.Logic do
  @moduledoc false

  import Bedrock.DataPlane.Storage.Olivine.State,
    only: [update_mode: 2, update_director_and_epoch: 3, reset_puller: 1, put_puller: 2]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Pulling
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Olivine.Telemetry, as: OlivineTelemetry
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
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

  defp gather_info(:oldest_durable_version, t), do: Database.durable_version(t.database)
  defp gather_info(:durable_version, t), do: Database.durable_version(t.database)
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

  defp max_eviction_size, do: 10 * 1024 * 1024

  @doc """
  Performs window advancement by delegating policy decisions to IndexManager and handling persistence.
  IndexManager determines what to evict based on buffer tracking and hot set management.
  Logic handles database persistence and telemetry for the eviction.
  """
  @spec advance_window(State.t()) :: {:ok, State.t()} | {:error, term()}
  def advance_window(%State{} = state) do
    start_time = System.monotonic_time(:microsecond)

    case IndexManager.advance_window(state.index_manager, max_eviction_size()) do
      {:no_eviction, updated_index_manager} ->
        updated_state = %{state | index_manager: updated_index_manager}
        {:ok, updated_state}

      {:evict, evicted_count, updated_index_manager, collected_pages, eviction_version} ->
        window_edge = calculate_window_edge_for_telemetry(eviction_version, state.window_lag_time_μs)
        {data_db, _index_db} = state.database

        current_durable_version = Database.durable_version(state.database)

        {:ok, updated_database, db_pipeline} =
          Database.advance_durable_version(
            state.database,
            eviction_version,
            current_durable_version,
            data_db.file_offset,
            collected_pages
          )

        updated_state = %{state | index_manager: updated_index_manager, database: updated_database}

        duration = System.monotonic_time(:microsecond) - start_time
        lag_time_μs = calculate_lag_time_μs(window_edge, eviction_version)

        OlivineTelemetry.trace_window_advanced(:evicted, eviction_version,
          duration_μs: duration,
          evicted_count: evicted_count,
          lag_time_μs: lag_time_μs,
          window_target_version: window_edge,
          data_size_in_bytes: data_db.file_offset,
          durable_version_duration_μs: db_pipeline.total_duration_μs,
          db_insert_time_μs: db_pipeline.insert_time_μs,
          db_write_time_μs: db_pipeline.write_time_μs
        )

        {:ok, updated_state}
    end
  end

  # Helper to calculate window edge for telemetry purposes.
  # Uses eviction version directly instead of extracting from batch for efficiency.
  defp calculate_window_edge_for_telemetry(eviction_version, window_lag_time_μs) do
    Version.subtract(eviction_version, window_lag_time_μs)
  rescue
    ArgumentError ->
      # Underflow - return zero version
      Version.zero()
  end

  defp calculate_lag_time_μs(window_edge_version, eviction_version) do
    max(0, Version.distance(window_edge_version, eviction_version))
  rescue
    _ -> 0
  end

  @doc """
  Apply a batch of transactions to the storage state.
  This is used for incremental processing to avoid large DETS writes.
  Buffer tracking is now handled directly by IndexManager.apply_transactions.
  """
  @spec apply_transactions(State.t(), [binary()]) :: {:ok, State.t(), Bedrock.version()}
  def apply_transactions(%State{} = t, encoded_transactions) do
    batch_size = length(encoded_transactions)
    batch_size_bytes = Enum.sum(Enum.map(encoded_transactions, &byte_size/1))
    start_time = System.monotonic_time(:microsecond)

    {updated_index_manager, updated_database} =
      IndexManager.apply_transactions(t.index_manager, encoded_transactions, t.database)

    version = updated_index_manager.current_version

    duration = System.monotonic_time(:microsecond) - start_time
    Telemetry.trace_transaction_processing_complete(batch_size, duration, batch_size_bytes)

    # Update state with both updated index manager and database
    updated_state = %{t | index_manager: updated_index_manager, database: updated_database}
    {:ok, updated_state, version}
  end
end
