defmodule Bedrock.DataPlane.Storage.Olivine.Logic do
  @moduledoc false

  import Bedrock.DataPlane.Storage.Olivine.State,
    only: [update_mode: 2, update_director_and_epoch: 3, reset_puller: 1]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.WaitingList
  alias Bedrock.Service.Worker

  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  def startup(otp_name, foreman, id, path) do
    with :ok <- ensure_directory_exists(path),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets")) do
      # Perform recovery from persistent storage
      version_manager = VersionManager.recover_from_database(database)

      {:ok,
       %State{
         path: path,
         otp_name: otp_name,
         id: id,
         foreman: foreman,
         database: database,
         version_manager: version_manager
       }}
    end
  end

  @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  @spec shutdown(State.t()) :: :ok
  def shutdown(%State{} = t) do
    :ok = Database.close(t.database)
    :ok = VersionManager.close(t.version_manager)
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

  def stop_pulling(%{pull_task: _puller} = t) do
    # Future implementation will gracefully stop the pulling task here.
    # This includes:
    # - Stopping the data stream from log shards
    # - Completing any in-flight transaction applications
    # - Cleaning up connection resources
    # For now, we just reset the puller reference.
    reset_puller(t)
  end

  @spec unlock_after_recovery(State.t(), Bedrock.version(), TransactionSystemLayout.t()) ::
          {:ok, State.t()}
  def unlock_after_recovery(t, durable_version, %{logs: _logs, services: _services}) do
    with :ok <- VersionManager.purge_transactions_newer_than(t.version_manager, durable_version) do
      _apply_and_notify_fn = fn encoded_transactions ->
        updated_vm = VersionManager.apply_transactions(t.version_manager, encoded_transactions)
        version = updated_vm.current_version
        send(self(), {:transactions_applied, version})
        version
      end

      # Future implementation will create a data pulling task here that syncs with the log.
      # The pulling mechanism will be responsible for:
      # - Connecting to the distributed log shards
      # - Streaming transaction data from the configured starting position
      # - Applying received transactions via the apply_and_notify callback
      # For now, we just transition to running mode without active pulling.

      t
      |> update_mode(:running)
      # |> put_puller(puller)
      |> then(&{:ok, &1})
    end
  end

  @spec fetch(State.t(), Bedrock.key(), Version.t()) ::
          {:error, :key_out_of_range | :not_found | :version_too_old} | {:ok, binary()}
  def fetch(%State{} = t, key, version), do: VersionManager.fetch(t.version_manager, key, version, t.database)

  @spec try_fetch_or_waitlist(State.t(), Bedrock.key(), Bedrock.version(), GenServer.from()) ::
          {:ok, Bedrock.value(), State.t()}
          | {:error, term(), State.t()}
          | {:waitlist, State.t()}
  def try_fetch_or_waitlist(%State{} = t, key, version, from) do
    current_version = VersionManager.last_committed_version(t.version_manager)
    durable_version = VersionManager.last_durable_version(t.version_manager)

    cond do
      # Check if version is too new - waitlist immediately
      current_version < version ->
        fetch_data = {key, version}
        reply_fn = fn result -> GenServer.reply(from, result) end
        timeout_ms = 30_000

        {new_waiting, _timeout} =
          WaitingList.insert(
            t.waiting_fetches,
            version,
            fetch_data,
            reply_fn,
            timeout_ms
          )

        {:waitlist, %{t | waiting_fetches: new_waiting}}

      # Check if version is too old - return error immediately
      version < durable_version ->
        {:error, :version_too_old, t}

      # Version is in valid range - do the actual lookup
      true ->
        case VersionManager.fetch(t.version_manager, key, version, t.database) do
          {:ok, value} ->
            {:ok, value, t}

          {:error, :version_too_old} ->
            {:error, :version_too_old, t}

          {:error, :not_found} ->
            {:error, :not_found, t}
        end
    end
  end

  @spec try_range_fetch_or_waitlist(State.t(), Bedrock.key(), Bedrock.key(), Bedrock.version(), GenServer.from()) ::
          {:ok, [{Bedrock.key(), Bedrock.value()}], State.t()}
          | {:error, term(), State.t()}
          | {:waitlist, State.t()}
  def try_range_fetch_or_waitlist(%State{} = t, start_key, end_key, version, from) do
    current_version = VersionManager.last_committed_version(t.version_manager)
    durable_version = VersionManager.last_durable_version(t.version_manager)

    cond do
      # Check if version is too new - waitlist immediately
      current_version < version ->
        range_fetch_data = {start_key, end_key, version}
        reply_fn = fn result -> GenServer.reply(from, result) end
        timeout_ms = 30_000

        {new_waiting, _timeout} =
          WaitingList.insert(
            t.waiting_fetches,
            version,
            range_fetch_data,
            reply_fn,
            timeout_ms
          )

        {:waitlist, %{t | waiting_fetches: new_waiting}}

      # Check if version is too old - return error immediately
      version < durable_version ->
        {:error, :version_too_old, t}

      # Version is in valid range - do the actual lookup
      true ->
        case VersionManager.range_fetch(t.version_manager, start_key, end_key, version, t.database) do
          {:ok, results} ->
            {:ok, results, t}

          {:error, :version_too_old} ->
            {:error, :version_too_old, t}

          {:error, :not_found} ->
            # Empty range returns empty list, not error
            {:ok, [], t}
        end
    end
  end

  @spec notify_waiting_fetches(State.t(), Bedrock.version()) :: State.t()
  def notify_waiting_fetches(%State{} = t, applied_version) do
    {new_waiting, notified_entries} = WaitingList.remove_all(t.waiting_fetches, applied_version)

    Enum.each(notified_entries, fn {_deadline, reply_fn, fetch_data} ->
      handle_fetch_request(t, fetch_data, reply_fn)
    end)

    %{t | waiting_fetches: new_waiting}
  end

  defp handle_fetch_request(t, fetch_data, reply_fn) do
    case fetch_data do
      {key, version} ->
        handle_single_key_fetch(t, key, version, reply_fn)

      {start_key, end_key, version} ->
        handle_range_fetch(t, start_key, end_key, version, reply_fn)
    end
  end

  defp handle_single_key_fetch(t, key, version, reply_fn) do
    case VersionManager.fetch(t.version_manager, key, version, t.database) do
      {:ok, value} -> reply_fn.({:ok, value})
      error -> reply_fn.(error)
    end
  end

  defp handle_range_fetch(t, start_key, end_key, version, reply_fn) do
    case VersionManager.range_fetch(t.version_manager, start_key, end_key, version, t.database) do
      {:ok, results} -> reply_fn.({:ok, results})
      # Empty range
      {:error, :not_found} -> reply_fn.({:ok, []})
      error -> reply_fn.(error)
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

  defp gather_info(:oldest_durable_version, t), do: VersionManager.oldest_durable_version(t.version_manager)
  defp gather_info(:durable_version, t), do: VersionManager.last_durable_version(t.version_manager)
  defp gather_info(:id, t), do: t.id
  defp gather_info(:key_ranges, t), do: VersionManager.info(t.version_manager, :key_ranges)
  defp gather_info(:kind, _t), do: :storage
  defp gather_info(:n_keys, t), do: VersionManager.info(t.version_manager, :n_keys)
  defp gather_info(:otp_name, t), do: t.otp_name
  defp gather_info(:path, t), do: t.path
  defp gather_info(:pid, _t), do: self()
  defp gather_info(:size_in_bytes, t), do: VersionManager.info(t.version_manager, :size_in_bytes)
  defp gather_info(:supported_info, _t), do: supported_info()
  defp gather_info(:utilization, t), do: VersionManager.info(t.version_manager, :utilization)
  defp gather_info(_unsupported, _t), do: {:error, :unsupported_info}
end
