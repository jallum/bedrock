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
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Page
  alias Bedrock.Internal.WaitingList
  alias Bedrock.Service.Worker

  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  def startup(otp_name, foreman, id, path) do
    with :ok <- ensure_directory_exists(path),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets")),
         {:ok, version_manager} <- VersionManager.recover_from_database(database) do
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

      t
      |> update_mode(:running)
      # |> put_puller(puller)
      |> then(&{:ok, &1})
    end
  end

  @doc """
  Fetch function with optional async callback support.

  When reply_fn is provided in opts, returns :ok immediately and delivers results via callback.
  When reply_fn is not provided, returns results synchronously (primarily for testing).
  """
  @spec fetch(State.t(), Bedrock.key(), Bedrock.version(), opts :: [reply_fn: function()]) ::
          {:ok, binary()} | :ok | {:error, :not_found} | {:error, :version_too_old} | {:error, :version_too_new}
  def fetch(%State{} = t, key, version, opts \\ []) do
    case VersionManager.fetch_page_for_key(t.version_manager, key, version) do
      {:ok, page} ->
        resolve_fetch_from_page(t, page, key, opts[:reply_fn])

      error ->
        error
    end
  end

  @doc """
  Range fetch function with optional async callback support.

  When reply_fn is provided in opts, returns :ok immediately and delivers results via callback.
  When reply_fn is not provided, returns results synchronously (primarily for testing).
  """
  @spec range_fetch(
          State.t(),
          Bedrock.key(),
          Bedrock.key(),
          Bedrock.version(),
          opts :: [reply_fn: function(), limit: pos_integer()]
        ) ::
          {:ok, [{Bedrock.key(), Bedrock.value()}]} | :ok | {:error, :version_too_old} | {:error, :version_too_new}
  def range_fetch(t, start_key, end_key, version, opts \\ []) do
    case VersionManager.fetch_pages_for_range(t.version_manager, start_key, end_key, version) do
      {:ok, pages} ->
        resolve_range_from_pages(t, pages, start_key, end_key, opts)

      error ->
        error
    end
  end

  defp apply_limit(stream, nil), do: stream
  defp apply_limit(stream, limit) when is_integer(limit) and limit > 0, do: Stream.take(stream, limit)

  @doc """
  Adds a fetch request to the waitlist.

  Used by the Server when a version_too_new error occurs and wait_ms is specified.
  """
  @spec add_to_waitlist(State.t(), term(), Bedrock.version(), function(), pos_integer()) :: State.t()
  def add_to_waitlist(%State{} = t, fetch_request, version, reply_fn, timeout_in_ms) do
    {new_waiting, _timeout} =
      WaitingList.insert(
        t.waiting_fetches,
        version,
        fetch_request,
        reply_fn,
        timeout_in_ms
      )

    %{t | waiting_fetches: new_waiting}
  end

  defp minimal_load_fn(t) do
    vm_loader = VersionManager.value_loader(t.version_manager)
    db_loader = Database.value_loader(t.database)

    fn key, version ->
      case vm_loader.(key, version) do
        {:ok, value} ->
          value

        {:error, :check_database} ->
          {:ok, value} = db_loader.(key)
          value

        {:error, :not_found} ->
          raise "Key not found"
      end
    end
  end

  defp resolve_fetch_from_page(t, page, key, reply_fn) do
    load_fn = minimal_load_fn(t)

    if reply_fn do
      Task.start(fn ->
        page
        |> resolve_fetch_from_page_with_loader(key, load_fn)
        |> reply_fn.()
      end)

      :ok
    else
      resolve_fetch_from_page_with_loader(page, key, load_fn)
    end
  end

  defp resolve_fetch_from_page_with_loader(page, key, load_fn) do
    case Page.find_version_for_key(page, key) do
      {:ok, found_version} ->
        result = load_fn.(key, found_version)
        {:ok, result}

      error ->
        error
    end
  end

  defp resolve_range_from_pages(t, pages, start_key, end_key, opts) do
    load_fn = minimal_load_fn(t)

    if reply_fn = opts[:reply_fn] do
      Task.start(fn ->
        pages
        |> resolve_range_from_pages_with_loader(start_key, end_key, opts[:limit], load_fn)
        |> reply_fn.()
      end)

      :ok
    else
      resolve_range_from_pages_with_loader(pages, start_key, end_key, opts[:limit], load_fn)
    end
  end

  defp resolve_range_from_pages_with_loader(pages, start_key, end_key, limit, load_fn) do
    key_value_pairs =
      pages
      |> Page.stream_key_versions_in_range(start_key, end_key)
      |> apply_limit(limit)
      |> Enum.map(fn {key, version} ->
        value = load_fn.(key, version)
        {key, value}
      end)

    {:ok, key_value_pairs}
  end

  @spec notify_waiting_fetches(State.t(), Bedrock.version()) :: State.t()
  def notify_waiting_fetches(%State{} = t, applied_version) do
    {new_waiting, fetches_to_run} = WaitingList.remove_all(t.waiting_fetches, applied_version)

    Enum.each(fetches_to_run, fn
      {_deadline, reply_fn, fetch_request} ->
        run_fetch_and_notify(t, reply_fn, fetch_request)
    end)

    %{t | waiting_fetches: new_waiting}
  end

  defp run_fetch_and_notify(t, reply_fn, {key, version}) do
    case fetch(t, key, version, reply_fn: reply_fn) do
      :ok -> :ok
      error -> reply_fn.(error)
    end
  end

  defp run_fetch_and_notify(t, reply_fn, {start_key, end_key, version}) do
    case range_fetch(t, start_key, end_key, version, reply_fn: reply_fn) do
      :ok -> :ok
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
