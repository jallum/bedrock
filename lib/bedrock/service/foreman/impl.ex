defmodule Bedrock.Service.Foreman.Impl do
  @moduledoc false
  import Bedrock.Service.Foreman.Health,
    only: [compute_health_from_worker_info: 1]

  import Bedrock.Service.Foreman.StartingWorkers,
    only: [
      worker_info_from_path: 2,
      try_to_start_workers: 2,
      try_to_start_worker: 2,
      initialize_new_worker: 5
    ]

  import Bedrock.Service.Foreman.State

  alias Bedrock.Cluster
  alias Bedrock.Cluster.Gateway
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Worker

  @spec do_fetch_workers(State.t()) :: [Worker.ref()]
  def do_fetch_workers(t), do: otp_names_for_running_workers(t)

  @spec do_fetch_storage_workers(State.t()) :: [Worker.ref()]
  def do_fetch_storage_workers(t), do: otp_names_for_running_storage_workers(t)

  @spec do_get_all_running_services(State.t()) :: [{:log | :storage, atom()}]
  def do_get_all_running_services(t) do
    t.workers
    |> Enum.filter(fn {_id, worker_info} ->
      worker_healthy?(worker_info) and worker_info.manifest != nil
    end)
    |> Enum.map(fn {_id, worker_info} -> compact_service_info_from_worker_info(worker_info) end)
  end

  @spec do_new_worker(State.t(), Worker.id(), :log | :storage) :: {State.t(), Worker.ref()}
  def do_new_worker(t, id, kind) do
    worker_info =
      id
      |> initialize_new_worker(worker_for_kind(kind), %{}, t.path, t.cluster)
      |> try_to_start_worker(t.cluster)
      |> advertise_running_worker(t.cluster)

    t = update_workers(t, &Map.put(&1, id, worker_info))

    {t, worker_info.otp_name}
  end

  @spec do_remove_worker(State.t(), Worker.id()) ::
          {State.t(),
           :ok
           | {:error, :worker_not_found | {:failed_to_remove_directory, File.posix(), Path.t()}}}
  def do_remove_worker(t, worker_id) do
    case Map.get(t.workers, worker_id) do
      nil ->
        {t, {:error, :worker_not_found}}

      worker_info ->
        result = remove_worker_completely(worker_info, t.cluster, t.path)

        t =
          t
          |> update_workers(&Map.delete(&1, worker_id))
          |> recompute_health()

        {t, result}
    end
  end

  @spec do_remove_workers(State.t(), [Worker.id()]) ::
          {State.t(),
           %{
             Worker.id() =>
               :ok
               | {:error, :worker_not_found | {:failed_to_remove_directory, File.posix(), Path.t()}}
           }}
  def do_remove_workers(t, worker_ids) do
    {updated_state, results} = process_worker_removals(t, worker_ids)
    final_state = recompute_health(updated_state)
    {final_state, results}
  end

  defp process_worker_removals(initial_state, worker_ids) do
    Enum.reduce(worker_ids, {initial_state, %{}}, &remove_single_worker/2)
  end

  defp remove_single_worker(worker_id, {state, acc_results}) do
    case Map.get(state.workers, worker_id) do
      nil ->
        {state, Map.put(acc_results, worker_id, {:error, :worker_not_found})}

      worker_info ->
        result = remove_worker_completely(worker_info, state.cluster, state.path)
        updated_state = remove_worker_from_state(state, worker_id)
        {updated_state, Map.put(acc_results, worker_id, result)}
    end
  end

  defp remove_worker_from_state(state, worker_id) do
    update_workers(state, &Map.delete(&1, worker_id))
  end

  @spec advertise_running_workers([WorkerInfo.t()], Cluster.t()) :: [WorkerInfo.t()]
  def advertise_running_workers(worker_infos, cluster) do
    Enum.each(worker_infos, &advertise_running_worker(&1, cluster))
    worker_infos
  end

  @spec advertise_running_worker(WorkerInfo.t(), module()) :: WorkerInfo.t()
  def advertise_running_worker(%{health: {:ok, pid}} = t, cluster) do
    Gateway.advertise_worker(cluster.otp_name(:gateway), pid)
    t
  end

  @spec advertise_running_worker(WorkerInfo.t(), module()) :: WorkerInfo.t()
  def advertise_running_worker(t, _), do: t

  @spec remove_worker_completely(WorkerInfo.t(), module(), String.t()) ::
          :ok | {:error, {:failed_to_remove_directory, File.posix(), Path.t()}}
  defp remove_worker_completely(worker_info, cluster, base_path) do
    with :ok <- terminate_worker_process(worker_info, cluster),
         :ok <- unadvertise_worker(worker_info, cluster) do
      cleanup_worker_directory(worker_info, base_path)
    end
  end

  @spec terminate_worker_process(WorkerInfo.t(), module()) :: :ok | {:error, :not_found}
  defp terminate_worker_process(%{health: {:ok, pid}, otp_name: _otp_name}, cluster) do
    worker_supervisor = cluster.otp_name(:worker_supervisor)

    case DynamicSupervisor.terminate_child(worker_supervisor, pid) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  end

  defp terminate_worker_process(%{health: :stopped}, _cluster), do: :ok
  defp terminate_worker_process(%{health: {:failed_to_start, _}}, _cluster), do: :ok

  @spec unadvertise_worker(WorkerInfo.t(), module()) :: :ok
  defp unadvertise_worker(_worker_info, _cluster), do: :ok

  @spec cleanup_worker_directory(WorkerInfo.t(), String.t()) ::
          :ok | {:error, {:failed_to_remove_directory, File.posix(), Path.t()}}
  defp cleanup_worker_directory(%{id: worker_id}, base_path) do
    worker_path = Path.join(base_path, worker_id)

    case File.rm_rf(worker_path) do
      {:ok, _files_and_directories} -> :ok
      {:error, reason, file} -> {:error, {:failed_to_remove_directory, reason, file}}
    end
  end

  @spec do_wait_for_healthy(State.t(), GenServer.from()) :: :ok | State.t()
  def do_wait_for_healthy(%{health: :ok}, _), do: :ok
  @spec do_wait_for_healthy(State.t(), GenServer.from()) :: State.t()
  def do_wait_for_healthy(t, from), do: add_pid_to_waiting_for_healthy(t, from)

  @spec do_worker_health(State.t(), Worker.id(), WorkerInfo.health()) :: State.t()
  def do_worker_health(t, worker_id, health) do
    t
    |> put_health_for_worker(worker_id, health)
    |> recompute_health()
    |> notify_waiting_for_healthy()
  end

  @spec do_spin_up(State.t()) :: State.t()
  def do_spin_up(t) do
    t
    |> load_workers_from_disk()
    |> start_workers_that_are_stopped()
  end

  @spec load_workers_from_disk(State.t()) :: State.t()
  def load_workers_from_disk(t) do
    update_workers(t, fn workers ->
      t.path
      |> worker_info_from_path(&t.cluster.otp_name_for_worker(&1))
      |> merge_worker_info_into_workers(workers)
    end)
  end

  @spec start_workers_that_are_stopped(State.t()) :: State.t()
  def start_workers_that_are_stopped(t) do
    update_workers(t, fn workers ->
      workers
      |> Map.values()
      |> Enum.filter(&(&1.health == :stopped))
      |> try_to_start_workers(t.cluster)
      |> advertise_running_workers(t.cluster)
      |> merge_worker_info_into_workers(workers)
    end)
  end

  @spec recompute_health(State.t()) :: State.t()
  def recompute_health(t) do
    put_health(t, t.workers |> Map.values() |> compute_health_from_worker_info())
  end

  @spec merge_worker_info_into_workers(
          [WorkerInfo.t()],
          workers :: %{Worker.id() => WorkerInfo.t()}
        ) ::
          %{Worker.id() => WorkerInfo.t()}
  defp merge_worker_info_into_workers(worker_info, workers), do: Enum.into(worker_info, workers, &{&1.id, &1})

  @spec add_pid_to_waiting_for_healthy(State.t(), GenServer.from()) :: State.t()
  def add_pid_to_waiting_for_healthy(t, pid), do: update_waiting_for_healthy(t, &[pid | &1])

  @spec notify_waiting_for_healthy(State.t()) :: State.t()
  def notify_waiting_for_healthy(%{health: :ok, waiting_for_healthy: waiting_for_healthy} = t)
      when waiting_for_healthy != [] do
    :ok = Enum.each(t.waiting_for_healthy, &GenServer.reply(&1, :ok))

    put_waiting_for_healthy(t, [])
  end

  @spec notify_waiting_for_healthy(State.t()) :: State.t()
  def notify_waiting_for_healthy(t), do: t

  @spec worker_for_kind(:log) :: module()
  defp worker_for_kind(:log), do: Bedrock.DataPlane.Log.Shale

  @spec worker_for_kind(:storage) :: module()
  defp worker_for_kind(:storage), do: Bedrock.DataPlane.Storage.Basalt

  @spec otp_names_for_running_workers(State.t()) :: [atom()]
  def otp_names_for_running_workers(t), do: Enum.map(t.workers, fn {_id, %{otp_name: otp_name}} -> otp_name end)

  @spec otp_names_for_running_storage_workers(State.t()) :: [atom()]
  def otp_names_for_running_storage_workers(t) do
    t.workers
    |> Enum.filter(fn {_id, worker_info} ->
      storage_worker?(worker_info)
    end)
    |> Enum.map(fn {_id, %{otp_name: otp_name}} -> otp_name end)
  end

  @spec storage_worker?(WorkerInfo.t()) :: boolean()
  def storage_worker?(%{manifest: %{worker: worker}}) when not is_nil(worker), do: worker.kind() == :storage

  def storage_worker?(_), do: false

  @spec worker_healthy?(WorkerInfo.t()) :: boolean()
  def worker_healthy?(%{health: {:ok, _pid}}), do: true
  def worker_healthy?(_), do: false

  @spec compact_service_info_from_worker_info(WorkerInfo.t()) ::
          {String.t(), :log | :storage, atom()}
  def compact_service_info_from_worker_info(%{id: id, manifest: %{worker: worker}, otp_name: otp_name}) do
    kind = worker.kind()
    {id, kind, otp_name}
  end

  @spec service_info_from_worker_info(WorkerInfo.t()) ::
          {String.t(), :log | :storage, {atom(), node()}}
  def service_info_from_worker_info(%{id: id, manifest: %{worker: worker}, otp_name: otp_name}) do
    kind = worker.kind()
    worker_ref = {otp_name, Node.self()}
    {id, kind, worker_ref}
  end
end
