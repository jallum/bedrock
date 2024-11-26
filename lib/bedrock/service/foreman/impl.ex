defmodule Bedrock.Service.Foreman.Impl do
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Worker
  alias Bedrock.Cluster.Gateway

  import Bedrock.Service.Foreman.State

  import Bedrock.Service.Foreman.StartingWorkers,
    only: [
      worker_info_from_path: 2,
      try_to_start_workers: 2,
      try_to_start_worker: 2,
      initialize_new_worker: 5
    ]

  import Bedrock.Service.Foreman.Health,
    only: [compute_health_from_worker_info: 1]

  def do_fetch_workers(t),
    do: otp_names_for_running_workers(t)

  @spec do_new_worker(State.t(), Worker.id(), :log | :storage) :: {State.t(), Worker.ref()}
  def do_new_worker(t, id, kind) do
    worker_info =
      id
      |> initialize_new_worker(worker_for_kind(kind), %{}, t.path, t.cluster)
      |> try_to_start_worker(t.cluster)
      |> advertise_running_worker(t.cluster)

    t =
      t
      |> update_workers(&Map.put(&1, id, worker_info))

    {t, worker_info.otp_name}
  end

  def advertise_running_workers(worker_infos, cluster) do
    Enum.each(worker_infos, &advertise_running_worker(&1, cluster))
    worker_infos
  end

  def advertise_running_worker(%{health: {:ok, pid}} = t, cluster) do
    Gateway.advertise_worker(cluster.otp_name(:gateway), pid)
    t
  end

  def advertise_running_worker(t, _), do: t

  @spec do_wait_for_healthy(State.t(), GenServer.from()) :: :ok | State.t()
  def do_wait_for_healthy(%{health: :ok}, _), do: :ok
  def do_wait_for_healthy(t, from), do: t |> add_pid_to_waiting_for_healthy(from)

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
    t
    |> update_workers(fn workers ->
      t.path
      |> worker_info_from_path(&t.cluster.otp_name_for_worker(&1))
      |> merge_worker_info_into_workers(workers)
    end)
  end

  @spec start_workers_that_are_stopped(State.t()) :: State.t()
  def start_workers_that_are_stopped(t) do
    t
    |> update_workers(fn workers ->
      workers
      |> Map.values()
      |> Enum.filter(&(&1.health == :stopped))
      |> try_to_start_workers(t.cluster)
      |> advertise_running_workers(t.cluster)
      |> merge_worker_info_into_workers(workers)
    end)
  end

  def recompute_health(t) do
    t
    |> put_health(
      t.workers
      |> Map.values()
      |> compute_health_from_worker_info()
    )
  end

  @spec merge_worker_info_into_workers(
          [WorkerInfo.t()],
          workers :: %{Worker.id() => WorkerInfo.t()}
        ) ::
          %{Worker.id() => WorkerInfo.t()}
  defp merge_worker_info_into_workers(worker_info, workers),
    do: Enum.into(worker_info, workers, &{&1.id, &1})

  @spec add_pid_to_waiting_for_healthy(State.t(), pid()) :: State.t()
  def add_pid_to_waiting_for_healthy(t, pid),
    do: t |> update_waiting_for_healthy(&[pid | &1])

  @spec notify_waiting_for_healthy(State.t()) :: State.t()
  def notify_waiting_for_healthy(%{health: :ok, waiting_for_healthy: waiting_for_healthy} = t)
      when waiting_for_healthy != [] do
    :ok = Enum.each(t.waiting_for_healthy, &GenServer.reply(&1, :ok))

    t |> put_waiting_for_healthy([])
  end

  def notify_waiting_for_healthy(t), do: t

  @spec worker_for_kind(:log | :storage) :: module()
  defp worker_for_kind(:log), do: Bedrock.DataPlane.Log.Shale
  defp worker_for_kind(:storage), do: Bedrock.DataPlane.Storage.Basalt

  @spec otp_names_for_running_workers(State.t()) :: [atom()]
  def otp_names_for_running_workers(t),
    do: Enum.map(t.workers, fn {_id, %{otp_name: otp_name}} -> otp_name end)
end
