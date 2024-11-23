defmodule Bedrock.Service.Foreman.StartingWorkers do
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.Worker

  import Bedrock.Service.Foreman.WorkerInfo,
    only: [put_health: 2, put_manifest: 2, put_otp_name: 2]

  import Bedrock.Service.Foreman.WorkingDirectory,
    only: [initialize_working_directory: 2, read_and_validate_manifest: 3]

  @spec worker_info_from_path(Path.t(), otp_namer :: (Worker.id() -> Worker.otp_name())) ::
          [WorkerInfo.t()]
  def worker_info_from_path(path, otp_namer) do
    path
    |> worker_paths_from_disk()
    |> Enum.map(&worker_info_for_id(Path.basename(&1), &1, otp_namer))
  end

  @spec worker_paths_from_disk(Path.t()) :: [Path.t()]
  def worker_paths_from_disk(path) do
    path
    |> Path.join("*")
    |> Path.wildcard()
  end

  def worker_info_for_id(id, path, otp_namer) when is_binary(id),
    do: %WorkerInfo{
      id: id,
      path: path,
      otp_name: otp_namer.(id),
      health: :stopped
    }

  @spec try_to_start_workers([WorkerInfo.t()], cluster :: module()) :: [WorkerInfo.t()]
  def try_to_start_workers(worker_info, cluster) do
    worker_info
    |> Task.async_stream(&try_to_start_worker(&1, cluster))
    |> Enum.map(fn
      {:ok, worker_info} -> worker_info
      {:error, reason} -> worker_info |> put_health({:failed_to_start, reason})
    end)
    |> Enum.to_list()
  end

  defmodule(StartWorkerOp) do
    @type t :: %__MODULE__{}
    defstruct [:path, :id, :otp_name, :cluster, :manifest, :child_spec, :pid, :error]
  end

  @spec try_to_start_worker(WorkerInfo.t(), cluster :: module()) :: WorkerInfo.t()
  def try_to_start_worker(worker_info, cluster) do
    %StartWorkerOp{
      id: worker_info.id,
      path: worker_info.path,
      otp_name: worker_info.otp_name,
      cluster: cluster
    }
    |> load_manifest()
    |> build_child_spec()
    |> start_supervised_child()
    |> find_worker()
    |> then(fn op ->
      worker_info
      |> put_manifest(op.manifest)
      |> put_otp_name(op.otp_name)
      |> put_health(
        case op.error do
          nil -> {:ok, op.pid}
          {:error, reason} -> {:failed_to_start, reason}
        end
      )
    end)
  end

  @spec load_manifest(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp load_manifest(%{error: nil} = op) do
    case read_and_validate_manifest(op.path, op.id, op.cluster.name()) do
      {:ok, manifest} -> %{op | manifest: manifest}
      error -> %{op | error: error}
    end
  end

  @spec build_child_spec(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp build_child_spec(%{error: nil} = op) do
    op.manifest.worker.child_spec(
      cluster: op.cluster,
      path: op.path,
      id: op.id,
      otp_name: op.otp_name,
      foreman: op.cluster.otp_name(:foreman),
      params: op.manifest.params
    )
    |> Map.put(:restart, :transient)
    |> then(&%{op | child_spec: &1})
  end

  @spec start_supervised_child(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp start_supervised_child(%{error: nil} = op) do
    case DynamicSupervisor.start_child(op.cluster.otp_name(:worker_supervisor), op.child_spec) do
      {:ok, _root_pid} -> op
      {:error, {:already_started, _root_pid}} -> op
      error -> %{op | error: error}
    end
  end

  @spec find_worker(StartWorkerOp.t()) :: StartWorkerOp.t()
  defp find_worker(%{error: nil} = op) do
    case Process.whereis(op.otp_name) do
      nil -> %{op | error: {:error, :process_not_started}}
      pid -> %{op | pid: pid}
    end
  end

  defp find_worker(op), do: op

  @spec initialize_new_worker(
          Worker.id(),
          worker :: module(),
          params :: map(),
          Path.t(),
          cluster :: module()
        ) :: WorkerInfo.t()
  def initialize_new_worker(id, worker, params, path, cluster) do
    working_directory = Path.join(path, id)
    worker_info = worker_info_for_id(id, working_directory, &cluster.otp_name_for_worker(&1))
    manifest = Manifest.new(cluster.name(), id, worker, params)

    case initialize_working_directory(working_directory, manifest) do
      :ok -> worker_info
      {:error, reason} -> worker_info |> put_health({:failed_to_start, reason})
    end
  end

  @spec random_worker_id() :: binary()
  def random_worker_id, do: :crypto.strong_rand_bytes(5) |> Base.encode32(case: :lower)
end
