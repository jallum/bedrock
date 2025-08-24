defmodule Bedrock.DataPlane.Storage.Olivine.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager
  alias Bedrock.Internal.WaitingList
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  @type t :: %__MODULE__{
          otp_name: atom(),
          path: Path.t(),
          foreman: Foreman.ref(),
          id: Worker.id(),
          database: Database.t(),
          version_manager: VersionManager.t(),
          pull_task: Task.t() | nil,
          epoch: Bedrock.epoch() | nil,
          director: Director.ref() | nil,
          mode: :locked | :running,
          waiting_fetches: WaitingList.t(),
          active_tasks: MapSet.t(pid())
        }
  defstruct otp_name: nil,
            path: nil,
            foreman: nil,
            id: nil,
            database: nil,
            version_manager: nil,
            pull_task: nil,
            epoch: nil,
            director: nil,
            mode: :locked,
            waiting_fetches: %{},
            active_tasks: MapSet.new()

  @spec update_mode(t(), :locked | :running) :: t()
  def update_mode(t, mode), do: %{t | mode: mode}

  @spec update_director_and_epoch(t(), Director.ref() | nil, Bedrock.epoch() | nil) :: t()
  def update_director_and_epoch(t, director, epoch), do: %{t | director: director, epoch: epoch}

  @spec reset_puller(t()) :: t()
  def reset_puller(t), do: %{t | pull_task: nil}

  @spec put_puller(t(), Task.t()) :: t()
  def put_puller(t, pull_task), do: %{t | pull_task: pull_task}

  @spec add_active_task(t(), pid()) :: t()
  def add_active_task(t, task_pid) do
    Process.monitor(task_pid)
    %{t | active_tasks: MapSet.put(t.active_tasks, task_pid)}
  end

  @spec remove_active_task(t(), pid()) :: t()
  def remove_active_task(t, task_pid), do: %{t | active_tasks: MapSet.delete(t.active_tasks, task_pid)}

  @spec get_active_tasks(t()) :: MapSet.t(pid())
  def get_active_tasks(t), do: t.active_tasks
end
