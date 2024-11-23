defmodule Bedrock.Cluster.Bootstrap do
  alias Bedrock.Service

  def child_spec(args) do
    cluster = args[:cluster] || raise ":cluster is required"
    foreman = args[:foreman] || raise ":foreman is required"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link, [__MODULE__.Service, [cluster: cluster, foreman: foreman], []]}
    }
  end

  defmodule Booting do
    alias Bedrock.Service.Foreman

    @type t :: %{}

    def run_boot(t, step \\ :initial) do
      case boot_step(step, t) do
        {:completed, t} ->
          {:ok, t}

        {{:stalled, _reason} = stalled, t} ->
          {stalled, t}

        {new_step, new_t} when is_atom(new_step) and step != new_step ->
          run_boot(new_t, new_step)
      end
    end

    def boot_step(:initial, t), do: {:find_workers, t}

    def boot_step(:find_workers, t) do
      with {:ok, workers} <- Foreman.all(t.foreman) do
        if 0 == length(workers) do
          {:seed_new_system, t}
        else
          {:completed, Map.put(t, :workers, workers)}
        end
      else
        {:error, reason} ->
          {{:stalled, reason}, t}
      end
    end

    def boot_step(:seed_new_system, t) do
      with {:ok, storage} <- Foreman.new_worker(t.foreman, random_worker_id(), :storage),
           {:ok, log_1} <- Foreman.new_worker(t.foreman, random_worker_id(), :log),
           {:ok, log_2} <- Foreman.new_worker(t.foreman, random_worker_id(), :log) do
        {:completed, Map.put(t, :workers, [storage, log_1, log_2])}
      end
    end

    defp random_worker_id() do
      :rand.uniform(1_000_000_000)
      |> Integer.to_string(36)
      |> String.slice(1..5)
      |> String.downcase()
    end
  end

  defmodule Service do
    alias Bedrock.Cluster.Bootstrap.Telemetry

    use GenServer

    @impl true
    def init(opts) do
      %{
        cluster: opts[:cluster],
        foreman: opts[:foreman]
      }
      |> then(&{:ok, &1, {:continue, :run_boot}})
    end

    @impl true
    def handle_continue(:run_boot, t) do
      Telemetry.execute_started(t.cluster)

      case Booting.run_boot(t) do
        {:ok, t} ->
          Telemetry.execute_completed()
          {:noreply, t}

        {stalled, t} ->
          Telemetry.execute_stalled(stalled)
          {:stop, :normal, t}
      end
    end
  end
end
