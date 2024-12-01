defmodule Bedrock.Cluster.Bootstrap.Service do
  alias Bedrock.Cluster.Bootstrap.FindingSeedNodes
  alias Bedrock.Cluster.Bootstrap.Telemetry

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @impl true
  def init(opts) do
    %{
      cluster: opts[:cluster],
      foreman: opts[:foreman],
      seed_nodes: opts[:seed_nodes]
    }
    |> then(&{:ok, &1, {:continue, :find_seed_nodes}})
  end

  @impl true
  def handle_continue(:find_seed_nodes, t) do
    Telemetry.execute_started(t.cluster)

    case FindingSeedNodes.run(t) do
      {:ok, t} ->
        Telemetry.execute_completed()

        if t.seed_nodes == [] do
          t |> noreply(continue: :start_a_new_cluster)
        else
          t |> noreply(continue: :try_to_contact_seed_nodes)
        end

      {stalled, t} ->
        Telemetry.execute_stalled(stalled)
        t |> stop(:normal)
    end
  end

  def handle_continue(:start_a_new_cluster, t) do
    IO.inspect("Starting a new cluster")
    IO.inspect(t)
    t |> noreply()
  end

  def handle_continue(:try_to_contact_seed_nodes, t) do
    IO.inspect("Trying to contact seed nodes")
    IO.inspect(t)
    t |> noreply()
  end
end
