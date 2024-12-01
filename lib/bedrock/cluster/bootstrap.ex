defmodule Bedrock.Cluster.Bootstrap do
  @moduledoc """
  Everything needs to start _somewhere_. For Bedrock, that somewhere is here.
  """

  def child_spec(args) do
    cluster = args[:cluster] || raise ":cluster is required"
    foreman = args[:foreman] || raise ":foreman is required"
    # seed_nodes = args[:seed_nodes] || [node()]

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__.Service,
           [
             cluster: cluster,
             foreman: foreman,
             seed_nodes: []
           ],
           []
         ]}
    }
  end
end
