defmodule Bedrock.Internal.ClusterSupervisor do
  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator

  alias Bedrock.Cluster.Gateway.Tracing, as: GatewayTracing
  alias Bedrock.ControlPlane.Coordinator.Tracing, as: CoordinatorTracing
  alias Bedrock.ControlPlane.Director.Recovery.Tracing, as: RecoveryTracing
  alias Bedrock.DataPlane.CommitProxy.Tracing, as: CommitProxyTracing
  alias Bedrock.DataPlane.Log.Tracing, as: LogTracing
  alias Bedrock.Internal.Tracing.RaftTelemetry

  require Logger

  use Supervisor

  @doc false
  @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    node = opts[:node] || Node.self()
    cluster_name = cluster.name()
    path_to_descriptor = opts[:path_to_descriptor] || cluster.path_to_descriptor()

    descriptor =
      with :ok <- check_node_is_in_a_cluster(node),
           {:ok, descriptor} <- path_to_descriptor |> Descriptor.read_from_file(),
           :ok <- check_descriptor_is_for_cluster(descriptor, cluster_name) do
        descriptor
      else
        {:error, :descriptor_not_for_this_cluster} ->
          Logger.warning(
            "Bedrock: The cluster name in the descriptor file does not match the cluster name (#{cluster_name}) in the configuration."
          )

          nil

        {:error, :not_in_a_cluster} ->
          Logger.warning(
            "Bedrock: This node is not part of a cluster (use the \"--name\" or \"--sname\" option when starting the Erlang VM)"
          )

        {:error, _reason} ->
          nil
      end
      |> case do
        nil ->
          Logger.warning("Bedrock: Creating a default single-node configuration")
          Descriptor.new(cluster_name, [node])

        descriptor ->
          descriptor
      end

    %{
      id: __MODULE__,
      start:
        {Supervisor, :start_link,
         [
           __MODULE__,
           {node, cluster, path_to_descriptor, descriptor},
           []
         ]},
      restart: :permanent,
      type: :supervisor
    }
  end

  @spec check_node_is_in_a_cluster(node()) :: :ok | {:error, :not_in_a_cluster}
  defp check_node_is_in_a_cluster(:nonode@nohost), do: {:error, :not_in_a_cluster}
  defp check_node_is_in_a_cluster(_), do: :ok

  @spec check_descriptor_is_for_cluster(Descriptor.t(), cluster_name :: String.t()) ::
          :ok | {:error, :descriptor_not_for_this_cluster}
  defp check_descriptor_is_for_cluster(descriptor, cluster_name),
    do:
      if(descriptor.cluster_name == cluster_name,
        do: :ok,
        else: {:error, :descriptor_not_for_this_cluster}
      )

  @doc false
  @impl Supervisor
  def init({_node, cluster, path_to_descriptor, descriptor}) do
    capabilities = cluster.capabilities()

    cluster.node_config()
    |> Keyword.get(:trace, [])
    |> Enum.each(fn
      :coordinator -> :ok = CoordinatorTracing.start()
      :commit_proxy -> :ok = CommitProxyTracing.start()
      :log -> :ok = LogTracing.start()
      :gateway -> :ok = GatewayTracing.start()
      :raft -> :ok = RaftTelemetry.start()
      :recovery -> :ok = RecoveryTracing.start()
    end)

    children =
      [
        {DynamicSupervisor, name: cluster.otp_name(:sup)},
        {Bedrock.Cluster.PubSub, otp_name: cluster.otp_name(:pub_sub)},
        {Bedrock.Cluster.Gateway,
         [
           cluster: cluster,
           descriptor: descriptor,
           path_to_descriptor: path_to_descriptor,
           otp_name: cluster.otp_name(:gateway),
           capabilities: capabilities,
           mode: mode_for_capabilities(capabilities)
         ]}
        | children_for_capabilities(cluster, capabilities)
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp mode_for_capabilities([]), do: :passive
  defp mode_for_capabilities(_), do: :active

  defp children_for_capabilities(_cluster, []), do: []

  defp children_for_capabilities(cluster, capabilities) do
    capabilities
    |> Enum.map(&{module_for_capability(&1), &1})
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {module, capabilities} ->
      {module,
       [
         cluster: cluster,
         capabilities: capabilities
       ] ++ Keyword.get(cluster.node_config(), module.config_key(), [])}
    end)
  end

  defp module_for_capability(:coordination), do: Bedrock.ControlPlane.Coordinator
  defp module_for_capability(:storage), do: Bedrock.Service.Foreman
  defp module_for_capability(:log), do: Bedrock.Service.Foreman

  defp module_for_capability(capability),
    do: raise("Unknown capability: #{inspect(capability)}")

  @spec fetch_config(module()) :: {:ok, Config.t()} | {:error, :unavailable}
  def fetch_config(module) do
    with {:ok, coordinator} <- module.fetch_coordinator() do
      Coordinator.fetch_config(coordinator)
    end
  end

  @spec config!(module()) :: Config.t()
  def config!(module) do
    fetch_config(module)
    |> case do
      {:ok, config} -> config
      {:error, _} -> raise "No configuration available"
    end
  end

  @spec director!(module()) :: Director.ref()
  def director!(module) do
    module.fetch_director()
    |> case do
      {:ok, director} -> director
      {:error, _} -> raise "No director available"
    end
  end

  @spec coordinator!(module()) :: Coordinator.ref()
  def coordinator!(module) do
    module.fetch_coordinator()
    |> case do
      {:ok, coordinator} -> coordinator
      {:error, _} -> raise "No coordinator available"
    end
  end

  @spec coordinator_nodes!(module()) :: [node()]
  def coordinator_nodes!(module) do
    module.fetch_coordinator_nodes()
    |> case do
      {:ok, coordinator_nodes} -> coordinator_nodes
      {:error, _} -> raise "No coordinator nodes available"
    end
  end

  @spec path_to_descriptor(module(), otp_app :: atom() | nil) :: Path.t()
  def path_to_descriptor(module, nil) do
    module.node_config()
    |> Keyword.get(
      :path_to_descriptor,
      Bedrock.Cluster.default_descriptor_file_name()
    )
  end

  def path_to_descriptor(module, otp_app) do
    module.node_config()
    |> Keyword.get(
      :path_to_descriptor,
      Path.join(
        Application.app_dir(otp_app, "priv"),
        Bedrock.Cluster.default_descriptor_file_name()
      )
    )
  end
end
