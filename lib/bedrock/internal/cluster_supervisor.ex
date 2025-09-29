defmodule Bedrock.Internal.ClusterSupervisor do
  @moduledoc false
  use Supervisor

  alias Bedrock.Cluster
  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Gateway
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Coordinator.Tracing, as: CoordinatorTracing
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Director.Recovery.Tracing, as: RecoveryTracing
  alias Bedrock.DataPlane.CommitProxy.Tracing, as: CommitProxyTracing
  alias Bedrock.DataPlane.Log.Tracing, as: LogTracing
  alias Bedrock.DataPlane.Resolver.Tracing, as: ResolverTracing
  alias Bedrock.DataPlane.Sequencer.Tracing, as: SequencerTracing
  alias Bedrock.DataPlane.Storage.Olivine.Tracing, as: OlivineTracing
  alias Bedrock.DataPlane.Storage.Tracing, as: StorageTracing
  alias Bedrock.Internal.Tracing.RaftTelemetry
  alias Bedrock.Service.Foreman
  alias Cluster.Gateway.Tracing, as: GatewayTracing

  require Logger

  @doc false
  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            node: node(),
            otp_app: atom() | nil,
            static_config: Keyword.t() | nil,
            path_to_descriptor: Path.t()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    node = opts[:node] || Node.self()
    otp_app = opts[:otp_app]
    static_config = opts[:static_config]
    cluster_name = cluster.name()

    path_to_descriptor =
      opts[:path_to_descriptor] || path_to_descriptor(cluster, otp_app, static_config)

    with_result =
      with :ok <- check_node_is_in_a_cluster(node),
           {:ok, descriptor} <- Descriptor.read_from_file(path_to_descriptor),
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
            ~s[Bedrock: This node is not part of a cluster (use the "--name" or "--sname" option when starting the Erlang VM)]
          )

          nil

        {:error, _reason} ->
          nil
      end

    descriptor =
      case with_result do
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
           {node, cluster, otp_app, static_config, path_to_descriptor, descriptor},
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
    do: if(descriptor.cluster_name == cluster_name, do: :ok, else: {:error, :descriptor_not_for_this_cluster})

  @doc false
  @impl Supervisor
  def init({_node, cluster, otp_app, static_config, path_to_descriptor, descriptor}) do
    capabilities = node_capabilities(cluster, otp_app, static_config)
    config = node_config(cluster, otp_app, static_config)

    config
    |> Keyword.get(:trace, [])
    |> Enum.each(fn
      :commit_proxy -> start_tracing(CommitProxyTracing)
      :coordinator -> start_tracing(CoordinatorTracing)
      :gateway -> start_tracing(GatewayTracing)
      :log -> start_tracing(LogTracing)
      :raft -> start_tracing(RaftTelemetry)
      :recovery -> start_tracing(RecoveryTracing)
      :resolver -> start_tracing(ResolverTracing)
      :sequencer -> start_tracing(SequencerTracing)
      :storage -> start_tracing(StorageTracing)
      :olivine -> start_tracing(OlivineTracing)
      unsupported -> Logger.warning("Unsupported tracing module: #{inspect(unsupported)}")
    end)

    children =
      [
        {DynamicSupervisor, name: cluster.otp_name(:sup)},
        {Gateway,
         [
           cluster: cluster,
           descriptor: descriptor,
           path_to_descriptor: path_to_descriptor,
           otp_name: cluster.otp_name(:gateway),
           capabilities: capabilities,
           mode: mode_for_capabilities(capabilities)
         ]}
        | children_for_capabilities(cluster, capabilities, config)
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp mode_for_capabilities([]), do: :passive
  defp mode_for_capabilities(_), do: :active

  defp children_for_capabilities(_cluster, [], _config), do: []

  defp children_for_capabilities(cluster, capabilities, config) do
    capabilities
    |> Enum.map(&{module_for_capability(&1), &1})
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {module, capabilities} ->
      # For modules that serve multiple capabilities (like Foreman),
      # we need to find a common config that works for all capabilities
      capability_configs =
        capabilities
        |> Enum.map(fn capability ->
          Keyword.get(config, capability, [])
        end)
        |> Enum.reduce([], fn capability_config, acc ->
          Keyword.merge(acc, capability_config)
        end)

      # Fall back to module-specific config if no capability-specific config exists
      module_config = Keyword.get(config, module.config_key(), [])

      # Merge configs with capability-specific taking precedence
      merged_config = Keyword.merge(module_config, capability_configs)

      {module,
       [
         cluster: cluster,
         capabilities: capabilities
       ] ++ merged_config}
    end)
  end

  defp module_for_capability(:coordination), do: Coordinator
  defp module_for_capability(:storage), do: Foreman
  defp module_for_capability(:log), do: Foreman

  defp module_for_capability(capability), do: raise("Unknown capability: #{inspect(capability)}")

  defp start_tracing(module) do
    case module.start() do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  @spec fetch_config(Cluster.t()) :: {:ok, Config.t()} | {:error, :unavailable}
  def fetch_config(module) do
    with {:ok, coordinator} <- module.fetch_coordinator() do
      Coordinator.fetch_config(coordinator)
    end
  end

  @spec config!(Cluster.t()) :: Config.t()
  def config!(module) do
    module
    |> fetch_config()
    |> case do
      {:ok, config} -> config
      {:error, _} -> raise "No configuration available"
    end
  end

  @spec fetch_transaction_system_layout(Cluster.t()) ::
          {:ok, TransactionSystemLayout.t()} | {:error, :unavailable}
  def fetch_transaction_system_layout(module) do
    with {:ok, coordinator} <- module.fetch_coordinator() do
      Coordinator.fetch_transaction_system_layout(coordinator)
    end
  end

  @spec transaction_system_layout!(Cluster.t()) :: TransactionSystemLayout.t()
  def transaction_system_layout!(module) do
    module
    |> fetch_transaction_system_layout()
    |> case do
      {:ok, layout} -> layout
      {:error, _} -> raise "No transaction system layout available"
    end
  end

  @spec director!(Cluster.t()) :: Director.ref()
  def director!(module) do
    case module.fetch_director() do
      {:ok, director} -> director
      {:error, _} -> raise "No director available"
    end
  end

  @spec fetch_coordinator(Cluster.t()) :: {:ok, Coordinator.ref()} | {:error, :unavailable}
  def fetch_coordinator(module) do
    case module.fetch_gateway() do
      {:ok, gateway} ->
        # Get the coordinator from the gateway's current state
        case GenServer.call(gateway, :get_known_coordinator, 1000) do
          {:ok, coordinator} -> {:ok, coordinator}
          _ -> {:error, :unavailable}
        end

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    _ -> {:error, :unavailable}
  end

  @spec coordinator!(Cluster.t()) :: Coordinator.ref()
  def coordinator!(module) do
    module
    |> fetch_coordinator()
    |> case do
      {:ok, coordinator} -> coordinator
      {:error, _} -> raise "No coordinator available"
    end
  end

  @spec fetch_coordinator_nodes(Cluster.t()) :: {:ok, [node()]} | {:error, :unavailable}
  def fetch_coordinator_nodes(module) do
    # Try to get descriptor from the running gateway first, since it has the
    # canonical descriptor (which may be an in-memory default if no file exists)
    case try_gateway_descriptor(module) do
      {:ok, descriptor} -> {:ok, descriptor.coordinator_nodes}
      {:error, _reason} -> try_disk_descriptor(module)
    end
  end

  @spec try_gateway_descriptor(Cluster.t()) :: {:ok, Descriptor.t()} | {:error, term()}
  defp try_gateway_descriptor(module) do
    with {:ok, gateway} <- module.fetch_gateway(),
         {:ok, descriptor} <- Gateway.get_descriptor(gateway) do
      {:ok, descriptor}
    else
      {:error, reason} -> {:error, {:gateway_error, reason}}
    end
  end

  @spec try_disk_descriptor(Cluster.t()) :: {:ok, [node()]} | {:error, :unavailable}
  defp try_disk_descriptor(module) do
    case Descriptor.read_from_file(module.path_to_descriptor()) do
      {:ok, descriptor} -> {:ok, descriptor.coordinator_nodes}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  @spec coordinator_nodes!(Cluster.t()) :: [node()]
  def coordinator_nodes!(module) do
    module
    |> fetch_coordinator_nodes()
    |> case do
      {:ok, coordinator_nodes} -> coordinator_nodes
      {:error, _} -> raise "No coordinator nodes available"
    end
  end

  @spec node_config(Cluster.t(), otp_app :: atom() | nil, static_config :: Keyword.t() | nil) ::
          Keyword.t()
  def node_config(module, otp_app, static_config) do
    case static_config do
      nil when otp_app != nil ->
        Application.get_env(otp_app, module, [])

      nil ->
        []

      config ->
        config
    end
  end

  @spec node_capabilities(
          Cluster.t(),
          otp_app :: atom() | nil,
          static_config :: Keyword.t() | nil
        ) ::
          [Cluster.capability()]
  def node_capabilities(module, otp_app, static_config) do
    module
    |> node_config(otp_app, static_config)
    |> Keyword.get(:capabilities, [])
  end

  @spec coordinator_ping_timeout_in_ms(
          Cluster.t(),
          otp_app :: atom() | nil,
          static_config :: Keyword.t() | nil
        ) :: non_neg_integer()
  def coordinator_ping_timeout_in_ms(module, otp_app, static_config) do
    module
    |> node_config(otp_app, static_config)
    |> Keyword.get(:coordinator_ping_timeout_in_ms, 300)
  end

  @spec gateway_ping_timeout_in_ms(
          Cluster.t(),
          otp_app :: atom() | nil,
          static_config :: Keyword.t() | nil
        ) :: non_neg_integer()
  def gateway_ping_timeout_in_ms(module, otp_app, static_config) do
    module
    |> node_config(otp_app, static_config)
    |> Keyword.get(:gateway_ping_timeout_in_ms, 300)
  end

  @spec path_to_descriptor(
          Cluster.t(),
          otp_app :: atom() | nil,
          static_config :: Keyword.t() | nil
        ) :: Path.t()
  def path_to_descriptor(module, otp_app, static_config) do
    module
    |> node_config(otp_app, static_config)
    |> Keyword.get(
      :path_to_descriptor,
      case otp_app do
        nil ->
          Cluster.default_descriptor_file_name()

        app ->
          Path.join(
            Application.app_dir(app, "priv"),
            Cluster.default_descriptor_file_name()
          )
      end
    )
  rescue
    _ -> Cluster.default_descriptor_file_name()
  end
end
