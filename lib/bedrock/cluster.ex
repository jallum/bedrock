defmodule Bedrock.Cluster do
  alias Bedrock.Cluster
  alias Bedrock.Cluster.Gateway
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Log.Transaction

  require Logger

  @type t :: module()
  @type name :: String.t()
  @type version :: Bedrock.version()
  @type transaction :: Transaction.t()
  @type storage :: Storage.ref()
  @type log :: Log.ref()
  @type capability :: :coordination | :log | :storage

  @callback capabilities() :: [Bedrock.Cluster.capability()]
  @callback config!() :: Config.t()
  @callback coordinator!() :: Coordinator.ref()
  @callback coordinator_nodes!() :: [node()]
  @callback coordinator_ping_timeout_in_ms() :: non_neg_integer()
  @callback director!() :: Director.ref()
  @callback fetch_config() :: {:ok, Config.t()} | {:error, :unavailable}
  @callback fetch_coordinator() :: {:ok, Coordinator.ref()} | {:error, :unavailable}
  @callback fetch_coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
  @callback fetch_director() :: {:ok, Director.ref()} | {:error, :unavailable}
  @callback fetch_gateway() :: {:ok, Gateway.ref()} | {:error, :unavailable}
  @callback gateway_ping_timeout_in_ms() :: non_neg_integer()
  @callback name() :: String.t()
  @callback node_config() :: Keyword.t()
  @callback otp_name() :: atom()
  @callback otp_name(service :: atom()) :: atom()
  @callback path_to_descriptor() :: Path.t()

  @doc false
  defmacro __using__(opts) do
    otp_app = opts[:otp_app] || raise "Missing :otp_app option"
    name = opts[:name] || raise "Missing :name option"

    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote location: :keep do
      @behaviour Bedrock.Cluster

      alias Bedrock.Client
      alias Bedrock.Cluster
      alias Bedrock.Cluster.Gateway
      alias Bedrock.ControlPlane.Config
      alias Bedrock.Internal.ClusterSupervisor
      alias Bedrock.Service.Worker

      @default_coordinator_ping_timeout_in_ms 300
      @default_gateway_ping_timeout_in_ms 300

      @name unquote(name)
      @otp_name Cluster.otp_name(@name)

      @supervisor_otp_name Cluster.otp_name(@name, :sup)
      @coordinator_otp_name Cluster.otp_name(@name, :coordinator)
      @foreman_otp_name Cluster.otp_name(@name, :foreman)
      @gateway_otp_name Cluster.otp_name(@name, :gateway)
      @sequencer_otp_name Cluster.otp_name(@name, :sequencer)
      @worker_supervisor_otp_name Cluster.otp_name(@name, :worker_supervisor)

      @doc """
      Get the name of the cluster.
      """
      @impl true
      @spec name() :: Cluster.name()
      def name, do: @name

      ######################################################################
      # Configuration
      ######################################################################

      @doc """
      Fetch the configuration for the cluster.
      """
      @impl true
      @spec fetch_config() :: {:ok, Config.t()} | {:error, :unavailable}
      def fetch_config, do: ClusterSupervisor.fetch_config(__MODULE__)

      @doc """
      Fetch the configuration for the cluster.
      """
      @impl true
      @spec config!() :: Config.t()
      def config!, do: ClusterSupervisor.config!(__MODULE__)

      @doc """
      Get the configuration for this node of the cluster.
      """
      @impl true
      @spec node_config() :: Keyword.t()
      def node_config, do: Application.get_env(unquote(otp_app), __MODULE__, [])

      @doc """
      Get the capability advertised to the cluster by this node.
      """
      @impl true
      @spec capabilities() :: [Cluster.capability()]
      def capabilities, do: node_config() |> Keyword.get(:capabilities, [])

      @doc """
      Get the path to the descriptor file. If the path is not set in the
      configuration, we default to a file named
      "#{Cluster.default_descriptor_file_name()}" in the `priv` directory for the
      application.
      """
      @impl true
      @spec path_to_descriptor() :: Path.t()
      def path_to_descriptor,
        do: ClusterSupervisor.path_to_descriptor(__MODULE__, unquote(otp_app))

      @doc """
      Get the timeout (in milliseconds) for pinging the coordinator.
      """
      @impl true
      @spec coordinator_ping_timeout_in_ms() :: non_neg_integer()
      def coordinator_ping_timeout_in_ms do
        node_config()
        |> Keyword.get(:coordinator_ping_timeout_in_ms, @default_coordinator_ping_timeout_in_ms)
      end

      @doc """
      Get the timeout (in milliseconds) for a gateway process waiting to
      receives a ping from the currently active cluster director. Once a
      gateway has successfully joined a cluster, it will wait for a ping from
      the director. If it does not receive a ping within the timeout, it
      will attempt to find a new director.
      """
      @impl true
      @spec gateway_ping_timeout_in_ms() :: non_neg_integer()
      def gateway_ping_timeout_in_ms do
        node_config()
        |> Keyword.get(:gateway_ping_timeout_in_ms, @default_gateway_ping_timeout_in_ms)
      end

      ######################################################################
      # OTP Names
      ######################################################################

      @doc """
      Get the OTP name for the cluster.
      """
      @impl true
      @spec otp_name() :: atom()
      def otp_name, do: @otp_name

      @doc """
      Get the OTP name for a component within the cluster. These names are
      limited in scope to the current node.
      """
      @impl true
      @spec otp_name(
              :sup
              | :foreman
              | :coordinator
              | :gateway
              | :storage
              | :log
            ) :: atom()
      def otp_name(:coordinator), do: @coordinator_otp_name
      def otp_name(:foreman), do: @foreman_otp_name
      def otp_name(:gateway), do: @gateway_otp_name
      def otp_name(:sup), do: @supervisor_otp_name
      def otp_name(:worker_supervisor), do: @worker_supervisor_otp_name

      @spec otp_name(atom() | String.t()) :: atom()
      def otp_name(component) when is_atom(component) or is_binary(component),
        do: Cluster.otp_name(@name, component)

      @spec otp_name_for_worker(Worker.id()) :: atom()
      def otp_name_for_worker(id), do: otp_name("worker_#{id}")

      ######################################################################
      # Cluster Services
      ######################################################################

      @doc """
      Fetch the gateway for this node of the cluster.
      """
      @impl true
      @spec fetch_gateway() :: {:ok, Gateway.ref()} | {:error, :unavailable}
      def fetch_gateway, do: {:ok, otp_name(:gateway)}

      @doc """
      Fetch the current director for the cluster. If we can't find one, we
      return an error.
      """
      @impl true
      @spec fetch_director() :: {:ok, Director.ref()} | {:error, :unavailable}
      def fetch_director, do: otp_name(:gateway) |> Gateway.fetch_director()

      @doc """
      Get the current director for the cluster. If we can't find one, we
      raise an error.
      """
      @impl true
      @spec director!() :: Director.ref()
      def director!, do: ClusterSupervisor.director!(__MODULE__)

      @doc """
      Fetch a coordinator for the cluster. If there is an instance running on
      the local node, we return it. Otherwise, we look for a live coordinator
      on the cluster. If we can't find one, we return an error.
      """
      @impl true
      @spec fetch_coordinator() :: {:ok, Coordinator.ref()} | {:error, :unavailable}
      def fetch_coordinator, do: otp_name(:gateway) |> Gateway.fetch_coordinator()

      @doc """
      Get a coordinator for the cluster. If there is an instance running on
      the local node, we return it. Otherwise, we look for a live coordinator
      on the cluster. If we can't find one, we raise an error.
      """
      @impl true
      @spec coordinator!() :: Coordinator.ref()
      def coordinator!, do: ClusterSupervisor.coordinator!(__MODULE__)

      @doc """
      Fetch the nodes that are running coordinators for the cluster.
      """
      @impl true
      @spec fetch_coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
      def fetch_coordinator_nodes, do: otp_name(:gateway) |> Gateway.fetch_coordinator_nodes()

      @doc """
      Get the nodes that are running coordinators for the cluster. If we can't
      find any, we raise an error.
      """
      @impl true
      @spec coordinator_nodes!() :: [node()]
      def coordinator_nodes!, do: ClusterSupervisor.coordinator_nodes!(__MODULE__)

      @doc false
      def child_spec(opts),
        do: ClusterSupervisor.child_spec([{:cluster, __MODULE__}, {:node, Node.self()} | opts])
    end
  end

  @doc false
  def default_descriptor_file_name, do: "bedrock.cluster"

  @doc """
  Get the OTP name for the cluster with the given name.
  """
  @spec otp_name(name()) :: atom()
  def otp_name(cluster_name) when is_binary(cluster_name), do: :"bedrock_#{cluster_name}"

  @doc """
  Get the OTP name for a component within the cluster with the given name.
  """
  @spec otp_name(name(), service :: atom() | String.t()) :: atom()
  def otp_name(cluster_name, service) when is_binary(cluster_name),
    do: :"#{otp_name(cluster_name)}_#{service}"
end
