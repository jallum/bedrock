defmodule Bedrock.Cluster do
  @moduledoc false

  alias Bedrock.Cluster.Gateway
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Transaction

  require Logger

  @type t :: module()
  @type name :: String.t()
  @type version :: Bedrock.version()
  @type transaction :: Transaction.encoded()
  @type storage :: Storage.ref()
  @type log :: Log.ref()
  @type capability :: :coordination | :log | :storage | :resolution

  @callback node_capabilities() :: [Bedrock.Cluster.capability()]
  @callback coordinator!() :: Coordinator.ref()
  @callback config!() :: Config.t()
  @callback coordinator_nodes!() :: [node()]
  @callback coordinator_ping_timeout_in_ms() :: non_neg_integer()
  @callback fetch_config() :: {:ok, Config.t()} | {:error, :unavailable}
  @callback fetch_coordinator() :: {:ok, Coordinator.ref()} | {:error, :unavailable}
  @callback fetch_coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
  @callback fetch_gateway() :: {:ok, Gateway.ref()} | {:error, :unavailable}
  @callback fetch_transaction_system_layout() ::
              {:ok, TransactionSystemLayout.t()} | {:error, :unavailable}
  @callback gateway_ping_timeout_in_ms() :: non_neg_integer()
  @callback name() :: String.t()
  @callback node_config() :: Keyword.t()
  @callback otp_name() :: atom()
  @callback otp_name(service :: atom()) :: atom()
  @callback path_to_descriptor() :: Path.t()
  @callback transaction_system_layout!() :: TransactionSystemLayout.t()

  @doc false
  defmacro __using__(opts) do
    otp_app = opts[:otp_app]
    config = opts[:config]
    name = opts[:name] || raise "Missing :name option"

    if !(otp_app || config) do
      raise "Must provide either :otp_app or :config option"
    end

    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote location: :keep do
      @behaviour Bedrock.Cluster

      alias Bedrock.Client
      alias Bedrock.Cluster
      alias Bedrock.Cluster.Gateway
      alias Bedrock.ControlPlane.Config
      alias Bedrock.Internal.ClusterSupervisor
      alias Bedrock.Service.Worker

      @name unquote(name)
      @otp_app unquote(otp_app)
      @static_config unquote(config)
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
      Get the coordinator for the cluster.
      """
      @impl true
      @spec coordinator!() :: Coordinator.ref()
      def coordinator!, do: ClusterSupervisor.coordinator!(__MODULE__)

      @doc """
      Fetch the transaction system layout for the cluster.
      """
      @impl true
      @spec fetch_transaction_system_layout() ::
              {:ok, TransactionSystemLayout.t()} | {:error, :unavailable}
      def fetch_transaction_system_layout, do: ClusterSupervisor.fetch_transaction_system_layout(__MODULE__)

      @doc """
      Get the transaction system layout for the cluster.
      """
      @impl true
      @spec transaction_system_layout!() :: TransactionSystemLayout.t()
      def transaction_system_layout!, do: ClusterSupervisor.transaction_system_layout!(__MODULE__)

      @doc """
      Get the configuration for this node of the cluster.
      """
      @impl true
      @spec node_config() :: Keyword.t()
      def node_config, do: ClusterSupervisor.node_config(__MODULE__, @otp_app, @static_config)

      @doc """
      Get the capability advertised to the cluster by this node.
      """
      @impl true
      @spec node_capabilities() :: [Cluster.capability()]
      def node_capabilities, do: ClusterSupervisor.node_capabilities(__MODULE__, @otp_app, @static_config)

      @doc """
      Get the path to the descriptor file. If the path is not set in the
      configuration, we default to a file named
      "#{Cluster.default_descriptor_file_name()}" in the `priv` directory for the
      application.
      """
      @impl true
      @spec path_to_descriptor() :: Path.t()
      def path_to_descriptor, do: ClusterSupervisor.path_to_descriptor(__MODULE__, @otp_app, @static_config)

      @doc """
      Get the timeout (in milliseconds) for pinging the coordinator.
      """
      @impl true
      @spec coordinator_ping_timeout_in_ms() :: non_neg_integer()
      def coordinator_ping_timeout_in_ms,
        do: ClusterSupervisor.coordinator_ping_timeout_in_ms(__MODULE__, @otp_app, @static_config)

      @doc """
      Get the timeout (in milliseconds) for a gateway process waiting to
      receives a ping from the currently active cluster director. Once a
      gateway has successfully joined a cluster, it will wait for a ping from
      the director. If it does not receive a ping within the timeout, it
      will attempt to find a new director.
      """
      @impl true
      @spec gateway_ping_timeout_in_ms() :: non_neg_integer()
      def gateway_ping_timeout_in_ms,
        do: ClusterSupervisor.gateway_ping_timeout_in_ms(__MODULE__, @otp_app, @static_config)

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
      def otp_name(component) when is_atom(component) or is_binary(component), do: Cluster.otp_name(@name, component)

      @spec otp_name_for_worker(Worker.id()) :: atom()
      def otp_name_for_worker(id), do: otp_name("worker_#{id}")

      ######################################################################
      # Cluster Services
      ######################################################################

      @doc """
      Fetch the coordinator that the gateway is connected to.
      """
      @impl true
      @spec fetch_coordinator() :: {:ok, Coordinator.ref()} | {:error, :unavailable}
      def fetch_coordinator, do: ClusterSupervisor.fetch_coordinator(__MODULE__)

      @doc """
      Fetch the gateway for this node of the cluster.
      """
      @impl true
      @spec fetch_gateway() :: {:ok, Gateway.ref()} | {:error, :unavailable}
      def fetch_gateway, do: {:ok, @gateway_otp_name}

      @doc """
      Fetch the nodes that are running coordinators for the cluster.
      """
      @impl true
      @spec fetch_coordinator_nodes() :: {:ok, [node()]} | {:error, :unavailable}
      def fetch_coordinator_nodes, do: ClusterSupervisor.fetch_coordinator_nodes(__MODULE__)

      @doc """
      Get the nodes that are running coordinators for the cluster. If we can't
      find any, we raise an error.
      """
      @impl true
      @spec coordinator_nodes!() :: [node()]
      def coordinator_nodes!, do: ClusterSupervisor.coordinator_nodes!(__MODULE__)

      @doc false
      @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
      def child_spec(opts),
        do:
          ClusterSupervisor.child_spec([
            {:cluster, __MODULE__},
            {:node, Node.self()},
            {:otp_app, @otp_app},
            {:static_config, @static_config} | opts
          ])
    end
  end

  @doc false
  @spec default_descriptor_file_name() :: String.t()
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
  def otp_name(cluster_name, service) when is_binary(cluster_name), do: :"#{otp_name(cluster_name)}_#{service}"
end
