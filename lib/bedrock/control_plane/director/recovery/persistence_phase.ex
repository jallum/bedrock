defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhase do
  @moduledoc """
  Persists cluster configuration through a complete system transaction.

  Constructs a system transaction containing the full cluster configuration and
  submits it through the entire data plane pipeline. This simultaneously persists
  the new configuration and validates that all transaction components work correctly.

  Stores configuration in both monolithic and decomposed formats. Monolithic keys
  support coordinator handoff while decomposed keys allow targeted component access.

  If the system transaction fails, the director exits immediately rather than
  retrying. System transaction failure indicates fundamental problems that require
  coordinator restart with a new epoch.

  Transitions to monitoring on success or exits the director on failure.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Persistence
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.SystemKeys

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    trace_recovery_persisting_system_state()

    transaction_system_layout = recovery_attempt.transaction_system_layout

    with system_transaction <-
           build_system_transaction(
             recovery_attempt.epoch,
             context.cluster_config,
             transaction_system_layout,
             recovery_attempt.cluster
           ),
         {:ok, _version} <-
           submit_system_transaction(system_transaction, recovery_attempt.proxies, context) do
      trace_recovery_system_state_persisted()

      {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.MonitoringPhase}
    else
      {:error, reason} ->
        trace_recovery_system_transaction_failed(reason)
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  @spec build_system_transaction(
          epoch :: non_neg_integer(),
          cluster_config :: Config.t(),
          transaction_system_layout :: TransactionSystemLayout.t(),
          cluster :: module()
        ) :: BedrockTransaction.encoded()
  defp build_system_transaction(epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_config = Persistence.encode_for_storage(cluster_config, cluster)

    encoded_layout =
      Persistence.encode_transaction_system_layout_for_storage(transaction_system_layout, cluster)

    tx = Tx.new()
    tx = build_monolithic_keys(tx, epoch, encoded_config, encoded_layout)
    tx = build_decomposed_keys(tx, epoch, cluster_config, transaction_system_layout, cluster)

    tx |> Tx.commit_binary()
  end

  @spec build_monolithic_keys(Tx.t(), Bedrock.epoch(), map(), map()) :: Tx.t()
  defp build_monolithic_keys(tx, epoch, encoded_config, encoded_layout) do
    tx
    |> Tx.set(SystemKeys.config_monolithic(), :erlang.term_to_binary({epoch, encoded_config}))
    |> Tx.set(SystemKeys.epoch_legacy(), :erlang.term_to_binary(epoch))
    |> Tx.set(
      SystemKeys.last_recovery_legacy(),
      :erlang.term_to_binary(System.system_time(:millisecond))
    )
    |> Tx.set(SystemKeys.layout_monolithic(), :erlang.term_to_binary(encoded_layout))
  end

  @spec build_decomposed_keys(
          Tx.t(),
          Bedrock.epoch(),
          Config.t(),
          TransactionSystemLayout.t(),
          module()
        ) ::
          Tx.t()
  defp build_decomposed_keys(tx, epoch, cluster_config, transaction_system_layout, cluster) do
    encoded_sequencer = encode_component_for_storage(transaction_system_layout.sequencer, cluster)
    encoded_proxies = encode_components_for_storage(transaction_system_layout.proxies, cluster)

    encoded_resolvers =
      encode_components_for_storage(transaction_system_layout.resolvers, cluster)

    encoded_services = encode_services_for_storage(transaction_system_layout.services, cluster)

    # Set cluster keys directly
    tx =
      tx
      |> Tx.set(
        SystemKeys.cluster_coordinators(),
        :erlang.term_to_binary(cluster_config.coordinators)
      )
      |> Tx.set(SystemKeys.cluster_epoch(), :erlang.term_to_binary(epoch))
      |> Tx.set(
        SystemKeys.cluster_policies_volunteer_nodes(),
        :erlang.term_to_binary(cluster_config.policies.allow_volunteer_nodes_to_join)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_logs(),
        :erlang.term_to_binary(cluster_config.parameters.desired_logs)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_replication(),
        :erlang.term_to_binary(cluster_config.parameters.desired_replication_factor)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_commit_proxies(),
        :erlang.term_to_binary(cluster_config.parameters.desired_commit_proxies)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_coordinators(),
        :erlang.term_to_binary(cluster_config.parameters.desired_coordinators)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_desired_read_version_proxies(),
        :erlang.term_to_binary(cluster_config.parameters.desired_read_version_proxies)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_empty_transaction_timeout_ms(),
        :erlang.term_to_binary(
          Map.get(cluster_config.parameters, :empty_transaction_timeout_ms, 1_000)
        )
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_ping_rate_in_hz(),
        :erlang.term_to_binary(cluster_config.parameters.ping_rate_in_hz)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_retransmission_rate_in_hz(),
        :erlang.term_to_binary(cluster_config.parameters.retransmission_rate_in_hz)
      )
      |> Tx.set(
        SystemKeys.cluster_parameters_transaction_window_in_ms(),
        :erlang.term_to_binary(cluster_config.parameters.transaction_window_in_ms)
      )

    # Set layout keys directly
    tx =
      tx
      |> Tx.set(SystemKeys.layout_sequencer(), :erlang.term_to_binary(encoded_sequencer))
      |> Tx.set(SystemKeys.layout_proxies(), :erlang.term_to_binary(encoded_proxies))
      |> Tx.set(SystemKeys.layout_resolvers(), :erlang.term_to_binary(encoded_resolvers))
      |> Tx.set(SystemKeys.layout_services(), :erlang.term_to_binary(encoded_services))
      |> Tx.set(
        SystemKeys.layout_director(),
        :erlang.term_to_binary(encode_component_for_storage(self(), cluster))
      )
      |> Tx.set(SystemKeys.layout_rate_keeper(), :erlang.term_to_binary(nil))
      |> Tx.set(SystemKeys.layout_id(), :erlang.term_to_binary(transaction_system_layout.id))

    # Set log keys directly
    tx =
      transaction_system_layout.logs
      |> Enum.reduce(tx, fn {log_id, log_descriptor}, tx ->
        encoded_descriptor =
          log_descriptor
          |> encode_log_descriptor_for_storage(cluster)
          |> :erlang.term_to_binary()

        Tx.set(tx, SystemKeys.layout_log(log_id), encoded_descriptor)
      end)

    # Set storage keys directly
    tx =
      transaction_system_layout.storage_teams
      |> Enum.with_index()
      |> Enum.reduce(tx, fn {storage_team, index}, tx ->
        team_id = "team_#{index}"

        encoded_team =
          storage_team
          |> encode_storage_team_for_storage(cluster)
          |> :erlang.term_to_binary()

        Tx.set(tx, SystemKeys.layout_storage_team(team_id), encoded_team)
      end)

    # Set recovery keys directly and return tx
    tx
    |> Tx.set(SystemKeys.recovery_attempt(), :erlang.term_to_binary(1))
    |> Tx.set(
      SystemKeys.recovery_last_completed(),
      System.system_time(:millisecond) |> :erlang.term_to_binary()
    )
  end

  @spec encode_component_for_storage(nil | pid() | {Bedrock.key(), pid()}, module()) ::
          nil | pid() | {Bedrock.key(), pid()}
  defp encode_component_for_storage(nil, _cluster), do: nil
  defp encode_component_for_storage(pid, _cluster) when is_pid(pid), do: pid

  defp encode_component_for_storage({start_key, pid}, _cluster) when is_pid(pid),
    do: {start_key, pid}

  defp encode_components_for_storage(components, cluster) when is_list(components),
    do: Enum.map(components, &encode_component_for_storage(&1, cluster))

  defp encode_services_for_storage(services, _cluster) when is_map(services), do: services

  @spec encode_log_descriptor_for_storage([term()], module()) :: [term()]
  defp encode_log_descriptor_for_storage(log_descriptor, _cluster) do
    # Log descriptors typically don't contain PIDs directly
    log_descriptor
  end

  @spec encode_storage_team_for_storage(map(), module()) :: map()
  defp encode_storage_team_for_storage(storage_team, _cluster) do
    # Storage team descriptors typically don't contain PIDs directly
    storage_team
  end

  @spec submit_system_transaction(
          BedrockTransaction.encoded(),
          [pid()],
          map()
        ) :: {:ok, Bedrock.version()} | {:error, :no_commit_proxies | :timeout | :unavailable}
  defp submit_system_transaction(_system_transaction, [], _context),
    do: {:error, :no_commit_proxies}

  defp submit_system_transaction(encoded_transaction, proxies, context) when is_list(proxies) do
    commit_fn = Map.get(context, :commit_transaction_fn, &CommitProxy.commit/2)

    proxies
    |> Enum.random()
    |> then(&commit_fn.(&1, encoded_transaction))
  end
end
