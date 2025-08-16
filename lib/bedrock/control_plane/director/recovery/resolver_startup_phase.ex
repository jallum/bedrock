defmodule Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhase do
  @moduledoc """
  Solves the critical concurrency control challenge by starting resolver components
  that implement MVCC conflict detection.

  Transforms resolver descriptors from vacancy creation into operational resolver
  processes that are immediately ready to handle transaction conflict detection.

  Uses round-robin distribution across resolution-capable nodes from
  `context.node_capabilities.resolution`, ensuring fault tolerance by spreading
  resolvers across different machines. Resolvers start directly in running mode
  without requiring recovery coordination.

  Stalls if no resolution-capable nodes are available or if individual resolver
  startup fails since conflict detection is fundamental to transaction isolation
  guarantees.

  See the Resolver Startup section in `docs/knowlege_base/02-deep/recovery-narrative.md`
  for detailed explanation of the concurrency control problem and mapping algorithms.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.DataPlane.Resolver

  @impl true
  def execute(recovery_attempt, context) do
    start_supervised_fn =
      Map.get(context, :start_supervised_fn, fn child_spec, node ->
        sup_otp_name = recovery_attempt.cluster.otp_name(:sup)
        starter_fn = Shared.starter_for(sup_otp_name)
        starter_fn.(child_spec, node)
      end)

    available_resolver_nodes = Map.get(context.node_capabilities, :resolution, [])

    resolver_context = %{
      resolvers: recovery_attempt.resolvers,
      epoch: recovery_attempt.epoch,
      available_nodes: available_resolver_nodes,
      start_supervised_fn: start_supervised_fn,
      lock_token: context.lock_token
    }

    define_resolvers(resolver_context)
    |> case do
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}

      {:ok, resolvers} ->
        updated_recovery_attempt = %{recovery_attempt | resolvers: resolvers}

        {updated_recovery_attempt,
         Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase}
    end
  end

  @spec define_resolvers(%{
          resolvers: [ResolverDescriptor.t()],
          epoch: Bedrock.epoch(),
          available_nodes: [node()],
          start_supervised_fn: (Supervisor.child_spec(), node() ->
                                  {:ok, pid()} | {:error, term()}),
          lock_token: Bedrock.lock_token()
        }) ::
          {:ok, [{start_key :: Bedrock.key(), resolver :: pid()}]}
          | {:error, {:failed_to_start, :resolver, node(), reason :: term()}}
  def define_resolvers(context) do
    resolver_boot_info =
      context.resolvers
      |> generate_resolver_ranges()
      |> Enum.map(fn [start_key, end_key] ->
        key_range = {start_key, end_key}
        {child_spec_for_resolver(context.epoch, key_range, context.lock_token), start_key}
      end)

    start_resolvers(
      resolver_boot_info,
      context.available_nodes,
      context.start_supervised_fn
    )
  end

  @spec generate_resolver_ranges([ResolverDescriptor.t()]) :: [[Bedrock.key() | :end]]
  defp generate_resolver_ranges(resolvers) do
    resolvers
    |> Enum.map(& &1.start_key)
    |> Enum.sort()
    |> Enum.concat([:end])
    |> Enum.chunk_every(2, 1, :discard)
  end

  @spec start_resolvers(
          resolver_boot_info :: [
            {Supervisor.child_spec(), start_key :: Bedrock.key()}
          ],
          available_nodes :: [node()],
          start_supervised :: (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()})
        ) ::
          {:ok, [{start_key :: Bedrock.key(), resolver :: pid()}]}
          | {:error, {:failed_to_start, :resolver, node(), reason :: term()}}
  def start_resolvers(
        resolver_boot_info,
        available_nodes,
        start_supervised
      ) do
    available_nodes
    |> Stream.cycle()
    |> Enum.zip(resolver_boot_info)
    |> Task.async_stream(
      fn {node, {child_spec, start_key}} ->
        case start_supervised.(child_spec, node) do
          {:ok, resolver} -> {node, {start_key, resolver}}
          {:error, reason} -> {node, {:error, reason}}
        end
      end,
      ordered: false
    )
    |> Enum.reduce_while([], fn
      {:ok, {_node, {start_key, pid}}}, resolvers when is_pid(pid) ->
        {:cont, [{start_key, pid} | resolvers]}

      {:ok, {node, {:error, reason}}}, _ ->
        {:halt, {:error, {:failed_to_start, :resolver, node, reason}}}

      {:exit, {node, reason}}, _ ->
        {:halt, {:error, {:failed_to_start, :resolver, node, reason}}}
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      resolvers -> {:ok, resolvers |> Enum.sort_by(&elem(&1, 0))}
    end
  end

  @spec child_spec_for_resolver(
          epoch :: Bedrock.epoch(),
          key_range :: Bedrock.key_range(),
          lock_token :: Bedrock.lock_token()
        ) ::
          Supervisor.child_spec()
  def child_spec_for_resolver(epoch, key_range, lock_token) do
    Resolver.child_spec(lock_token: lock_token, epoch: epoch, key_range: key_range)
  end
end
