defmodule Bedrock.DataPlane.CommitProxy do
  @moduledoc """
  Central coordinator of Bedrock's transaction commit process.

  The Commit Proxy batches transactions from multiple clients for efficient processing,
  orchestrates conflict resolution through Resolvers, and ensures durable persistence
  across all required log servers. It transforms individual transaction requests into
  efficiently processed batches while maintaining strict consistency guarantees.

  Transaction batching creates a fundamental trade-off between latency and throughput.
  The Commit Proxy manages this through configurable size and time limits that balance
  responsiveness against processing efficiency. This batching strategy enables
  intra-batch conflict detection and amortizes the fixed costs of conflict resolution
  and logging across multiple transactions while preserving the arrival order of
  transactions within each batch.

  The component uses a fail-fast recovery model where unrecoverable errors trigger
  process exit and Director-coordinated recovery. Commit Proxies start in locked mode
  and require explicit unlocking through `recover_from/3` before accepting transaction
  commits, ensuring proper coordination during cluster recovery scenarios.

  For detailed architectural concepts and design reasoning, see the
  [Commit Proxy documentation](../../../../docs/components/commit-proxy.md).
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.BedrockTransaction

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @spec recover_from(
          commit_proxy_ref :: ref(),
          lock_token :: binary(),
          transaction_system_layout :: TransactionSystemLayout.t()
        ) :: :ok | {:error, :timeout} | {:error, :unavailable}
  def recover_from(commit_proxy, lock_token, transaction_system_layout),
    do: call(commit_proxy, {:recover_from, lock_token, transaction_system_layout}, :infinity)

  @spec commit(commit_proxy_ref :: ref(), transaction :: BedrockTransaction.encoded()) ::
          {:ok, version :: Bedrock.version()} | {:error, :timeout | :unavailable}
  def commit(commit_proxy, transaction),
    do: call(commit_proxy, {:commit, transaction}, :infinity)
end
