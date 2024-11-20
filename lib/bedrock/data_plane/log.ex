defmodule Bedrock.DataPlane.Log do
  @moduledoc """
  """

  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Transaction
  alias Bedrock.Service.Worker

  use Bedrock.Internal.GenServerApi

  @type ref :: Worker.ref()
  @type id :: Worker.id()
  @type health :: Worker.health()
  @type fact_name ::
          Worker.fact_name()
          | :last_version
          | :oldest_version
          | :minimum_durable_version

  @type recovery_info :: %{
          kind: :log,
          last_version: Bedrock.version(),
          oldest_version: Bedrock.version(),
          minimum_durable_version: Bedrock.version() | :unavailable
        }

  @doc """
  Returns information needed for recovery processes. This includes the kind of
  entity (:log) and various version markers indicating the state and durability
  of the log.

  ## Return Values:

    - A list containing:
      - `:kind`: Identifies the entity as a log.
      - `:last_version`: The latest version present in the log.
      - `:oldest_version`: The earliest version still available in the log.
      - `:minimum_durable_version`: The lowest version that is guaranteed to
        be durable. This could be a specific version or `:unavailable` if
        not determinable.

  Useful in scenarios where system recovery needs to consider the current
  state of log versions, ensuring consistency and order.
  """
  @spec recovery_info :: [fact_name()]
  def recovery_info, do: [:kind, :last_version, :oldest_version, :minimum_durable_version]

  @doc """
  Apply a new transaction to the log. The previous transaction version is given
  as a check to ensure strict ordering. If the previous transaction version is
  lower than the latest transaction, the transaction will be rejected. If it is
  greater, then the transaction will be queued for later application.

  This call will not return until the transaction has been made durable on the
  log, ensuring that the transactions that precede it are also durable.
  """
  @spec push(log :: ref(), EncodedTransaction.t(), last_commit_version :: Bedrock.version()) ::
          :ok | {:error, :tx_out_of_order | :locked | :unavailable}
  def push(log, transaction, last_commit_version),
    do: call(log, {:push, transaction, last_commit_version}, :infinity)

  @doc """
  Pull transactions from the log starting from a given version. Options allow
  specifying the maximum number of transactions to return, the last version
  considered valid, whether the operation is recovery-related, subscriber
  details to maintain state, and a timeout for the operation.

  Returns a list of transactions or an error indicating why the pull failed.

  ## Parameters:

    - `log`: Reference to the log from which transactions are to be pulled.
    - `start_after`: The version after which transactions are to be pulled.
    - `opts`: Options to tailor the behavior of the pull operation.
      - `limit`: Maximum number of transactions to return. This may be
        additionally limited by the log's configuration.
      - `last_version`: The last valid version for pulling transactions
        (inclusive).
      - `recovery`: Indicates if this pull is part of a recovery operation.
      - `subscriber`: A tuple containing an ID and the last durable version.
      - `timeout_in_ms`: Timeout for the operation in milliseconds.

  ## Return Values:

    - `{:ok, [Transaction.t()]}`: A successful pull with a list of transactions.
    - `{:error, :not_ready}`: Log is not ready for pulling.
    - `{:error, :not_locked}`: Log is not locked for pulling transactions.
    - `{:error, :invalid_from_version}`: The provided `from_version` is invalid.
    - `{:error, :invalid_last_version}`: The specified `last_version` is invalid.
    - `{:error, :version_too_new}`: The version specified is too recent.
    - `{:error, :version_too_old}`: The version specified is too old.
    - `{:error, :version_not_found}`: The version cannot be found.
    - `{:error, :unavailable}`: Log is unavailable for operation.
  """
  @spec pull(
          log :: ref(),
          start_after :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            recovery: boolean(),
            subscriber: {id :: String.t(), last_durable_version :: Bedrock.version()},
            timeout_in_ms: pos_integer()
          ]
        ) ::
          {:ok, [EncodedTransaction.t()]} | pull_errors()
  @type pull_errors ::
          {:error, :not_ready}
          | {:error, :not_locked}
          | {:error, :invalid_from_version}
          | {:error, :invalid_last_version}
          | {:error, :version_too_new}
          | {:error, :version_too_old}
          | {:error, :version_not_found}
          | {:error, :unavailable}
  def pull(log, start_after, opts),
    do: call(log, {:pull, start_after, opts}, opts[:timeout_in_ms] || :infinity)

  @doc """
  The initial transaction that is applied to a new log if the current version
  is set to 0 during a recovery. It is an explicit a directive to clear the
  entire key range.
  """
  @spec initial_transaction :: Transaction.t()
  def initial_transaction, do: Transaction.new(0, %{})

  @doc """
  Request that the transaction log worker lock itself and stop accepting new
  transactions. This mechanism is used by a newly elected cluster director
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock_for_recovery(log :: ref(), Bedrock.epoch()) ::
          {:ok, pid(), recovery_info :: keyword()} | {:error, :newer_epoch_exists}
  defdelegate lock_for_recovery(storage, epoch), to: Worker

  @doc """
  Initiates a recovery process from the given `source_log` spanning the
  transactions specified by the given first/last versions. If the first version
  is 0, a special _initial transaction_ is applied to the log. This transaction
  is defined by `initial_transaction/0`.

  The function ensures that the transaction log is consistent with the
  `source_log` by pulling from that log and applying the transactions.

  ## Parameters:

    - `log`: Reference to the target log where recovery should be applied.
    - `source_log`: Reference to the source log from which transactions are
      recovered. nil is sent for the initial recovery, since there is no
      source log.
    - `first_version`: The starting point of the recovery. Transactions _after_
      this version are applied.
    - `last_version`: The ending point of the recovery. Transactions up to and
      including this version are applied.

  ## Return Values:

    - `:ok`: Recovery was successful.
    - `{:error, :unavailable}`: The log is unavailable, and recovery cannot be
      performed.
  """
  @spec recover_from(
          log :: ref(),
          source_log :: ref() | nil,
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          :ok | {:error, :unavailable}
  def recover_from(log, source_log, first_version, last_version),
    do: call(log, {:recover_from, source_log, first_version, last_version}, :infinity)

  @doc """
  Ask the transaction log worker for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()], opts :: keyword()) ::
          {:ok, keyword()} | {:error, term()}
  defdelegate info(storage, fact_names, opts \\ []), to: Worker
end
