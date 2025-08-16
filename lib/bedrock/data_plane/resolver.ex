defmodule Bedrock.DataPlane.Resolver do
  @moduledoc """
  MVCC conflict detection engine for Bedrock's optimistic concurrency control system.

  The Resolver detects read-write and write-write conflicts by maintaining an interval
  tree that tracks which key ranges were written at which versions. It processes
  transaction batches from Commit Proxies and returns lists of conflicting transaction
  indices to abort.

  Resolvers start in running mode and are immediately ready to process transactions.
  They handle out-of-order transactions through a version-indexed waiting queue that
  ensures consistent conflict detection regardless of network timing variations.

  For detailed conflict detection concepts and architectural integration, see the
  [Resolver documentation](../../../../docs/components/resolver.md).
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @type read_info :: {version :: Bedrock.version(), keys :: [Bedrock.key() | Bedrock.key_range()]}

  @type transaction_summary :: {
          read_info :: read_info() | nil,
          write_keys :: [Bedrock.key() | Bedrock.key_range()]
        }

  @spec resolve_transactions(
          ref(),
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [transaction_summary()],
          opts :: [timeout: Bedrock.timeout_in_ms()]
        ) ::
          {:ok, aborted :: [transaction_index :: non_neg_integer()]}
          | {:error, :timeout | :unavailable | :unknown}
  def resolve_transactions(ref, last_version, commit_version, transaction_summaries, opts \\ []) do
    call(
      ref,
      {:resolve_transactions, {last_version, commit_version}, transaction_summaries},
      opts[:timeout] || :infinity
    )
  end
end
