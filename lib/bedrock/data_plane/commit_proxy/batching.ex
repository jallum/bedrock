defmodule Bedrock.DataPlane.CommitProxy.Batching do
  @moduledoc false

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.State

  import Bedrock.DataPlane.Sequencer, only: [next_commit_version: 1]

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [new_batch: 3, add_transaction: 3, set_finalized_at: 2]

  @spec timestamp() :: Bedrock.timestamp_in_ms()
  defp timestamp, do: :erlang.monotonic_time(:millisecond)

  @spec single_transaction_batch(
          state :: State.t(),
          transaction :: Bedrock.transaction(),
          reply_fn :: Batch.reply_fn()
        ) ::
          {:ok, Batch.t()}
          | {:error, :sequencer_unavailable}
  def single_transaction_batch(t, transaction, reply_fn \\ fn _result -> :ok end)

  def single_transaction_batch(
        %{transaction_system_layout: %{sequencer: nil}},
        _transaction,
        _reply_fn
      ),
      do: {:error, :sequencer_unavailable}

  def single_transaction_batch(state, transaction, reply_fn) do
    case next_commit_version(state.transaction_system_layout.sequencer) do
      {:ok, last_commit_version, commit_version} ->
        {:ok,
         new_batch(timestamp(), last_commit_version, commit_version)
         |> add_transaction(transaction, reply_fn)
         |> set_finalized_at(timestamp())}

      {:error, :unavailable} ->
        {:error, :sequencer_unavailable}
    end
  end

  @spec start_batch_if_needed(State.t()) :: State.t() | no_return()
  def start_batch_if_needed(%{batch: nil} = t) do
    case next_commit_version(t.transaction_system_layout.sequencer) do
      {:ok, last_commit_version, commit_version} ->
        %{t | batch: new_batch(timestamp(), last_commit_version, commit_version)}

      {:error, reason} ->
        # Sequencer not available - this is a system error that should propagate
        exit({:sequencer_unavailable, reason})
    end
  end

  def start_batch_if_needed(t), do: t

  @spec add_transaction_to_batch(State.t(), BedrockTransaction.encoded(), Batch.reply_fn()) ::
          State.t()
  def add_transaction_to_batch(t, transaction, reply_fn),
    do: %{t | batch: t.batch |> add_transaction(transaction, reply_fn)}

  @spec apply_finalization_policy(State.t()) ::
          {State.t(), batch_to_finalize :: Batch.t()} | {State.t(), nil}
  def apply_finalization_policy(t) do
    now = timestamp()

    if max_latency?(t.batch, now, t.max_latency_in_ms) or
         max_transactions?(t.batch, t.max_per_batch) do
      {%{t | batch: nil}, t.batch |> set_finalized_at(now)}
    else
      {t, nil}
    end
  end

  @spec max_latency?(
          Batch.t(),
          now :: Bedrock.timestamp_in_ms(),
          max_latency_in_ms :: pos_integer()
        ) :: boolean()
  defp max_latency?(batch, now, max_latency_in_ms),
    do: batch.started_at + max_latency_in_ms < now

  @spec max_transactions?(Batch.t(), max_per_batch :: pos_integer()) :: boolean()
  defp max_transactions?(batch, max_per_batch),
    do: batch.n_transactions >= max_per_batch
end
