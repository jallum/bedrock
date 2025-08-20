defmodule Bedrock.DataPlane.CommitProxy.Server do
  @moduledoc """
  GenServer implementation of the Commit Proxy.

  ## Overview

  The Commit Proxy batches transaction requests from clients to optimize throughput while
  maintaining strict consistency guarantees. It coordinates with resolvers for conflict
  detection and logs for durable persistence.

  ## Lifecycle

  1. **Initialization**: Starts in `:locked` mode, waiting for recovery completion
  2. **Recovery**: Director calls `recover_from/3` to provide transaction system layout and unlock
  3. **Transaction Processing**: Accepts `:commit` calls, batches transactions, and finalizes
  4. **Empty Transaction Timeout**: Creates empty transactions during quiet periods to advance read versions

  ## Batching Strategy

  - **Size-based**: Batches finalize when reaching `max_per_batch` transactions
  - **Time-based**: Batches finalize after `max_latency_in_ms` milliseconds
  - **Immediate**: Single transactions may bypass batching for low-latency processing

  ## Timeout Mechanisms

  - **Fast timeout (0ms)**: Allows GenServer to process any queued `:commit` messages before
    finalizing the current batch, ensuring optimal batching efficiency
  - **Empty transaction timeout**: Creates empty `{nil, %{}}` transactions during quiet periods
    to keep read versions advancing and provide system health checking

  ## Error Handling

  Uses fail-fast recovery model where unrecoverable errors (sequencer unavailable, log failures)
  trigger process exit and Director-coordinated cluster recovery.
  """

  use GenServer

  import Bedrock.DataPlane.CommitProxy.Batching,
    only: [
      start_batch_if_needed: 1,
      add_transaction_to_batch: 3,
      apply_finalization_policy: 1,
      single_transaction_batch: 2
    ]

  import Bedrock.DataPlane.CommitProxy.Finalization, only: [finalize_batch: 3]

  import Bedrock.DataPlane.CommitProxy.Telemetry,
    only: [
      trace_metadata: 1,
      trace_commit_proxy_batch_started: 3,
      trace_commit_proxy_batch_finished: 4,
      trace_commit_proxy_batch_failed: 3
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Time

  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            director: pid(),
            epoch: Bedrock.epoch(),
            lock_token: Bedrock.lock_token(),
            max_latency_in_ms: non_neg_integer(),
            max_per_batch: pos_integer(),
            empty_transaction_timeout_ms: non_neg_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    max_latency_in_ms = opts[:max_latency_in_ms] || 1
    max_per_batch = opts[:max_per_batch] || 10
    empty_transaction_timeout_ms = opts[:empty_transaction_timeout_ms] || 1_000

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, director, epoch, max_latency_in_ms, max_per_batch, empty_transaction_timeout_ms, lock_token}
         ]},
      restart: :temporary
    }
  end

  @impl true
  @spec init({module(), pid(), Bedrock.epoch(), non_neg_integer(), pos_integer(), non_neg_integer(), binary()}) ::
          {:ok, State.t(), timeout()}
  def init({cluster, director, epoch, max_latency_in_ms, max_per_batch, empty_transaction_timeout_ms, lock_token}) do
    trace_metadata(%{cluster: cluster, pid: self()})

    state =
      %State{
        cluster: cluster,
        director: director,
        epoch: epoch,
        max_latency_in_ms: max_latency_in_ms,
        max_per_batch: max_per_batch,
        empty_transaction_timeout_ms: empty_transaction_timeout_ms,
        lock_token: lock_token
      }

    {:ok, state, empty_transaction_timeout_ms}
  end

  @impl true
  @spec terminate(term(), State.t()) :: :ok
  def terminate(_reason, t) do
    abort_current_batch(t)
    :ok
  end

  @impl true
  @spec handle_call(
          {:recover_from, binary(), map()} | {:commit, Bedrock.transaction()},
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), timeout() | {:continue, term()}}
  def handle_call({:recover_from, lock_token, transaction_system_layout}, _from, %{mode: :locked} = t) do
    case t.lock_token do
      ^lock_token ->
        reply(%{t | transaction_system_layout: transaction_system_layout, mode: :running}, :ok)

      _ ->
        reply(t, {:error, :unauthorized})
    end
  end

  def handle_call({:commit, transaction}, _from, %{mode: :running, transaction_system_layout: nil} = t)
      when is_binary(transaction) do
    reply(t, {:error, :no_transaction_system_layout})
  end

  def handle_call({:commit, transaction}, from, %{mode: :running} = t) when is_binary(transaction) do
    case start_batch_if_needed(t) do
      {:error, reason} ->
        GenServer.reply(from, {:error, :abort})
        exit(reason)

      updated_t ->
        updated_t
        |> add_transaction_to_batch(transaction, reply_fn(from))
        |> apply_finalization_policy()
        |> case do
          {t, nil} -> noreply(t, timeout: 0)
          {t, batch} -> noreply(t, continue: {:finalize, batch})
        end
    end
  end

  def handle_call({:commit, _transaction}, _from, %{mode: :locked} = t), do: reply(t, {:error, :locked})

  @impl true
  @spec handle_info(:timeout, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, term()}}
  def handle_info(:timeout, %{batch: nil, mode: :running} = t) do
    empty_transaction = Transaction.encode(%{mutations: []})

    case single_transaction_batch(t, empty_transaction) do
      {:ok, batch} ->
        noreply(t, continue: {:finalize, batch})

      {:error, :sequencer_unavailable} ->
        exit({:sequencer_unavailable, :timeout_empty_transaction})
    end
  end

  def handle_info(:timeout, %{batch: nil} = t), do: noreply(t, timeout: t.empty_transaction_timeout_ms)

  def handle_info(:timeout, %{batch: batch} = t), do: noreply(%{t | batch: nil}, continue: {:finalize, batch})

  @impl true
  @spec handle_continue({:finalize, Batch.t()}, State.t()) :: {:noreply, State.t()}
  def handle_continue({:finalize, batch}, t) do
    trace_commit_proxy_batch_started(batch.commit_version, length(batch.buffer), Time.now_in_ms())

    case :timer.tc(fn -> finalize_batch(batch, t.transaction_system_layout, epoch: t.epoch) end) do
      {n_usec, {:ok, n_aborts, n_oks}} ->
        trace_commit_proxy_batch_finished(batch.commit_version, n_aborts, n_oks, n_usec)
        maybe_set_empty_transaction_timeout(t)

      {n_usec, {:error, {:log_failures, errors}}} ->
        trace_commit_proxy_batch_failed(batch, {:log_failures, errors}, n_usec)
        exit({:log_failures, errors})

      {n_usec, {:error, {:insufficient_acknowledgments, count, required, errors}}} ->
        trace_commit_proxy_batch_failed(
          batch,
          {:insufficient_acknowledgments, count, required, errors},
          n_usec
        )

        exit({:insufficient_acknowledgments, count, required, errors})

      {n_usec, {:error, {:resolver_unavailable, reason}}} ->
        trace_commit_proxy_batch_failed(batch, {:resolver_unavailable, reason}, n_usec)
        exit({:resolver_unavailable, reason})

      {n_usec, {:error, {:storage_team_coverage_error, key}}} ->
        trace_commit_proxy_batch_failed(batch, {:storage_team_coverage_error, key}, n_usec)
        exit({:storage_team_coverage_error, key})

      {n_usec, {:error, reason}} ->
        trace_commit_proxy_batch_failed(batch, reason, n_usec)
        exit({:unknown_error, reason})
    end
  end

  @spec reply_fn(GenServer.from()) :: Batch.reply_fn()
  def reply_fn(from), do: &GenServer.reply(from, &1)

  @spec maybe_set_empty_transaction_timeout(State.t()) :: {:noreply, State.t()}
  defp maybe_set_empty_transaction_timeout(%{mode: :running} = t),
    do: noreply(t, timeout: t.empty_transaction_timeout_ms)

  defp maybe_set_empty_transaction_timeout(t), do: noreply(t)

  @spec abort_current_batch(State.t()) :: :ok
  defp abort_current_batch(%{batch: nil}), do: :ok

  defp abort_current_batch(%{batch: batch}) do
    batch
    |> Batch.all_callers()
    |> Enum.each(fn reply_fn -> reply_fn.({:error, :abort}) end)
  end

  @doc """
  Updates the transaction system layout if the transaction contains system layout information.

  This function extracts transaction system layout from system transactions that contain
  the layout key and updates the commit proxy's state accordingly.
  """
  @spec maybe_update_layout_from_transaction(State.t(), Bedrock.transaction()) :: State.t()
  def maybe_update_layout_from_transaction(state, {_reads, writes}) when is_map(writes) do
    layout_key = "\xff/system/transaction_system_layout"

    case Map.get(writes, layout_key) do
      nil ->
        state

      encoded_layout when is_binary(encoded_layout) ->
        try do
          layout = :erlang.binary_to_term(encoded_layout)
          %{state | transaction_system_layout: layout}
        rescue
          _ ->
            state
        end

      _ ->
        state
    end
  end

  def maybe_update_layout_from_transaction(state, _transaction), do: state
end
