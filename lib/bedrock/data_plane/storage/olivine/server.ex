defmodule Bedrock.DataPlane.Storage.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.Reading
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.Service.Foreman

  # Transaction count limits for adaptive batching
  # Small batches for responsiveness during normal operation
  @continuation_batch_count 5
  # Larger batches during lulls when no reads are waiting
  @timeout_batch_count 50

  @spec child_spec(opts :: keyword()) :: map()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    foreman = opts[:foreman] || raise "Missing :foreman option"
    id = opts[:id] || raise "Missing :id option"
    path = opts[:path] || raise "Missing :path option"

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {otp_name, foreman, id, path},
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :finish_startup}}

  @impl true

  def handle_call({:get, key, version, opts}, from, %State{} = t) do
    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get, key: key})

    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} = Reading.handle_get(t.read_request_manager, context, key, version, fetch_opts)
    updated_state = %{t | read_request_manager: updated_manager}

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  def handle_call({:get_range, start_key, end_key, version, opts}, from, %State{} = t) do
    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get_range, key: {start_key, end_key}})

    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get_range(t.read_request_manager, context, start_key, end_key, version, fetch_opts)

    updated_state = %{t | read_request_manager: updated_manager}

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  @impl true
  def handle_call({:info, fact_names}, _from, %State{} = t), do: t |> Logic.info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    with {:ok, t} <- Logic.lock_for_recovery(t, director, epoch),
         {:ok, info} <- Logic.info(t, Storage.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  @impl true
  def handle_call({:unlock_after_recovery, durable_version, transaction_system_layout}, {_director, _}, t) do
    case Logic.unlock_after_recovery(t, durable_version, transaction_system_layout) do
      {:ok, updated_state} -> reply(updated_state, :ok)
    end
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    # Set persistent telemetry metadata for this server
    Telemetry.trace_metadata(%{otp_name: otp_name, storage_id: id})

    Telemetry.trace_startup_start()

    case Logic.startup(otp_name, foreman, id, path) do
      {:ok, state} ->
        Telemetry.trace_startup_complete()
        noreply(state, continue: :report_health_to_foreman)

      {:error, reason} ->
        Telemetry.trace_startup_failed(reason)
        stop(:no_state, reason)
    end
  end

  @impl true
  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply(t, continue: :process_transactions)
  end

  @impl true
  def handle_continue(:process_transactions, %State{} = t) do
    case IntakeQueue.take_batch_by_count(t.intake_queue, @continuation_batch_count) do
      {[], nil, updated_intake_queue} ->
        # Queue empty, just wait for new transactions or timeout
        updated_state = %{t | intake_queue: updated_intake_queue}
        noreply(updated_state)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = %{t | intake_queue: updated_intake_queue}
        # Process small batch for responsiveness
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        final_state = notify_waiting_fetches(state_with_txns, version)

        # Check for more transactions to process
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  @impl true
  def handle_continue(:maybe_process_transactions, %State{} = t) do
    if IntakeQueue.empty?(t.intake_queue) do
      noreply(t, timeout: 0)
    else
      noreply(t, continue: :process_transactions)
    end
  end

  @impl true
  def handle_continue(:advance_window, %State{} = t) do
    case Logic.advance_window(t) do
      {:ok, state_after_window} ->
        noreply(state_after_window)
    end
  end

  defp notify_waiting_fetches(state, version) do
    context = Reading.ReadingContext.new(state.index_manager, state.database)
    updated_manager = Reading.notify_waiting_fetches(state.read_request_manager, context, version)
    %{state | read_request_manager: updated_manager}
  end

  @impl true
  # Discard transactions when locked
  def handle_info({:apply_transactions, _encoded_transactions}, %State{mode: :locked} = t), do: noreply(t)

  @impl true
  def handle_info({:apply_transactions, encoded_transactions}, %State{} = t) do
    # Queue the transactions and start processing
    updated_intake_queue = IntakeQueue.add_transactions(t.intake_queue, encoded_transactions)
    updated_state = %{t | intake_queue: updated_intake_queue}
    queue_size = IntakeQueue.size(updated_intake_queue)
    Telemetry.trace_transactions_queued(length(encoded_transactions), queue_size)
    Telemetry.trace_transaction_timeout_scheduled()
    noreply(updated_state, continue: :process_transactions)
  end

  @impl true
  def handle_info(:timeout, %State{} = t) do
    # First, process a larger batch of transactions for throughput
    case IntakeQueue.take_batch_by_count(t.intake_queue, @timeout_batch_count) do
      {[], nil, updated_intake_queue} ->
        # No transactions to process, advance window during this lull
        updated_state = %{t | intake_queue: updated_intake_queue}
        noreply(updated_state, continue: :advance_window)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = %{t | intake_queue: updated_intake_queue}
        # Process larger batch for throughput
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        state_after_txns = notify_waiting_fetches(state_with_txns, version)

        # Now advance window after processing transactions
        {:ok, final_state} = Logic.advance_window(state_after_txns)
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  @impl true
  def handle_info({:transactions_applied, version}, %State{} = t) do
    t
    |> notify_waiting_fetches(version)
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = t) do
    updated_manager = Reading.remove_active_task(t.read_request_manager, pid)
    updated_state = %{t | read_request_manager: updated_manager}
    noreply(updated_state)
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(reason, %State{} = t) do
    Telemetry.trace_shutdown_start(reason)
    Reading.shutdown(t.read_request_manager)
    Logic.shutdown(t)
    Telemetry.trace_shutdown_complete()
    :ok
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  defp reply_fn_for(from), do: fn result -> GenServer.reply(from, result) end
end
