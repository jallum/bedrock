defmodule Bedrock.DataPlane.Storage.Telemetry do
  @moduledoc """
  Telemetry utilities for storage operations.
  """

  @type telemetry_metadata :: %{optional(atom()) => term()}

  @spec trace_metadata() :: telemetry_metadata()
  def trace_metadata, do: Process.get(:trace_metadata, %{})

  @spec trace_metadata(metadata :: telemetry_metadata()) :: telemetry_metadata()
  def trace_metadata(metadata), do: Process.put(:trace_metadata, Enum.into(metadata, trace_metadata()))

  @spec emit_storage_operation(atom(), map(), map()) :: :ok
  def emit_storage_operation(operation, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(
      [:bedrock, :storage, operation],
      measurements,
      Map.merge(trace_metadata(), metadata)
    )
  end

  # Trace functions used by pulling modules
  @spec trace_log_pull_start(term(), term()) :: :ok
  def trace_log_pull_start(start_after, _start_after),
    do: emit_storage_operation(:pull_start, %{}, %{start_after: start_after})

  @spec trace_log_pull_succeeded(term(), integer()) :: :ok
  def trace_log_pull_succeeded(start_after, count),
    do: emit_storage_operation(:pull_succeeded, %{count: count}, %{start_after: start_after})

  @spec trace_log_pull_failed(term(), term()) :: :ok
  def trace_log_pull_failed(start_after, reason),
    do: emit_storage_operation(:pull_failed, %{}, %{start_after: start_after, reason: reason})

  @spec trace_log_pull_circuit_breaker_tripped(term(), integer()) :: :ok
  def trace_log_pull_circuit_breaker_tripped(start_after, ms_to_wait),
    do: emit_storage_operation(:circuit_breaker_tripped, %{wait_ms: ms_to_wait}, %{start_after: start_after})

  @spec trace_log_marked_as_failed(term(), String.t()) :: :ok
  def trace_log_marked_as_failed(start_after, log_id),
    do: emit_storage_operation(:log_marked_failed, %{}, %{start_after: start_after, log_id: log_id})

  @spec trace_log_pull_circuit_breaker_reset(term()) :: :ok
  def trace_log_pull_circuit_breaker_reset(start_after),
    do: emit_storage_operation(:circuit_breaker_reset, %{}, %{start_after: start_after})

  # Server lifecycle trace functions
  @spec trace_startup_start() :: :ok
  def trace_startup_start, do: emit_storage_operation(:startup_start, %{}, %{})

  @spec trace_startup_complete() :: :ok
  def trace_startup_complete, do: emit_storage_operation(:startup_complete, %{}, %{})

  @spec trace_startup_failed(term()) :: :ok
  def trace_startup_failed(reason), do: emit_storage_operation(:startup_failed, %{}, %{reason: reason})

  @spec trace_shutdown_start(term()) :: :ok
  def trace_shutdown_start(reason), do: emit_storage_operation(:shutdown_start, %{}, %{reason: reason})

  @spec trace_shutdown_complete() :: :ok
  def trace_shutdown_complete, do: emit_storage_operation(:shutdown_complete, %{}, %{})

  @spec trace_shutdown_waiting(integer()) :: :ok
  def trace_shutdown_waiting(task_count), do: emit_storage_operation(:shutdown_waiting, %{task_count: task_count}, %{})

  @spec trace_shutdown_timeout(integer()) :: :ok
  def trace_shutdown_timeout(task_count), do: emit_storage_operation(:shutdown_timeout, %{task_count: task_count}, %{})

  # Transaction processing trace functions
  @spec trace_transactions_queued(integer(), integer()) :: :ok
  def trace_transactions_queued(transaction_count, queue_size),
    do:
      emit_storage_operation(:transactions_queued, %{transaction_count: transaction_count, queue_size: queue_size}, %{})

  @spec trace_transaction_processing_complete(integer(), integer()) :: :ok
  def trace_transaction_processing_complete(batch_size, duration_microseconds),
    do: trace_transaction_processing_complete(batch_size, duration_microseconds, 0)

  @spec trace_transaction_processing_complete(integer(), integer(), integer()) :: :ok
  def trace_transaction_processing_complete(batch_size, duration_microseconds, batch_size_bytes) do
    emit_storage_operation(
      :transaction_processing_complete,
      %{batch_size: batch_size, duration_μs: duration_microseconds, batch_size_bytes: batch_size_bytes},
      %{}
    )
  end

  @spec trace_transaction_timeout_scheduled() :: :ok
  def trace_transaction_timeout_scheduled, do: emit_storage_operation(:transaction_timeout_scheduled, %{}, %{})

  # Read operation trace functions
  @spec trace_read_operation_complete(term(), term(), keyword()) :: :ok
  def trace_read_operation_complete(operation, key, opts) do
    measurements = %{
      total_duration_μs: Keyword.fetch!(opts, :total_duration_μs),
      wait_duration_μs: Keyword.get(opts, :wait_duration_μs, 0),
      task_duration_μs: Keyword.get(opts, :task_duration_μs, 0)
    }

    metadata = %{
      operation: operation,
      key: key,
      was_waitlisted: Keyword.get(opts, :was_waitlisted, false),
      was_async: Keyword.get(opts, :was_async, false),
      result: Keyword.get(opts, :result, :unknown)
    }

    emit_storage_operation(:read_operation_complete, measurements, metadata)
  end
end
