defmodule Bedrock.DataPlane.Storage.Olivine.Tracing do
  @moduledoc false

  alias Bedrock.Internal.Time

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_storage_olivine"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :storage, :transactions_queued],
        [:bedrock, :storage, :transaction_processing_start],
        [:bedrock, :storage, :transaction_processing_complete],
        [:bedrock, :storage, :transaction_timeout_scheduled],
        [:bedrock, :storage, :read_operation_complete],
        [:bedrock, :storage, :window_advanced]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(list(atom()), map(), map(), term()) :: :ok
  def handler([:bedrock, :storage, event], measurements, metadata, _), do: log_event(event, measurements, metadata)

  @spec log_event(atom(), map(), map()) :: :ok
  def log_event(:transactions_queued, measurements, metadata) do
    info(
      "#{metadata.otp_name}: Queued #{measurements.transaction_count} transactions (queue size: #{measurements.queue_size})"
    )
  end

  def log_event(:transaction_processing_start, measurements, metadata) do
    batch_size = Map.get(measurements, :batch_size, 0)
    batch_size_bytes = Map.get(measurements, :batch_size_bytes, 0)
    otp_name = Map.get(metadata, :otp_name, "unknown")

    debug("#{otp_name}: Starting transaction processing (batch size: #{batch_size}, #{format_bytes(batch_size_bytes)})")
  end

  def log_event(:transaction_processing_complete, measurements, metadata) do
    duration_μs = Map.get(measurements, :duration_μs, 0)
    batch_size = Map.get(measurements, :batch_size, 0)
    batch_size_bytes = Map.get(measurements, :batch_size_bytes, 0)
    otp_name = Map.get(metadata, :otp_name, "unknown")

    info(
      "#{otp_name}: Completed transaction processing (batch size: #{batch_size}, #{format_bytes(batch_size_bytes)}, duration: #{Time.Interval.humanize({:microsecond, duration_μs})})"
    )
  end

  def log_event(:transaction_timeout_scheduled, _measurements, metadata) do
    otp_name = Map.get(metadata, :otp_name, "unknown")
    debug("#{otp_name}: Scheduled timeout for transaction processing")
  end

  def log_event(:read_operation_complete, measurements, metadata) do
    otp_name = Map.get(metadata, :otp_name, "unknown")
    operation = Map.get(metadata, :operation, "unknown")
    key = Map.get(metadata, :key)
    key_str = format_key(key)

    total_duration_μs = Map.get(measurements, :total_duration_μs, 0)
    wait_duration_μs = Map.get(measurements, :wait_duration_μs, 0)
    task_duration_μs = Map.get(measurements, :task_duration_μs, 0)

    info(
      "#{otp_name}: Read operation complete (operation: #{operation}, key: #{key_str}, " <>
        "total: #{Time.Interval.humanize({:microsecond, total_duration_μs})}, " <>
        "wait: #{Time.Interval.humanize({:microsecond, wait_duration_μs})}, " <>
        "task: #{Time.Interval.humanize({:microsecond, task_duration_μs})})"
    )
  end

  def log_event(:window_advanced, measurements, metadata) do
    worker_id = Map.get(metadata, :worker_id) || Map.get(metadata, :storage_id, "unknown")
    result = Map.get(metadata, :result, :unknown)
    new_durable_version = Map.get(metadata, :new_durable_version)
    duration_μs = Map.get(measurements, :duration_μs, 0)
    evicted_count = Map.get(measurements, :evicted_count, 0)
    lag_time_μs = Map.get(measurements, :lag_time_μs, 0)

    case result do
      :no_eviction ->
        debug(
          "#{worker_id}: Window advancement - no eviction needed " <>
            "(duration: #{Time.Interval.humanize({:microsecond, duration_μs})})"
        )

      :evicted ->
        window_target_version = Map.get(metadata, :window_target_version)
        data_size_in_bytes = Map.get(metadata, :data_size_in_bytes, 0)

        # Database operation metrics
        db_duration_μs = Map.get(measurements, :durable_version_duration_μs, 0)

        # Database operation breakdown
        db_insert_time_μs = Map.get(measurements, :db_insert_time_μs, 0)
        db_write_time_μs = Map.get(measurements, :db_write_time_μs, 0)

        info(
          "#{worker_id}: Window advanced - #{evicted_count} versions -> #{Bedrock.DataPlane.Version.to_string(new_durable_version)}, " <>
            "#{format_bytes(data_size_in_bytes)} data, " <>
            "target #{Bedrock.DataPlane.Version.to_string(window_target_version)}, " <>
            "lag #{Time.Interval.humanize({:microsecond, lag_time_μs})}, " <>
            "duration: #{Time.Interval.humanize({:microsecond, duration_μs})} | " <>
            "DB: " <>
            "#{Time.Interval.humanize({:microsecond, db_duration_μs})} " <>
            "(" <>
            "insert: #{Time.Interval.humanize({:microsecond, db_insert_time_μs})}, " <>
            "write: #{Time.Interval.humanize({:microsecond, db_write_time_μs})})"
        )
    end
  end

  defp format_key(key) when is_binary(key) do
    if String.printable?(key) and byte_size(key) <= 50 do
      "\"#{key}\""
    else
      hex = Base.encode16(key, case: :lower)
      "0x#{hex}"
    end
  end

  defp format_key({start_key, end_key}) do
    start_str = format_key(start_key)
    end_str = format_key(end_key)
    "{#{start_str}, #{end_str}}"
  end

  defp format_key(key), do: inspect(key)

  defp format_bytes(bytes) when bytes < 1024 do
    "#{bytes}B"
  end

  defp format_bytes(bytes) when bytes < 1024 * 1024 do
    "#{Float.round(bytes / 1024, 1)}KB"
  end

  defp format_bytes(bytes) do
    "#{Float.round(bytes / (1024 * 1024), 1)}MB"
  end

  @spec debug(String.t()) :: :ok
  defp debug(message) do
    Logger.debug("Bedrock Storage Olivine: #{message}", ansi_color: :magenta)
  end

  @spec info(String.t()) :: :ok
  defp info(message) do
    Logger.info("Bedrock Storage Olivine: #{message}", ansi_color: :magenta)
  end
end
