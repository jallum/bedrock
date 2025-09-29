defmodule Bedrock.DataPlane.Storage.Olivine.Telemetry do
  @moduledoc """
  Telemetry utilities specifically for Olivine storage operations.
  """

  alias Bedrock.DataPlane.Storage.Telemetry

  @spec trace_read_operation_complete(term(), term(), keyword()) :: :ok
  def trace_read_operation_complete(operation, key, opts) do
    Telemetry.trace_read_operation_complete(operation, key, opts)
  end

  @spec trace_index_update_complete(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def trace_index_update_complete(keys_added, keys_removed, keys_changed, total_keys) do
    total_keys_affected = keys_added + keys_removed + keys_changed

    Telemetry.emit_storage_operation(
      :index_update_complete,
      %{
        keys_added: keys_added,
        keys_removed: keys_removed,
        keys_changed: keys_changed,
        total_keys_affected: total_keys_affected,
        total_keys: total_keys
      },
      %{}
    )
  end

  @spec trace_window_advanced(atom(), term(), keyword()) :: :ok
  def trace_window_advanced(result, new_durable_version, opts \\ []) do
    measurements = %{
      duration_μs: Keyword.fetch!(opts, :duration_μs),
      evicted_count: Keyword.get(opts, :evicted_count, 0),
      lag_time_μs: Keyword.get(opts, :lag_time_μs, 0),
      # Database operation metrics
      durable_version_duration_μs: Keyword.get(opts, :durable_version_duration_μs, 0),
      # Database operation breakdown
      db_insert_time_μs: Keyword.get(opts, :db_insert_time_μs, 0),
      db_write_time_μs: Keyword.get(opts, :db_write_time_μs, 0)
    }

    metadata = %{
      result: result,
      new_durable_version: new_durable_version,
      window_target_version: Keyword.get(opts, :window_target_version),
      data_size_in_bytes: Keyword.get(opts, :data_size_in_bytes)
    }

    Telemetry.emit_storage_operation(:window_advanced, measurements, metadata)
  end
end
