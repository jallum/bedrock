defmodule Bedrock.DataPlane.CommitProxy.Telemetry do
  @moduledoc false
  alias Bedrock.Telemetry

  @type telemetry_metadata :: %{optional(atom()) => term()}

  @spec trace_metadata() :: telemetry_metadata()
  def trace_metadata, do: Process.get(:trace_metadata, %{})

  @spec trace_metadata(metadata :: telemetry_metadata()) :: telemetry_metadata()
  def trace_metadata(metadata), do: Process.put(:trace_metadata, Enum.into(metadata, trace_metadata()))

  @spec trace_commit_proxy_batch_started(
          commit_version :: Bedrock.version(),
          n_transactions :: non_neg_integer(),
          started_at :: Bedrock.timestamp_in_ms()
        ) :: :ok
  def trace_commit_proxy_batch_started(commit_version, n_transactions, started_at) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :start],
      %{n_transactions: n_transactions},
      Map.merge(trace_metadata(), %{commit_version: commit_version, started_at: started_at})
    )
  end

  @spec trace_commit_proxy_batch_finished(
          commit_version :: Bedrock.version(),
          n_aborts :: non_neg_integer(),
          n_oks :: non_neg_integer(),
          duration_μs :: Bedrock.interval_in_us()
        ) :: :ok
  def trace_commit_proxy_batch_finished(commit_version, n_aborts, n_oks, duration_μs) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :stop],
      %{n_oks: n_oks, n_aborts: n_aborts, duration_μs: duration_μs},
      Map.put(trace_metadata(), :commit_version, commit_version)
    )
  end

  @spec trace_commit_proxy_batch_failed(
          batch :: Bedrock.DataPlane.CommitProxy.Batch.t(),
          reason :: any(),
          duration_μs :: Bedrock.interval_in_us()
        ) :: :ok
  def trace_commit_proxy_batch_failed(batch, reason, duration_μs) do
    Telemetry.execute(
      [:bedrock, :data_plane, :commit_proxy, :failed],
      %{
        n_transactions: length(batch.buffer),
        duration_μs: duration_μs,
        commit_version: batch.commit_version
      },
      Map.put(trace_metadata(), :reason, reason)
    )
  end
end
