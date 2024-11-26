defmodule Bedrock.DataPlane.CommitProxy.Tracing do
  @moduledoc false

  import Bedrock.Internal.Time.Interval, only: [humanize: 1]

  require Logger

  defp handler_id, do: "bedrock_trace_commit_proxy"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :data_plane, :commit_proxy, :start],
        [:bedrock, :data_plane, :commit_proxy, :stop],
        [:bedrock, :data_plane, :commit_proxy, :failed]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def handler([:bedrock, :data_plane, :commit_proxy, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  def trace(:start, %{n_transactions: n_transactions}, %{
        cluster: cluster,
        commit_version: commit_version
      }) do
    Logger.metadata(cluster: cluster)
    info("Transaction Batch #{commit_version} started with #{n_transactions} transactions")
  end

  def trace(:stop, %{n_aborts: n_aborts, n_oks: n_oks, duration_us: duration_us}, %{
        commit_version: commit_version
      }) do
    info(
      "Transaction Batch #{commit_version} completed with #{n_aborts} aborts and #{n_oks} oks in #{humanize({:microsecond, duration_us})}"
    )
  end

  def trace(:failed, %{duration_us: duration_us}, %{
        reason: reason,
        commit_version: commit_version
      }) do
    error(
      "Transaction Batch #{commit_version} failed (#{inspect(reason)}) in #{humanize({:microsecond, duration_us})}"
    )
  end

  defp info(message) do
    metadata = Logger.metadata()

    Logger.info("Bedrock [#{metadata[:cluster].name()}}]: #{message}",
      ansi_color: :magenta
    )
  end

  defp error(message) do
    metadata = Logger.metadata()

    Logger.error("Bedrock [#{metadata[:cluster].name()}]: #{message}",
      ansi_color: :red
    )
  end
end
