defmodule Bedrock.Cluster.Bootstrap.Tracing do
  require Logger

  defp handler_id, do: "bedrock_tracing_bootstrap"

  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :node, :bootstrap, :started],
        [:bedrock, :node, :bootstrap, :completed],
        [:bedrock, :node, :bootstrap, :stalled]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  def stop, do: :telemetry.detach(handler_id())

  def handler([:bedrock, :node, :bootstrap, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  def trace(:started, _, %{cluster: cluster}) do
    Logger.metadata(cluster: cluster)

    info("Bootstrap started")
  end

  def trace(:completed, _, _),
    do: info("Bootstrapping complete")

  def trace(:stalled, _, %{reason: reason}),
    do: info("Bootstrapping stalled: #{inspect(reason)}")

  def info(message) do
    metadata = Logger.metadata()

    Logger.info("Bedrock [#{metadata[:cluster].name()}]: #{message}")
  end
end
