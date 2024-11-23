defmodule Bedrock.Cluster.Bootstrap.Telemetry do
  alias Bedrock.Telemetry

  def execute_started(cluster) do
    Telemetry.execute(
      [:bedrock, :node, :bootstrap, :started],
      %{},
      %{cluster: cluster}
    )
  end

  def execute_completed() do
    Telemetry.execute(
      [:bedrock, :node, :bootstrap, :completed],
      %{},
      %{}
    )
  end

  def execute_stalled(reason) do
    Telemetry.execute(
      [:bedrock, :node, :bootstrap, :stalled],
      %{},
      %{reason: reason}
    )
  end
end
