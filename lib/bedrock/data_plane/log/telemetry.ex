defmodule Bedrock.DataPlane.Log.Telemetry do
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log
  alias Bedrock.Telemetry

  @spec trace_metadata() :: map()
  def trace_metadata, do: Process.get(:trace_metadata, %{})

  @spec trace_metadata(metadata :: map()) :: map()
  def trace_metadata(metadata),
    do: Process.put(:trace_metadata, Enum.into(metadata, trace_metadata()))

  @spec trace_started() :: :ok
  def trace_started,
    do: Telemetry.execute([:bedrock, :log, :started], %{}, trace_metadata())

  @spec trace_lock_for_recovery(epoch :: Bedrock.epoch()) :: :ok
  def trace_lock_for_recovery(epoch) do
    Telemetry.execute(
      [:bedrock, :log, :lock_for_recovery],
      %{},
      Map.merge(trace_metadata(), %{
        epoch: epoch
      })
    )
  end

  @spec trace_recover_from(
          source_log :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) :: :ok
  def trace_recover_from(source_log, first_version, last_version) do
    Telemetry.execute(
      [:bedrock, :log, :recover_from],
      %{},
      Map.merge(trace_metadata(), %{
        source_log: source_log,
        first_version: first_version,
        last_version: last_version
      })
    )
  end

  @spec trace_push_transaction(transaction :: BedrockTransaction.encoded()) :: :ok
  def trace_push_transaction(transaction) when is_binary(transaction) do
    Telemetry.execute(
      [:bedrock, :log, :push],
      %{},
      Map.merge(trace_metadata(), %{
        transaction: transaction
      })
    )
  end

  @spec trace_push_out_of_order(
          expected_version :: Bedrock.version(),
          current_version :: Bedrock.version()
        ) :: :ok
  def trace_push_out_of_order(expected_version, current_version) do
    Telemetry.execute(
      [:bedrock, :log, :push_out_of_order],
      %{},
      Map.merge(trace_metadata(), %{
        expected_version: expected_version,
        current_version: current_version
      })
    )
  end

  @spec trace_pull_transactions(from_version :: Bedrock.version(), opts :: Keyword.t()) :: :ok
  def trace_pull_transactions(from_version, opts) do
    Telemetry.execute(
      [:bedrock, :log, :pull],
      %{},
      Map.merge(trace_metadata(), %{
        from_version: from_version,
        opts: opts
      })
    )
  end
end
