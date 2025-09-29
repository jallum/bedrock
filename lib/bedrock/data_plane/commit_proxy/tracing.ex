defmodule Bedrock.DataPlane.CommitProxy.Tracing do
  @moduledoc false

  import Bedrock.Internal.Time.Interval, only: [humanize: 1]

  alias Bedrock.DataPlane.Version

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_commit_proxy"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :data_plane, :commit_proxy, :start],
        [:bedrock, :data_plane, :commit_proxy, :stop],
        [:bedrock, :data_plane, :commit_proxy, :failed],
        [:bedrock, :commit_proxy, :resolver, :retry],
        [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(list(atom()), map(), map(), term()) :: :ok
  def handler([:bedrock, :data_plane, :commit_proxy, event], measurements, metadata, _),
    do: trace(event, measurements, metadata)

  def handler([:bedrock, :commit_proxy, :resolver, event], measurements, metadata, _),
    do: trace_resolver(event, measurements, metadata)

  @spec trace(atom(), map(), map()) :: :ok
  def trace(:start, %{n_transactions: n_transactions}, %{cluster: cluster, commit_version: commit_version}) do
    Logger.metadata(cluster: cluster)

    info("Transaction Batch #{Version.to_string(commit_version)} started with #{n_transactions} transactions")
  end

  def trace(:stop, %{n_aborts: n_aborts, n_oks: n_oks, duration_μs: duration_μs}, %{commit_version: commit_version}) do
    info(
      "Transaction Batch #{Version.to_string(commit_version)} completed with #{n_aborts} aborts and #{n_oks} oks in #{humanize({:microsecond, duration_μs})}"
    )
  end

  def trace(:failed, %{duration_μs: duration_μs, commit_version: commit_version}, %{reason: reason}) do
    error(
      "Transaction Batch #{Version.to_string(commit_version)} failed (#{inspect(reason)}) in #{humanize({:microsecond, duration_μs})}"
    )
  end

  @spec trace_resolver(atom(), map(), map()) :: :ok
  def trace_resolver(:retry, %{attempts_remaining: attempts_remaining, attempts_used: attempts_used}, %{reason: reason}) do
    Logger.warning(
      "Resolver retry #{attempts_used + 1}, #{attempts_remaining} attempts remaining due to: #{inspect(reason)}",
      ansi_color: :yellow
    )
  end

  def trace_resolver(:max_retries_exceeded, %{total_attempts: total_attempts}, %{reason: reason}) do
    error("Resolver failed after #{total_attempts} attempts: #{inspect(reason)}")
  end

  @spec emit_resolver_retry(non_neg_integer(), non_neg_integer(), term()) :: :ok
  def emit_resolver_retry(attempts_remaining, attempts_used, reason) do
    :telemetry.execute(
      [:bedrock, :commit_proxy, :resolver, :retry],
      %{attempts_remaining: attempts_remaining, attempts_used: attempts_used},
      %{reason: reason}
    )
  end

  @spec emit_resolver_max_retries_exceeded(non_neg_integer(), term()) :: :ok
  def emit_resolver_max_retries_exceeded(total_attempts, reason) do
    :telemetry.execute(
      [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded],
      %{total_attempts: total_attempts},
      %{reason: reason}
    )
  end

  @spec info(String.t()) :: :ok
  defp info(message) do
    metadata = Logger.metadata()
    cluster = Keyword.fetch!(metadata, :cluster)

    Logger.info("Bedrock [#{cluster.name()}]: #{message}",
      ansi_color: :magenta
    )
  end

  @spec error(String.t()) :: :ok
  defp error(message) do
    metadata = Logger.metadata()
    cluster = Keyword.fetch!(metadata, :cluster)

    Logger.error("Bedrock [#{cluster.name()}]: #{message}",
      ansi_color: :red
    )
  end
end
