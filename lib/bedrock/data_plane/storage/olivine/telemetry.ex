defmodule Bedrock.DataPlane.Storage.Olivine.Telemetry do
  @moduledoc false
  alias Bedrock.Telemetry

  @spec trace_fetch_start(Bedrock.key(), Bedrock.version()) :: :ok
  def trace_fetch_start(key, version) do
    Telemetry.execute([:olivine, :fetch, :start], %{}, %{
      key: key,
      version: version
    })
  end

  @spec trace_fetch_stop(Bedrock.key(), Bedrock.version(), term()) :: :ok
  def trace_fetch_stop(key, version, result) do
    Telemetry.execute([:olivine, :fetch, :stop], %{}, %{
      key: key,
      version: version,
      result: result
    })
  end

  @spec trace_range_fetch_start(Bedrock.key(), Bedrock.key(), Bedrock.version()) :: :ok
  def trace_range_fetch_start(start_key, end_key, version) do
    Telemetry.execute([:olivine, :range_fetch, :start], %{}, %{
      start_key: start_key,
      end_key: end_key,
      version: version
    })
  end

  @spec trace_range_fetch_stop(Bedrock.key(), Bedrock.key(), Bedrock.version(), term()) :: :ok
  def trace_range_fetch_stop(start_key, end_key, version, result) do
    Telemetry.execute([:olivine, :range_fetch, :stop], %{}, %{
      start_key: start_key,
      end_key: end_key,
      version: version,
      result: result
    })
  end

  @spec trace_transaction_applied(Bedrock.version(), non_neg_integer()) :: :ok
  def trace_transaction_applied(version, n_mutations) do
    Telemetry.execute([:olivine, :transaction, :applied], %{}, %{
      version: version,
      n_mutations: n_mutations
    })
  end

  @spec trace_page_split(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def trace_page_split(old_page_id, new_left_id, new_right_id) do
    Telemetry.execute([:olivine, :page, :split], %{}, %{
      old_id: old_page_id,
      new_left_id: new_left_id,
      new_right_id: new_right_id
    })
  end

  @spec trace_persistent_error(term()) :: :ok
  def trace_persistent_error(error) do
    Telemetry.execute([:olivine, :error, :persistent], %{}, %{
      error: error
    })
  end

  @spec trace_log_pull_start(Bedrock.version(), Bedrock.version()) :: :ok
  def trace_log_pull_start(timestamp_version, next_version) do
    Telemetry.execute([:olivine, :storage, :pull_start], %{}, %{
      timestamp: timestamp_version,
      next_version: next_version
    })
  end

  @spec trace_log_pull_succeeded(Bedrock.version(), non_neg_integer()) :: :ok
  def trace_log_pull_succeeded(timestamp_version, n_transactions) do
    Telemetry.execute([:olivine, :storage, :pull_succeeded], %{}, %{
      timestamp: timestamp_version,
      n_transactions: n_transactions
    })
  end

  @spec trace_log_pull_failed(Bedrock.version(), term()) :: :ok
  def trace_log_pull_failed(timestamp_version, reason) do
    Telemetry.execute([:olivine, :storage, :pull_failed], %{}, %{
      timestamp: timestamp_version,
      reason: reason
    })
  end
end
