defmodule Bedrock.DataPlane.Storage.Basalt.Pulling do
  @moduledoc false
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Worker

  import Bedrock.DataPlane.Storage.Basalt.Telemetry

  @type puller_state :: %{
          start_after: Bedrock.version(),
          apply_transactions_fn: ([BedrockTransaction.encoded()] -> Bedrock.version()),
          get_durable_version_fn: (-> Bedrock.version()),
          flush_window_fn: (-> :ok),
          logs: %{Log.id() => LogDescriptor.t()},
          services: %{Worker.id() => ServiceDescriptor.t()},
          failed_logs: %{Log.id() => any()}
        }

  @spec start_pulling(
          start_after :: Bedrock.version(),
          logs :: %{Log.id() => LogDescriptor.t()},
          services :: %{Worker.id() => ServiceDescriptor.t()},
          apply_transactions_fn :: ([BedrockTransaction.encoded()] -> Bedrock.version()),
          get_durable_version_fn :: (-> Bedrock.version()),
          flush_window_fn :: (-> :ok)
        ) :: Task.t()
  def start_pulling(
        start_after,
        logs,
        services,
        apply_transactions_fn,
        get_durable_version_fn,
        flush_window_fn
      ) do
    state = %{
      start_after: start_after,
      apply_transactions_fn: apply_transactions_fn,
      get_durable_version_fn: get_durable_version_fn,
      flush_window_fn: flush_window_fn,
      logs: logs,
      services: services,
      failed_logs: %{}
    }

    Task.async(fn -> long_pull_loop(state) end)
  end

  @spec stop(Task.t()) :: :ok
  def stop(puller) do
    Task.shutdown(puller)
    :ok
  end

  @spec circuit_breaker_timeout() :: pos_integer()
  def circuit_breaker_timeout, do: 10_000
  @spec retry_delay() :: pos_integer()
  def retry_delay, do: 5_000
  @spec call_timeout() :: pos_integer()
  def call_timeout, do: 5_000

  @spec long_pull_loop(puller_state()) :: no_return()
  def long_pull_loop(%{apply_transactions_fn: apply_transactions_fn} = state) do
    timestamp = System.system_time(:millisecond)

    case select_log(state) do
      {:ok, {log_id, %{status: {:up, worker_pid}}}} ->
        trace_log_pull_start(timestamp, state.start_after)

        case Log.pull(worker_pid, state.start_after,
               limit: 100,
               willing_to_wait_in_ms: call_timeout(),
               subscriber: {"storage_server", state.get_durable_version_fn.()}
             ) do
          {:ok, encoded_transactions} ->
            trace_log_pull_succeeded(timestamp, length(encoded_transactions))

            next_version = apply_transactions_fn.(encoded_transactions)

            # Flush window once per pull batch
            :ok = state.flush_window_fn.()

            %{state | start_after: next_version}
            |> long_pull_loop()

          {:error, reason} ->
            trace_log_pull_failed(timestamp, reason)
            new_state = mark_log_as_failed(state, log_id)
            long_pull_loop(new_state)
        end

      :no_available_logs ->
        ms_to_wait = retry_delay()
        trace_log_pull_circuit_breaker_tripped(timestamp, ms_to_wait)
        :timer.sleep(ms_to_wait)
        long_pull_loop(reset_failed_logs(state))
    end
  end

  # Select a log, excluding those with active circuit breakers
  @spec select_log(puller_state()) ::
          {:ok, {Log.id(), ServiceDescriptor.t()}} | :no_available_logs
  def select_log(%{logs: logs, services: services, failed_logs: failed_logs}) do
    now = System.monotonic_time(:millisecond)

    available_log_services =
      logs
      |> Map.keys()
      |> Enum.filter(fn log_id ->
        case Map.get(failed_logs, log_id) do
          nil -> true
          retry_timestamp -> now >= retry_timestamp
        end
      end)
      |> Enum.map(&{&1, Map.get(services, &1)})
      |> Enum.reject(&is_nil(elem(&1, 1)))
      |> Map.new()

    if Enum.empty?(available_log_services) do
      :no_available_logs
    else
      {:ok, Enum.random(available_log_services)}
    end
  end

  # Mark a server as failed and set a retry timestamp
  @spec mark_log_as_failed(puller_state(), Log.id()) :: puller_state()
  def mark_log_as_failed(state, log_id) do
    now = System.monotonic_time(:millisecond)
    retry_timestamp = now + circuit_breaker_timeout()
    failed_logs = Map.put(state.failed_logs, log_id, retry_timestamp)

    trace_log_marked_as_failed(now, log_id)

    %{state | failed_logs: failed_logs}
  end

  # Reset all failed logs, clearing the circuit breakers
  @spec reset_failed_logs(puller_state()) :: puller_state()
  def reset_failed_logs(state) do
    now = System.monotonic_time(:millisecond)
    trace_log_pull_circuit_breaker_reset(now)

    %{state | failed_logs: %{}}
  end
end
