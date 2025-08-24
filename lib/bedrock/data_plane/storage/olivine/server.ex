defmodule Bedrock.DataPlane.Storage.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Olivine.Telemetry
  alias Bedrock.Service.Foreman

  @spec child_spec(opts :: keyword()) :: map()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    foreman = opts[:foreman] || raise "Missing :foreman option"
    id = opts[:id] || raise "Missing :id option"
    path = opts[:path] || raise "Missing :path option"

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {otp_name, foreman, id, path},
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :finish_startup}}

  @impl true

  def handle_call({:fetch, key, version, opts}, from, %State{} = t) do
    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))

    case Logic.fetch(t, key, version, fetch_opts) do
      {:ok, task_pid} ->
        t
        |> State.add_active_task(task_pid)
        |> noreply()

      {:error, :version_too_new} ->
        if wait_ms = opts[:wait_ms] do
          t
          |> Logic.add_to_waitlist({key, version}, version, reply_fn_for(from), wait_ms)
          |> noreply()
        else
          reply(t, {:error, :version_too_new})
        end

      {:error, _reason} = error ->
        reply(t, error)
    end
  end

  def handle_call({:range_fetch, start_key, end_key, version, opts}, from, %State{} = t) do
    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))

    case Logic.range_fetch(t, start_key, end_key, version, fetch_opts) do
      {:ok, task_pid} ->
        t
        |> State.add_active_task(task_pid)
        |> noreply()

      {:error, :version_too_new} ->
        if wait_ms = opts[:wait_ms] do
          t
          |> Logic.add_to_waitlist({start_key, end_key, version}, version, reply_fn_for(from), wait_ms)
          |> noreply()
        else
          reply(t, {:error, :version_too_new})
        end

      {:error, _reason} = error ->
        reply(t, error)
    end
  end

  @impl true
  def handle_call({:info, fact_names}, _from, %State{} = t), do: t |> Logic.info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    with {:ok, t} <- Logic.lock_for_recovery(t, director, epoch),
         {:ok, info} <- Logic.info(t, Storage.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  @impl true
  def handle_call({:unlock_after_recovery, durable_version, transaction_system_layout}, {_director, _}, t) do
    case Logic.unlock_after_recovery(t, durable_version, transaction_system_layout) do
      {:ok, updated_state} -> reply(updated_state, :ok)
    end
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    Telemetry.trace_startup_start(otp_name)

    case Logic.startup(otp_name, foreman, id, path) do
      {:ok, state} ->
        Telemetry.trace_startup_complete(otp_name)
        noreply(state, continue: :report_health_to_foreman)

      {:error, reason} ->
        Telemetry.trace_startup_failed(otp_name, reason)
        stop(:no_state, reason)
    end
  end

  @impl true
  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply(t)
  end

  @impl true
  def handle_info({:transactions_applied, version}, %State{} = t) do
    t
    |> Logic.notify_waiting_fetches(version)
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = t) do
    t
    |> State.remove_active_task(pid)
    |> noreply()
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, %State{} = t) do
    Telemetry.trace_shutdown_start(t.otp_name, reason)
    active_tasks = State.get_active_tasks(t)

    if MapSet.size(active_tasks) > 0 do
      wait_for_tasks(active_tasks, 5_000, t.otp_name)
    end

    Logic.shutdown(t)
    Telemetry.trace_shutdown_complete(t.otp_name)
    :ok
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  defp wait_for_tasks(tasks, timeout, otp_name) do
    Telemetry.trace_shutdown_waiting(otp_name, MapSet.size(tasks))
    do_wait_for_tasks(tasks, timeout)
  end

  defp do_wait_for_tasks(tasks, timeout) do
    cond do
      MapSet.size(tasks) == 0 ->
        :ok

      timeout <= 0 ->
        Telemetry.trace_shutdown_timeout(MapSet.size(tasks))
        :timeout

      true ->
        start_time = System.monotonic_time(:millisecond)

        receive do
          {:DOWN, _ref, :process, pid, _reason} ->
            tasks = MapSet.delete(tasks, pid)
            elapsed = System.monotonic_time(:millisecond) - start_time
            do_wait_for_tasks(tasks, timeout - elapsed)
        after
          timeout ->
            Telemetry.trace_shutdown_timeout(MapSet.size(tasks))
            :timeout
        end
    end
  end

  defp reply_fn_for(from), do: fn result -> GenServer.reply(from, result) end
end
