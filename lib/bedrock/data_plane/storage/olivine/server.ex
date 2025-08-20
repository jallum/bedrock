defmodule Bedrock.DataPlane.Storage.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.State
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
  def terminate(_reason, %State{} = state) do
    Logic.shutdown(state)
    :ok
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  @impl true
  def handle_call({:fetch, key, version, _opts}, from, %State{} = t) do
    case Logic.try_fetch_or_waitlist(t, key, version, from) do
      {:ok, value, new_state} -> reply(new_state, {:ok, value})
      {:error, reason, new_state} -> reply(new_state, {:error, reason})
      {:waitlist, new_state} -> noreply(new_state)
    end
  end

  @impl true
  def handle_call({:range_fetch, start_key, end_key, version, _opts}, from, %State{} = t) do
    case Logic.try_range_fetch_or_waitlist(t, start_key, end_key, version, from) do
      {:ok, results, new_state} -> reply(new_state, {:ok, results})
      {:error, reason, new_state} -> reply(new_state, {:error, reason})
      {:waitlist, new_state} -> noreply(new_state)
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
    t
    |> Logic.unlock_after_recovery(durable_version, transaction_system_layout)
    |> case do
      {:ok, t} -> reply(t, :ok)
    end
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    otp_name
    |> Logic.startup(foreman, id, path)
    |> case do
      {:ok, t} -> noreply(t, continue: :report_health_to_foreman)
      {:error, reason} -> stop(:no_state, reason)
    end
  end

  @impl true
  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply(t)
  end

  @impl true
  def handle_info({:transactions_applied, version}, %State{} = t) do
    new_state = Logic.notify_waiting_fetches(t, version)
    noreply(new_state)
  end

  @impl true
  def handle_info(_msg, %State{} = t) do
    noreply(t)
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
