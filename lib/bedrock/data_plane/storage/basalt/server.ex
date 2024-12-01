defmodule Bedrock.DataPlane.Storage.Basalt.Server do
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Basalt.Logic
  alias Bedrock.DataPlane.Storage.Basalt.State
  alias Bedrock.Service.Foreman

  use GenServer
  import Bedrock.Internal.GenServer.Replies

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
  def init(args),
    # We use a continuation here to ensure that the foreman isn't blocked
    # waiting for the worker to finish it's startup sequence (which could take
    # a few seconds or longer if the database is large.) The foreman will
    # be notified when the worker is ready to accept requests.
    do: {:ok, args, {:continue, :finish_startup}}

  @impl true
  def terminate(_, %State{} = t) do
    Logic.shutdown(t)
    :normal
  end

  @impl true
  def handle_call({:fetch, key, version, _opts}, _from, %State{} = t),
    do: t |> Logic.fetch(key, version) |> then(&reply(t, &1))

  def handle_call({:range_query, min_key, max_key_ex, _opts}, _from, %State{} = t),
    do: t |> Logic.fetch_key_range(min_key, max_key_ex) |> then(&reply(t, &1))

  def handle_call({:info, fact_names}, _from, %State{} = t),
    do: t |> Logic.info(fact_names) |> then(&reply(t, &1))

  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    with {:ok, t} <- t |> Logic.lock_for_recovery(director, epoch),
         {:ok, info} <- t |> Logic.info(Storage.recovery_info()) do
      t |> reply({:ok, self(), info})
    else
      error -> t |> reply(error)
    end
  end

  def handle_call(
        {:unlock_after_recovery, durable_version, transaction_system_layout},
        {_director, _},
        t
      ) do
    t
    |> Logic.unlock_after_recovery(durable_version, transaction_system_layout)
    |> case do
      {:ok, t} -> t |> reply(:ok)
    end
  end

  def handle_call(_, _from, t),
    do: t |> reply({:error, :not_ready})

  @impl true
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    Logic.startup(otp_name, foreman, id, path)
    |> case do
      {:ok, t} -> t |> noreply(continue: :report_health_to_foreman)
      {:error, reason} -> :no_state |> stop(reason)
    end
  end

  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    t |> noreply()
  end
end
