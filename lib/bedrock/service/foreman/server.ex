defmodule Bedrock.Service.Foreman.Server do
  alias Bedrock.Service.Foreman.State

  import Bedrock.Service.Foreman.State, only: [new_state: 1]
  import Bedrock.Service.Foreman.Impl

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  def required_opt_keys,
    do: [:cluster, :path, :capabilities, :otp_name]

  @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    args = opts |> Keyword.take(required_opt_keys()) |> Map.new()
    %{id: __MODULE__, start: {GenServer, :start_link, [__MODULE__, args, [name: args.otp_name]]}}
  end

  @impl true
  @spec init(map()) :: {:ok, State.t(), {:continue, :spin_up}} | {:stop, :missing_required_params}
  def init(args) do
    args
    |> new_state()
    |> case do
      {:ok, t} -> {:ok, t, {:continue, :spin_up}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:ping, _from, t),
    do: t |> reply(:pong)

  def handle_call(:workers, _from, t),
    do: t |> do_fetch_workers() |> then(&(t |> reply({:ok, &1})))

  def handle_call({:new_worker, id, kind}, _from, t),
    do: t |> do_new_worker(id, kind) |> then(fn {t, health} -> t |> reply({:ok, health}) end)

  def handle_call(:wait_for_healthy, from, t) do
    t
    |> do_wait_for_healthy(from)
    |> case do
      :ok -> t |> reply(:ok)
      t -> t |> noreply()
    end
  end

  def handle_call(_, _from, t),
    do: t |> reply({:error, :unknown_command})

  @impl true
  def handle_cast({:worker_health, worker_id, health}, t),
    do: t |> do_worker_health(worker_id, health) |> noreply()

  def handle_cast(_, t), do: t |> noreply()

  @impl true
  def handle_continue(:spin_up, t), do: do_spin_up(t) |> noreply()
end
