defmodule Bedrock.DataPlane.Resolver.Server do
  @moduledoc """
  GenServer implementation for the Resolver conflict detection engine.

  Manages resolver state including version ordering through waiting queues.
  Handles out-of-order transaction resolution by queuing later versions until
  earlier ones complete.

  Starts in running mode and is immediately ready to process transaction
  resolution requests.
  """
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Version

  import Bedrock.DataPlane.Resolver.ConflictResolution, only: [resolve: 3]

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @type reply_fn :: (aborted :: [non_neg_integer()] -> :ok)

  @spec child_spec(
          opts :: [
            lock_token: Bedrock.lock_token(),
            key_range: Bedrock.key_range(),
            epoch: Bedrock.epoch()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    _key_range = opts[:key_range] || raise "Missing :key_range option"
    _epoch = opts[:epoch] || raise "Missing :epoch option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {lock_token}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({lock_token}) do
    zero_version = Version.zero()

    %State{
      lock_token: lock_token,
      tree: %Tree{},
      oldest_version: zero_version,
      last_version: zero_version,
      mode: :running
    }
    |> then(&{:ok, &1})
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  # When transactions come in order, we can resolve them immediately. Once we're
  # done, we check if there are any transactions waiting for this version to be
  # resolved, and if so, we resolve them as well. We reply to this caller before
  # we do to avoid blocking them.
  @impl true
  def handle_call({:resolve_transactions, {last_version, next_version}, transactions}, _from, t)
      when t.mode == :running and last_version == t.last_version do
    {tree, aborted} = resolve(t.tree, transactions, next_version)
    t = %{t | tree: tree, last_version: next_version}

    if Map.has_key?(t.waiting, next_version) do
      t |> reply({:ok, aborted}, continue: {:resolve_next, next_version})
    else
      t |> reply({:ok, aborted})
    end
  end

  # When transactions come in a little out of order, we need to wait for the
  # previous transaction to be resolved before we can resolve the next one.
  @impl true
  def handle_call({:resolve_transactions, {last_version, next_version}, transactions}, from, t)
      when t.mode == :running do
    %{t | waiting: Map.put(t.waiting, last_version, {next_version, transactions, reply_fn(from)})}
    |> noreply()
  end

  @impl true
  def handle_info({:resolve_next, next_version}, t) do
    {{next_version, transactions, reply_fn}, waiting} = Map.pop(t.waiting, next_version)

    {tree, aborted} = resolve(t.tree, transactions, next_version)
    t = %{t | tree: tree, last_version: next_version, waiting: waiting}

    reply_fn.(aborted)

    if Map.has_key?(t.waiting, next_version) do
      t |> noreply(continue: {:resolve_next, next_version})
    else
      t |> noreply()
    end
  end

  @spec reply_fn(GenServer.from()) :: reply_fn()
  defp reply_fn(from), do: &GenServer.reply(from, &1)
end
