defmodule Bedrock.Cluster.Bootstrap do
  @moduledoc """

  """

  alias Bedrock.Service

  def child_spec(args) do
    cluster = args[:cluster] || raise ":cluster is required"
    foreman = args[:foreman] || raise ":foreman is required"
    # seed_nodes = args[:seed_nodes] || [node()]

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__.Service,
           [
             cluster: cluster,
             foreman: foreman,
             seed_nodes: []
           ],
           []
         ]}
    }
  end

  defmodule FindingSeedNodes do
    alias Bedrock.DataPlane.Storage
    alias Bedrock.Service.Foreman

    @system_key_range_min <<0xFF>>

    @type t :: %{}

    def run(t, step \\ :start) do
      case step(step, t) do
        {:completed, t} ->
          {:ok, t}

        {{:stalled, _reason} = stalled, t} ->
          {stalled, t}

        {new_step, new_t} when is_atom(new_step) and step != new_step ->
          run(new_t, new_step)
      end
    end

    def step(:start, t), do: {:find_workers, t}

    # Figure out what workers are available to us. If we don't have any workers
    # and we don't have a seed node, then we'll need to start one. Otherwise,
    def step(:find_workers, t) do
      with {:ok, workers} <- Foreman.all(t.foreman) do
        if [] == workers && [] == t.seed_nodes do
          {:setup_local_storage, t}
        else
          t
          |> Map.put(:workers, workers)
          |> then(&{:interrogate_local_storage, &1})
        end
      else
        {:error, reason} -> {{:stalled, reason}, t}
      end
    end

    # No storage nodes are available and we don't have a seed node to bootstrap
    # from, so we'll setup a storage worker in anticipation of seeding an
    # entirely new system.
    def step(:setup_local_storage, t) do
      with {:ok, storage} <- Foreman.new_worker(t.foreman, random_worker_id(), :storage) do
        t
        |> Map.put(:workers, [storage])
        |> then(&{:interrogate_local_storage, &1})
      else
        {:error, reason} -> {{:stalled, reason}, t}
      end
    end

    # Use the list of workers to find out which ones are storage nodes, and then
    # ask them for information about the key ranges / durability of their data.
    def step(:interrogate_local_storage, t) do
      t
      |> Map.put(
        :available_storage,
        t.workers
        |> Task.async_stream(
          &Storage.info(&1, [:kind, :id, :otp_name, :key_ranges]),
          ordered: false
        )
        |> Enum.reduce(%{}, fn
          {:ok, {:ok, %{kind: :storage, id: id, key_ranges: key_ranges} = info}}, acc
          when is_list(key_ranges) ->
            Map.put(acc, id, info |> Map.delete(:id))

          _, acc ->
            acc
        end)
      )
      |> then(&{:determine_best_durable_system_range_storage, &1})
    end

    # Given a set of locally available storage workers, determine which one (if
    # any) has the best durable system range for this node to use. It is
    # important that the range be _complete_ -- i.e., it must cover the entire
    # system keyspace and not be missing any keys. (This could occur if the
    # service _happens_ to be replicating the range from another server, but
    # hasn't validated that it has _all_ the keys yet.)
    def step(:determine_best_durable_system_range_storage, t) do
      {durable_version, best_storage_id} =
        t.available_storage
        |> Enum.flat_map(fn
          {id, %{key_ranges: key_ranges}} ->
            key_ranges
            |> Enum.map(fn
              {{min_key, :end}, true, durable_version} when min_key <= @system_key_range_min ->
                {durable_version, id}

              _ ->
                nil
            end)
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.max_by(&elem(&1, 0), fn -> {:undefined, nil} end)

      t
      |> Map.put(:durable_version, durable_version)
      |> Map.put(:best_storage_id, best_storage_id)
      |> then(&{:try_to_read_seeds_from_durable_storage, &1})
    end

    # If we have a durable storage node, try to read the seeds from it. If we
    # can't, then we'll fall back to the seed nodes we were given.
    def step(:try_to_read_seeds_from_durable_storage, t) do
      best_storage = get_in(t.available_storage, [t.best_storage_id, :otp_name])

      seeds_from_storage =
        Storage.fetch(best_storage, <<0xFF, "/seeds">>, t.durable_version)
        |> case do
          {:ok, seeds} -> seeds |> String.split(",")
          _ -> t.seed_nodes
        end

      t
      |> Map.put(:seed_nodes, seeds_from_storage)
      |> then(&{:try_to_contact_seed_nodes, &1})
    end

    def step(:try_to_contact_seed_nodes, t) do
      t
      |> then(&{:completed, &1})
    end

    defp random_worker_id() do
      1_000_000_000
      |> Kernel.+(:rand.uniform(1_000_000_000))
      |> Integer.to_string(36)
      |> String.slice(0..6)
      |> String.downcase()
    end
  end

  defmodule Service do
    alias Bedrock.Cluster.Bootstrap.Telemetry

    use GenServer

    @impl true
    def init(opts) do
      %{
        cluster: opts[:cluster],
        foreman: opts[:foreman],
        seed_nodes: opts[:seed_nodes]
      }
      |> then(&{:ok, &1, {:continue, :run}})
    end

    @impl true
    def handle_continue(:run, t) do
      Telemetry.execute_started(t.cluster)

      case FindingSeedNodes.run(t) do
        {:ok, t} ->
          Telemetry.execute_completed()
          IO.inspect(t)
          {:noreply, t}

        {stalled, t} ->
          Telemetry.execute_stalled(stalled)
          {:stop, :normal, t}
      end
    end
  end
end
