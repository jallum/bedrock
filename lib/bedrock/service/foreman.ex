defmodule Bedrock.Service.Foreman do
  alias Bedrock.Service.Worker

  use Bedrock.Internal.GenServerApi, for: Bedrock.Service.Foreman.Supervisor

  @type ref :: GenServer.server()

  @type health :: :ok | {:failed_to_start, :at_least_one_failed_to_start} | :unknown | :starting

  def config_key, do: :worker

  @doc """
  Return a list of running workers.
  """
  @spec all(foreman :: ref(), opts :: [timeout: timeout()]) ::
          {:ok, [Worker.ref()]} | {:error, term()}
  def all(foreman, opts \\ []),
    do: call(foreman, :workers, to_timeout(opts[:timeout] || :infinity))

  @doc """
  Create a new worker.
  """
  @spec new_worker(
          foreman :: ref(),
          id :: Worker.id(),
          kind :: :log | :storage,
          opts :: [timeout: timeout()]
        ) ::
          {:ok, Worker.ref()} | {:error, term()}
  def new_worker(foreman, id, kind, opts \\ []),
    do: call(foreman, {:new_worker, id, kind}, to_timeout(opts[:timeout] || :infinity))

  @doc """
  Wait until the foreman signals that it (and all of it's workers) are
  reporting that they are healthy, or the timeout happens... whichever comes
  first.
  """
  @spec wait_for_healthy(foreman :: ref(), opts :: [timeout: timeout()]) ::
          :ok | {:error, :unavailable}
  def wait_for_healthy(foreman, opts \\ []),
    do: call(foreman, :wait_for_healthy, to_timeout(opts[:timeout] || :infinity))

  @doc """
  Called by a worker to report it's health to the foreman.
  """
  @spec report_health(foreman :: ref(), Worker.id(), any()) :: :ok
  def report_health(foreman, worker_id, health),
    do: cast(foreman, {:worker_health, worker_id, health})
end
