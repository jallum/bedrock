defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.DataDatabase
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase

  @type t :: {DataDatabase.t(), IndexDatabase.t()}
  @type locator :: DataDatabase.locator()

  @spec open(otp_name :: atom(), file_path :: String.t(), opts :: keyword()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, opts \\ []) when is_atom(otp_name) and is_list(opts) do
    with {:ok, data_db} <- DataDatabase.open(file_path, opts),
         {:ok, index_db} <- IndexDatabase.open(otp_name, file_path) do
      {:ok, {data_db, index_db}}
    end
  end

  @spec close(t()) :: :ok
  def close({data_db, index_db}) do
    DataDatabase.close(data_db)
    IndexDatabase.close(index_db)
    :ok
  end

  @spec load_value(t(), locator()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value({data_db, _index_db}, locator), do: DataDatabase.load_value(data_db, locator)

  @doc """
  Store a value in the lookaside buffer for the given version and key.
  This is used during transaction application for values within the window.
  """
  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) ::
          {:ok, locator(), database :: t()}
  def store_value({data_db, index_db}, key, version, value) do
    {:ok, locator, updated_data_db} = DataDatabase.store_value(data_db, key, version, value)
    {:ok, locator, {updated_data_db, index_db}}
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec value_loader(t()) :: (locator() -> {:ok, Bedrock.value()} | {:error, :not_found} | {:error, :shutting_down})
  def value_loader({data_db, _index_db}), do: DataDatabase.value_loader(data_db)

  @spec many_value_loader(t()) ::
          ([locator()] ->
             {:ok, %{locator() => Bedrock.value()}}
             | {:error, :not_found}
             | {:error, :shutting_down})
  def many_value_loader({data_db, _index_db}), do: DataDatabase.many_value_loader(data_db)

  @spec durable_version(t()) :: Bedrock.version()
  def durable_version({_data_db, index_db}), do: IndexDatabase.durable_version(index_db)

  @doc """
  Load durable version directly from storage.
  This is useful for background processes that may have a stale database struct.
  """
  @spec load_current_durable_version(t()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_current_durable_version({_data_db, index_db}), do: IndexDatabase.load_durable_version(index_db)

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) :: any() | :undefined
  def info({_data_db, index_db}, stat), do: IndexDatabase.info(index_db, stat)

  @spec advance_durable_version(
          t(),
          version :: Bedrock.version(),
          previous_durable_version :: Bedrock.version(),
          data_size_in_bytes :: pos_integer(),
          collected_pages :: [%{Page.id() => {Page.t(), Page.id()}}]
        ) ::
          {:ok, t(), metadata :: map()} | {:error, term()}
  def advance_durable_version(
        {data_db, index_db},
        version,
        previous_durable_version,
        data_size_in_bytes,
        collected_pages
      ) do
    start_time = System.monotonic_time(:microsecond)

    {write_time_μs, updated_data_db} = :timer.tc(fn -> DataDatabase.flush(data_db, data_size_in_bytes) end)

    {insert_time_μs, updated_index_db} =
      :timer.tc(fn -> IndexDatabase.flush(index_db, version, previous_durable_version, collected_pages) end)

    total_duration_μs = System.monotonic_time(:microsecond) - start_time

    metadata = %{
      insert_time_μs: insert_time_μs,
      write_time_μs: write_time_μs,
      total_duration_μs: total_duration_μs
    }

    {:ok, {updated_data_db, updated_index_db}, metadata}
  end
end
