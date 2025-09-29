defmodule OlivineRangeClearBench do
  @moduledoc """
  Benchmarks range clear operations in the olivine storage driver.

  This benchmark specifically measures the performance of range clear mutations
  to understand the impact of Tree.page_ids_in_range optimization opportunities.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Version

  defmodule MockDatabase do
    @moduledoc """
    Mock database for dependency injection - isolates page/index performance.
    Updated to work with the new tuple-based database architecture.
    """

    defmodule MockDataDatabase do
      @moduledoc false
      defstruct [
        :file,
        :file_offset,
        :file_name,
        :window_size_in_microseconds,
        :buffer
      ]

      def new do
        %__MODULE__{
          file: nil,
          file_offset: 0,
          file_name: nil,
          window_size_in_microseconds: 5_000_000,
          buffer: :ets.new(:mock_buffer, [:ordered_set, :public, {:read_concurrency, true}])
        }
      end

      def store_value(data_db, _key, _version, value) do
        offset = data_db.file_offset
        size = byte_size(value)
        locator = <<offset::47, size::17>>
        :ets.insert(data_db.buffer, {locator, value})
        {:ok, locator, %{data_db | file_offset: offset + size}}
      end

      def load_value(data_db, locator) do
        case locator do
          <<_offset::47, 0::17>> ->
            {:ok, <<>>}

          <<_offset::47, _size::17>> = locator ->
            case :ets.lookup(data_db.buffer, locator) do
              [{^locator, value}] -> {:ok, value}
              [] -> {:error, :not_found}
            end
        end
      end
    end

    defmodule MockIndexDatabase do
      @moduledoc false
      defstruct [
        :dets_storage,
        :durable_version
      ]

      def new do
        %__MODULE__{
          dets_storage: :ets.new(:mock_dets, [:set, :public]),
          durable_version: Version.zero()
        }
      end

      def store_page(index_db, page_id, page_tuple) do
        :ets.insert(index_db.dets_storage, {page_id, page_tuple})
        :ok
      end

      def load_page(index_db, page_id) do
        case :ets.lookup(index_db.dets_storage, page_id) do
          [{^page_id, page_tuple}] -> {:ok, page_tuple}
          [] -> {:error, :not_found}
        end
      end

      def durable_version(index_db), do: index_db.durable_version
    end

    def new do
      data_db = MockDataDatabase.new()
      index_db = MockIndexDatabase.new()
      {data_db, index_db}
    end

    def store_value({data_db, index_db}, key, version, value) do
      {:ok, locator, updated_data_db} = MockDataDatabase.store_value(data_db, key, version, value)
      {:ok, locator, {updated_data_db, index_db}}
    end

    def load_value({data_db, _index_db}, locator) do
      MockDataDatabase.load_value(data_db, locator)
    end

    def store_page({_data_db, index_db}, page_id, page_tuple) do
      MockIndexDatabase.store_page(index_db, page_id, page_tuple)
    end

    def load_page({_data_db, index_db}, page_id) do
      MockIndexDatabase.load_page(index_db, page_id)
    end

    def durable_version({_data_db, index_db}) do
      MockIndexDatabase.durable_version(index_db)
    end
  end

  def run do
    IO.puts("Starting Olivine Range Clear benchmark...")
    IO.puts("This will create an index with many pages, then measure range clear performance.")
    IO.puts("")

    # Set up benchmark
    database = MockDatabase.new()
    id_allocator = IdAllocator.new(0, [])
    index = Index.new()
    version = Version.zero()

    # Create initial data: populate index with many small ranges
    # This will create multiple pages, making range clears more interesting
    {populated_index, populated_allocator} = populate_index_with_ranges(index, id_allocator, database, version)

    IO.puts("Index populated with #{map_size(populated_index.page_map)} pages")
    IO.puts("")

    # Benchmark different range clear scenarios
    Benchee.run(
      %{
        "small_range_clear" => fn ->
          measure_range_clear(populated_index, populated_allocator, database, "key_100", "key_199")
        end,
        "medium_range_clear" => fn ->
          measure_range_clear(populated_index, populated_allocator, database, "key_200", "key_499")
        end,
        "large_range_clear" => fn ->
          measure_range_clear(populated_index, populated_allocator, database, "key_500", "key_999")
        end,
        "cross_page_range_clear" => fn ->
          # This should span multiple pages
          measure_range_clear(populated_index, populated_allocator, database, "key_050", "key_550")
        end
      },
      time: 3,
      warmup: 1
    )
  end

  # Populate index with data across multiple pages
  defp populate_index_with_ranges(index, id_allocator, database, base_version) do
    # Create 20 transactions with 100 keys each = 2000 total keys
    # This should create multiple pages due to the 256 key per page limit
    transactions = create_population_transactions(20, 100)

    # Apply all transactions to build up the index
    Enum.reduce(transactions, {index, id_allocator}, fn transaction, {acc_index, acc_allocator} ->
      version = Version.increment(base_version)
      index_update = IndexUpdate.new(acc_index, version, acc_allocator, database)

      updated_index_update =
        index_update
        |> IndexUpdate.apply_mutations(transaction)
        |> IndexUpdate.process_pending_operations()

      {updated_index, _database, updated_id_allocator, _modified_pages} = IndexUpdate.finish(updated_index_update)
      {updated_index, updated_id_allocator}
    end)
  end

  # Create transactions with sequential keys to ensure page splits
  defp create_population_transactions(num_transactions, keys_per_transaction) do
    for tx_num <- 1..num_transactions do
      base_key_num = (tx_num - 1) * keys_per_transaction

      mutations =
        for key_num <- base_key_num..(base_key_num + keys_per_transaction - 1) do
          key = "key_#{String.pad_leading(to_string(key_num), 4, "0")}"
          value = "value_#{key_num}"
          {:set, key, value}
        end

      mutations
    end
  end

  # Measure a single range clear operation
  defp measure_range_clear(index, id_allocator, database, start_key, end_key) do
    version = Version.increment(Version.zero())
    mutation = {:clear_range, start_key, end_key}

    index_update = IndexUpdate.new(index, version, id_allocator, database)

    # Apply the range clear mutation
    updated_index_update =
      index_update
      |> IndexUpdate.apply_mutations([mutation])
      |> IndexUpdate.process_pending_operations()

    {updated_index, _database, updated_id_allocator, _modified_pages} = IndexUpdate.finish(updated_index_update)
    {updated_index, updated_id_allocator}
  end
end

OlivineRangeClearBench.run()
