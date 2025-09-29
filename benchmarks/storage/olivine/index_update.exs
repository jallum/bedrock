# Run with: mix run benchmarks/storage/olivine/index_update.exs

defmodule Benchmarks.OlivineIndexUpdateBench do
  @moduledoc """
  Benchee micro benchmark for the olivine index_update module.

  This benchmark isolates pure index operations by pre-processing all data
  and measuring only the core IndexUpdate operations without transaction
  decoding or other overhead.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Benchee.Formatters.Console

  defmodule MockDatabase do
    @moduledoc """
    Mock database implementation that avoids actual I/O operations
    for isolated benchmarking of index manipulation.

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

  defp generate_key(i) do
    # Generate keys that will distribute across pages
    # Use zero-padded integers to ensure proper ordering
    key_base = i |> Integer.to_string() |> String.pad_leading(10, "0")
    "key_#{key_base}"
  end

  defp generate_value(i) do
    # Generate reasonably sized values
    "value_#{i}_" <> String.duplicate("x", 100)
  end

  # Pre-process mutations for clean benchmarking
  defp create_mutations(_base_index, 0), do: []

  defp create_mutations(base_index, key_count) when key_count > 0 do
    for i <- base_index..(base_index + key_count - 1) do
      {:set, generate_key(i), generate_value(i)}
    end
  end

  # Core index operation - what we actually want to benchmark
  defp apply_mutations_to_index(index, id_allocator, database, mutations, version) do
    index_update = IndexUpdate.new(index, version, id_allocator, database)

    updated_index_update =
      index_update
      |> IndexUpdate.apply_mutations(mutations)
      |> IndexUpdate.process_pending_operations()

    {updated_index, _database, updated_id_allocator, _modified_pages} = IndexUpdate.finish(updated_index_update)
    {{updated_index, updated_id_allocator}, updated_index_update}
  end

  # Apply multiple batches of mutations sequentially
  defp apply_mutation_batches(setup_data) do
    %{
      index: initial_index,
      id_allocator: initial_id_allocator,
      database: database,
      mutation_batches: mutation_batches,
      base_version: base_version
    } = setup_data

    {final_state, _all_stats} =
      mutation_batches
      |> Enum.with_index()
      |> Enum.reduce({{initial_index, initial_id_allocator}, []}, fn {mutations, idx},
                                                                     {{current_index, current_id_allocator}, stats_acc} ->
        version =
          if idx == 0 do
            base_version
          else
            Enum.reduce(1..idx, base_version, fn _i, v -> Version.increment(v) end)
          end

        {updated_state, batch_stats} =
          apply_mutations_to_index(current_index, current_id_allocator, database, mutations, version)

        {updated_state, [batch_stats | stats_acc]}
      end)

    final_state
  end

  # Setup for single mutation batch benchmark
  defp setup_single_batch_data do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    mutations = create_mutations(0, 200)
    base_version = Version.zero()

    %{
      index: index,
      id_allocator: id_allocator,
      database: database,
      mutations: mutations,
      base_version: base_version
    }
  end

  # Setup for multiple batches benchmark - pre-process all data
  defp setup_multiple_batches_data(num_batches, keys_per_batch) do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    base_version = Version.zero()

    # Pre-create all mutation batches to avoid measuring data generation
    mutation_batches =
      for i <- 0..(num_batches - 1) do
        create_mutations(i * keys_per_batch, keys_per_batch)
      end

    %{
      index: index,
      id_allocator: id_allocator,
      database: database,
      mutation_batches: mutation_batches,
      base_version: base_version
    }
  end

  # Setup for pre-populated index (to test performance on existing data)
  defp setup_prepopulated_index_data(prepopulate_keys, new_batch_keys) do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    base_version = Version.zero()

    # Pre-populate the index
    prepopulate_mutations = create_mutations(0, prepopulate_keys)

    {{populated_index, populated_id_allocator}, _stats} =
      apply_mutations_to_index(index, id_allocator, database, prepopulate_mutations, base_version)

    # Create new batch to insert
    new_mutations = create_mutations(prepopulate_keys, new_batch_keys)

    %{
      index: populated_index,
      id_allocator: populated_id_allocator,
      database: database,
      mutations: new_mutations,
      base_version: Version.increment(base_version)
    }
  end

  def run do
    IO.puts("=== Single Batch Performance (200 keys on empty index) ===")

    Benchee.run(
      %{
        "single_200key_batch" => fn setup_data ->
          %{index: index, id_allocator: id_allocator, database: database, mutations: mutations, base_version: version} =
            setup_data

          apply_mutations_to_index(index, id_allocator, database, mutations, version)
        end
      },
      before_each: fn _input -> setup_single_batch_data() end,
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("\n=== Multiple Batches Performance (10 batches × 200 keys) ===")

    Benchee.run(
      %{
        "apply_10_batches_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end
      },
      before_each: fn _input -> setup_multiple_batches_data(10, 200) end,
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("\n=== Scaling Test (different batch counts) ===")

    Benchee.run(
      %{
        "1_batch_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end,
        "5_batches_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end,
        "20_batches_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end
      },
      before_each: fn input ->
        case input do
          "1_batch_200keys" -> setup_multiple_batches_data(1, 200)
          "5_batches_200keys" -> setup_multiple_batches_data(5, 200)
          "20_batches_200keys" -> setup_multiple_batches_data(20, 200)
        end
      end,
      inputs: %{
        "1_batch_200keys" => "1_batch_200keys",
        "5_batches_200keys" => "5_batches_200keys",
        "20_batches_200keys" => "20_batches_200keys"
      },
      time: 3,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("\n=== Performance on Pre-populated Index ===")

    Benchee.run(
      %{
        "insert_into_empty_index" => fn setup_data ->
          %{index: index, id_allocator: id_allocator, database: database, mutations: mutations, base_version: version} =
            setup_data

          apply_mutations_to_index(index, id_allocator, database, mutations, version)
        end,
        "insert_into_1k_key_index" => fn setup_data ->
          %{index: index, id_allocator: id_allocator, database: database, mutations: mutations, base_version: version} =
            setup_data

          apply_mutations_to_index(index, id_allocator, database, mutations, version)
        end
      },
      before_each: fn input ->
        case input do
          "insert_into_empty_index" -> setup_prepopulated_index_data(0, 200)
          "insert_into_1k_key_index" -> setup_prepopulated_index_data(1000, 200)
        end
      end,
      inputs: %{
        "insert_into_empty_index" => "insert_into_empty_index",
        "insert_into_1k_key_index" => "insert_into_1k_key_index"
      },
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )
  end
end

# Run the benchmark
IO.puts("Starting Olivine IndexUpdate benchmark...")
IO.puts("This benchmark isolates pure index operations from transaction processing overhead.")
IO.puts("All data is pre-processed to measure only IndexUpdate performance.\n")

Benchmarks.OlivineIndexUpdateBench.run()
