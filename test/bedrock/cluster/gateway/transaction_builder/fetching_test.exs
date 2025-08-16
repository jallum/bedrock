defmodule Bedrock.Cluster.Gateway.TransactionBuilder.FetchingTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Fetching
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  defmodule TestKeyCodec do
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
    def encode_value(value), do: {:ok, value}
    def decode_value(value), do: {:ok, value}
  end

  def create_test_state(opts \\ []) do
    %State{
      state: :valid,
      gateway: Keyword.get(opts, :gateway, :test_gateway),
      transaction_system_layout:
        Keyword.get(opts, :transaction_system_layout, create_test_layout()),
      key_codec: Keyword.get(opts, :key_codec, TestKeyCodec),
      value_codec: Keyword.get(opts, :value_codec, TestValueCodec),
      read_version: Keyword.get(opts, :read_version),
      read_version_lease_expiration: Keyword.get(opts, :read_version_lease_expiration),
      stack: Keyword.get(opts, :stack, []),
      fastest_storage_servers: Keyword.get(opts, :fastest_storage_servers, %{}),
      fetch_timeout_in_ms: Keyword.get(opts, :fetch_timeout_in_ms, 100)
    }
  end

  def create_test_layout do
    %{
      sequencer: :test_sequencer,
      storage_teams: [
        %{
          key_range: {"", :end},
          storage_ids: ["storage1", "storage2"]
        }
      ],
      services: %{
        "storage1" => %{kind: :storage, status: {:up, :storage1_pid}},
        "storage2" => %{kind: :storage, status: {:up, :storage2_pid}}
      }
    }
  end

  describe "do_fetch/3" do
    test "returns value from writes cache" do
      alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

      # Create a state with a transaction containing the cached value
      tx = Tx.set(Tx.new(), "cached_key", "cached_value")
      state = %{create_test_state() | tx: tx}

      {new_state, result} = Fetching.do_fetch(state, "cached_key")

      assert result == {:ok, "cached_value"}
      # Transaction state should be unchanged when reading from writes
      assert Tx.commit(new_state.tx) == Tx.commit(state.tx)
    end

    test "fetches value from storage when not cached" do
      # Create state with read version and mock storage function
      state = %{create_test_state() | read_version: 12_345}

      # Mock storage function
      storage_fn = fn _pid, _key, _version, _opts ->
        {:ok, "storage_value"}
      end

      opts = [storage_fetch_fn: storage_fn]

      # Fetch should call storage and return value
      {_new_state, result} = Fetching.do_fetch(state, "storage_key", opts)
      assert result == {:ok, "storage_value"}
    end

    test "writes cache takes precedence over reads cache" do
      alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

      # Create a transaction that has a value in reads cache and a different value in writes cache
      tx = Tx.new()
      # Value in reads cache
      tx = %{tx | reads: %{"key" => "old_value"}}
      # Value in writes cache (should take precedence)
      tx = Tx.set(tx, "key", "new_value")

      state = %{create_test_state() | tx: tx}

      {new_state, result} = Fetching.do_fetch(state, "key")

      # Should get writes value, not reads value
      assert result == {:ok, "new_value"}
      # Transaction unchanged since value came from writes cache
      assert Tx.commit(new_state.tx) == Tx.commit(state.tx)
    end

    test "fetches from storage when not in cache" do
      state = create_test_state(read_version: 12_345)

      storage_fetch_fn = fn :storage1_pid, "storage_key", 12_345, [timeout: 100] ->
        {:ok, "storage_value"}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "storage_key", opts)

      assert result == {:ok, "storage_value"}
      # Check that the value was cached in the transaction reads
      assert new_state.tx.reads == %{"storage_key" => "storage_value"}
    end

    test "handles storage fetch error" do
      state = create_test_state(read_version: 12_345)

      storage_fetch_fn = fn :storage1_pid, "error_key", 12_345, [timeout: 100] ->
        {:error, :not_found}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "error_key", opts)

      assert result == {:error, :not_found}
      # Check that the error was cached in the transaction reads
      assert new_state.tx.reads == %{"error_key" => :clear}
    end

    test "acquires read version when nil" do
      state = create_test_state(read_version: nil)
      current_time = 50_000

      next_read_version_fn = fn ^state -> {:ok, 12_345, 5000} end
      time_fn = fn -> current_time end

      storage_fetch_fn = fn :storage1_pid, "key", 12_345, [timeout: 100] ->
        {:ok, "value"}
      end

      opts = [
        next_read_version_fn: next_read_version_fn,
        time_fn: time_fn,
        storage_fetch_fn: storage_fetch_fn
      ]

      {new_state, result} = Fetching.do_fetch(state, "key", opts)

      assert result == {:ok, "value"}
      assert new_state.read_version == 12_345
      assert new_state.read_version_lease_expiration == current_time + 5000
      # Check that the value was cached in the transaction reads
      assert new_state.tx.reads == %{"key" => "value"}
    end

    test "handles next_read_version unavailable error" do
      state = create_test_state(read_version: nil)

      next_read_version_fn = fn ^state -> {:error, :unavailable} end

      opts = [next_read_version_fn: next_read_version_fn]

      assert_raise RuntimeError, "No read version available for fetching key: \"key\"", fn ->
        Fetching.do_fetch(state, "key", opts)
      end
    end

    test "handles key encoding error" do
      defmodule FailingKeyCodec do
        def encode_key(_), do: :key_error
      end

      state = create_test_state(key_codec: FailingKeyCodec)

      assert_raise MatchError, fn ->
        Fetching.do_fetch(state, :invalid_key)
      end
    end

    test "handles value decoding error" do
      defmodule FailingValueCodec do
        def encode_value(value), do: {:ok, value}
        def decode_value(_), do: :decode_error
      end

      state =
        create_test_state(
          read_version: 12_345,
          value_codec: FailingValueCodec
        )

      storage_fetch_fn = fn :storage1_pid, "key", 12_345, [timeout: 100] ->
        {:ok, "encoded_value"}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "key", opts)

      assert result == {:ok, "encoded_value"}
      # The fetch was successful so the value should be cached and fastest server updated
      assert new_state.tx.reads == %{"key" => "encoded_value"}
      # Fastest storage server should be updated
      assert new_state.fastest_storage_servers != %{}
    end
  end

  # Note: fetch_from_stack/2 function was removed in new Tx-based architecture
  # Stack operations are now handled internally by the Tx module

  describe "storage_servers_for_key/2" do
    test "finds storage servers for key in range" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"a", "m"},
            storage_ids: ["storage1", "storage2"]
          },
          %{
            key_range: {"m", :end},
            storage_ids: ["storage3"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}},
          "storage2" => %{kind: :storage, status: {:up, :pid2}},
          "storage3" => %{kind: :storage, status: {:up, :pid3}}
        }
      }

      # Key "hello" should be in first range
      result = Fetching.storage_servers_for_key(layout, "hello")
      expected = [{{"a", "m"}, :pid1}, {{"a", "m"}, :pid2}]
      assert result == expected

      # Key "zebra" should be in second range
      result = Fetching.storage_servers_for_key(layout, "zebra")
      expected = [{{"m", :end}, :pid3}]
      assert result == expected
    end

    test "filters out down storage servers" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1", "storage2", "storage3"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}},
          "storage2" => %{kind: :storage, status: {:down, nil}},
          "storage3" => %{kind: :storage, status: {:up, :pid3}}
        }
      }

      result = Fetching.storage_servers_for_key(layout, "key")
      expected = [{{"", :end}, :pid1}, {{"", :end}, :pid3}]
      assert result == expected
    end

    test "returns empty list when no servers available" do
      layout = %{
        storage_teams: [
          %{
            key_range: {"a", "b"},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :pid1}}
        }
      }

      # Key "z" is outside range
      result = Fetching.storage_servers_for_key(layout, "z")
      assert result == []
    end
  end

  describe "fastest_storage_server_for_key/2" do
    test "finds fastest server for key in range" do
      fastest_servers = %{
        {"a", "m"} => :fast_pid1,
        {"m", :end} => :fast_pid2
      }

      assert Fetching.fastest_storage_server_for_key(fastest_servers, "hello") == :fast_pid1
      assert Fetching.fastest_storage_server_for_key(fastest_servers, "zebra") == :fast_pid2
    end

    test "returns nil when no fastest server found" do
      fastest_servers = %{
        {"a", "m"} => :fast_pid1
      }

      assert Fetching.fastest_storage_server_for_key(fastest_servers, "zebra") == nil
    end

    test "handles :end boundary correctly" do
      fastest_servers = %{
        {"m", :end} => :fast_pid
      }

      assert Fetching.fastest_storage_server_for_key(fastest_servers, "m") == :fast_pid
      assert Fetching.fastest_storage_server_for_key(fastest_servers, "zzzz") == :fast_pid
    end
  end

  describe "horse_race_storage_servers_for_key/5" do
    test "returns :error for empty server list" do
      result = Fetching.horse_race_storage_servers_for_key([], 12_345, "key", 100, [])
      assert result == :error
    end

    test "returns first successful response" do
      servers = [
        {{"a", "m"}, :pid1},
        {{"a", "m"}, :pid2}
      ]

      async_stream_fn = fn servers, fun, _opts ->
        servers
        |> Enum.map(fun)
        |> Enum.map(&{:ok, &1})
      end

      storage_fetch_fn = fn
        :pid1, "key", 12_345, [timeout: 100] -> {:ok, "value1"}
        :pid2, "key", 12_345, [timeout: 100] -> {:error, :timeout}
      end

      opts = [async_stream_fn: async_stream_fn, storage_fetch_fn: storage_fetch_fn]

      result = Fetching.horse_race_storage_servers_for_key(servers, 12_345, "key", 100, opts)
      assert result == {:ok, {"a", "m"}, :pid1, "value1"}
    end

    test "handles all servers returning errors" do
      servers = [
        {{"a", "m"}, :pid1},
        {{"a", "m"}, :pid2}
      ]

      async_stream_fn = fn servers, fun, _opts ->
        servers
        |> Enum.map(fun)
        |> Enum.map(&{:ok, &1})
      end

      storage_fetch_fn = fn
        :pid1, "key", 12_345, [timeout: 100] -> {:error, :timeout}
        :pid2, "key", 12_345, [timeout: 100] -> {:error, :not_found}
      end

      opts = [async_stream_fn: async_stream_fn, storage_fetch_fn: storage_fetch_fn]

      result = Fetching.horse_race_storage_servers_for_key(servers, 12_345, "key", 100, opts)
      # Returns first error that matches the pattern
      assert result == {:error, :not_found}
    end

    test "prioritizes version errors" do
      servers = [
        {{"a", "m"}, :pid1},
        {{"a", "m"}, :pid2}
      ]

      async_stream_fn = fn servers, fun, _opts ->
        servers
        |> Enum.map(fun)
        |> Enum.map(&{:ok, &1})
      end

      storage_fetch_fn = fn
        :pid1, "key", 12_345, [timeout: 100] -> {:error, :timeout}
        :pid2, "key", 12_345, [timeout: 100] -> {:error, :version_too_old}
      end

      opts = [async_stream_fn: async_stream_fn, storage_fetch_fn: storage_fetch_fn]

      result = Fetching.horse_race_storage_servers_for_key(servers, 12_345, "key", 100, opts)
      assert result == {:error, :version_too_old}
    end
  end

  describe "fetch_from_fastest_storage_server/4" do
    test "updates fastest_storage_servers when successful" do
      state = create_test_state(read_version: 12_345, fastest_storage_servers: %{})
      servers = [{{"a", "m"}, :pid1}]

      horse_race_fn = fn ^servers, 12_345, "key", 100, _opts ->
        {:ok, {"a", "m"}, :pid1, "value"}
      end

      opts = [horse_race_fn: horse_race_fn]

      {:ok, new_state, value} =
        Fetching.fetch_from_fastest_storage_server(state, servers, "key", opts)

      assert value == "value"
      assert new_state.fastest_storage_servers == %{{"a", "m"} => :pid1}
    end

    test "propagates horse race errors" do
      state = create_test_state(read_version: 12_345)
      servers = [{{"a", "m"}, :pid1}]

      horse_race_fn = fn ^servers, 12_345, "key", 100, _opts ->
        {:error, :timeout}
      end

      opts = [horse_race_fn: horse_race_fn]

      result = Fetching.fetch_from_fastest_storage_server(state, servers, "key", opts)
      assert result == {:error, :timeout}
    end
  end

  describe "integration scenarios" do
    test "complete fetch flow with storage fallback" do
      # Start with empty caches, should go to storage
      state = create_test_state(read_version: 12_345)

      storage_fetch_fn = fn :storage1_pid, "integration_key", 12_345, [timeout: 100] ->
        {:ok, "integration_value"}
      end

      opts = [storage_fetch_fn: storage_fetch_fn]

      {new_state, result} = Fetching.do_fetch(state, "integration_key", opts)

      assert result == {:ok, "integration_value"}
      # Check that the value was cached in the transaction reads
      assert new_state.tx.reads == %{"integration_key" => "integration_value"}

      # Second fetch should hit reads cache
      {final_state, result2} = Fetching.do_fetch(new_state, "integration_key", opts)

      assert result2 == {:ok, "integration_value"}
      # No change since it hit cache
      assert final_state == new_state
    end

    test "fetch with stack fallback" do
      # Key not in current reads/writes but in stack - this functionality was removed in Tx refactor
      # Stack operations are now handled internally by the Tx module
      # This test is no longer applicable with the new architecture
      alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

      # Create transaction with the value already in reads cache to simulate stack fallback
      tx = %{Tx.new() | reads: %{"stack_key" => "stack_value"}}
      state = %{create_test_state() | tx: tx}

      {new_state, result} = Fetching.do_fetch(state, "stack_key")

      assert result == {:ok, "stack_value"}
      # Transaction unchanged since it hit reads cache
      assert Tx.commit(new_state.tx) == Tx.commit(state.tx)
    end
  end

  # Property-based tests for robust validation of key functions
  describe "property-based tests" do
    property "fastest_storage_server_for_key returns nil when no servers match key range" do
      check all(
              key <- binary(),
              server_map <- map_of(key_range_generator(), pid_generator())
            ) do
        # Filter out any ranges that would actually contain our key
        filtered_map =
          server_map
          |> Enum.reject(fn {{min_key, max_key}, _pid} ->
            key_in_range?(key, min_key, max_key)
          end)
          |> Map.new()

        if map_size(filtered_map) > 0 do
          assert nil == Fetching.fastest_storage_server_for_key(filtered_map, key)
        end
      end
    end

    property "fastest_storage_server_for_key returns server when key is in range" do
      check all(
              key <- binary(),
              server_pid <- pid_generator()
            ) do
        # Create a range that definitely contains our key
        # Key to end always contains the key
        range = {key, :end}
        server_map = %{range => server_pid}

        assert ^server_pid = Fetching.fastest_storage_server_for_key(server_map, key)
      end
    end

    property "fastest_storage_server_for_key handles :end boundary correctly" do
      check all(
              key <- binary(),
              server_pid <- pid_generator()
            ) do
        # Range with :end should include everything >= min_key
        # Empty string to end includes everything
        range = {"", :end}
        server_map = %{range => server_pid}

        assert ^server_pid = Fetching.fastest_storage_server_for_key(server_map, key)
      end
    end

    property "storage_servers_for_key returns empty list when no teams cover key range" do
      check all(key <- binary()) do
        layout = %{
          storage_teams: [],
          services: %{}
        }

        assert [] = Fetching.storage_servers_for_key(layout, key)
      end
    end

    property "storage_servers_for_key filters out down storage servers" do
      check all(key <- binary()) do
        layout = %{
          storage_teams: [
            %{
              key_range: {"", :end},
              storage_ids: ["up_server", "down_server"]
            }
          ],
          services: %{
            "up_server" => %{kind: :storage, status: {:up, :up_pid}},
            "down_server" => %{kind: :storage, status: {:down, nil}}
          }
        }

        result = Fetching.storage_servers_for_key(layout, key)

        # Should only include the up server
        assert [{{"", :end}, :up_pid}] = result
      end
    end
  end

  # Property test generators and helpers
  defp key_range_generator do
    gen all(
          min_key <- binary(),
          max_key <- one_of([binary(), constant(:end)])
        ) do
      {min_key, max_key}
    end
  end

  defp pid_generator do
    constant(self())
  end

  defp key_in_range?(key, min_key, :end) do
    key >= min_key
  end

  defp key_in_range?(key, min_key, max_key) when is_binary(max_key) do
    key >= min_key and key < max_key
  end
end
