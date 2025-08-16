defmodule Bedrock.Cluster.Gateway.TransactionBuilder.PuttingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Putting
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  defmodule TestKeyCodec do
    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(:invalid_key), do: :key_error
    def encode_key(_), do: :key_error
  end

  defmodule TestValueCodec do
    def encode_value(value) when is_binary(value), do: {:ok, value}
    def encode_value(value) when is_integer(value), do: {:ok, "int:#{value}"}
    def encode_value(:invalid_value), do: :value_error
    def encode_value(_), do: {:ok, "encoded"}
  end

  defp mutations_to_writes(mutations) do
    mutations
    |> Enum.reduce(%{}, fn
      {:set, key, value}, acc -> Map.put(acc, key, value)
      _, acc -> acc
    end)
  end

  def create_test_state(_writes \\ %{}) do
    %State{
      state: :valid,
      gateway: self(),
      transaction_system_layout: %{},
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec
    }
  end

  describe "do_put/3" do
    test "successfully puts a binary key and value" do
      state = create_test_state()

      {:ok, new_state} = Putting.do_put(state, "test_key", "test_value")

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{
               "test_key" => "test_value"
             }
    end

    test "successfully puts multiple key-value pairs" do
      state = create_test_state()

      {:ok, state1} = Putting.do_put(state, "key1", "value1")
      {:ok, state2} = Putting.do_put(state1, "key2", "value2")
      {:ok, state3} = Putting.do_put(state2, "key3", "value3")

      assert Tx.commit(state3.tx).mutations |> mutations_to_writes == %{
               "key1" => "value1",
               "key2" => "value2",
               "key3" => "value3"
             }
    end

    test "overwrites existing key with new value" do
      state = create_test_state(%{"existing_key" => "old_value"})

      {:ok, new_state} = Putting.do_put(state, "existing_key", "new_value")

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{
               "existing_key" => "new_value"
             }
    end

    test "handles integer values with codec encoding" do
      state = create_test_state()

      {:ok, new_state} = Putting.do_put(state, "number_key", 42)

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{
               "number_key" => "int:42"
             }
    end

    test "handles various value types through codec" do
      state = create_test_state()

      {:ok, state1} = Putting.do_put(state, "string", "text")
      {:ok, state2} = Putting.do_put(state1, "number", 123)
      {:ok, state3} = Putting.do_put(state2, "atom", :test)
      {:ok, state4} = Putting.do_put(state3, "map", %{a: 1})

      assert Tx.commit(state4.tx).mutations |> mutations_to_writes == %{
               "string" => "text",
               "number" => "int:123",
               "atom" => "encoded",
               "map" => "encoded"
             }
    end

    test "returns :key_error for invalid key" do
      state = create_test_state()

      result = Putting.do_put(state, :invalid_key, "value")

      assert result == :key_error
    end

    test "handles empty string key and value" do
      state = create_test_state()

      {:ok, new_state} = Putting.do_put(state, "", "")

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{"" => ""}
    end

    test "handles unicode keys and values" do
      state = create_test_state()

      {:ok, new_state} = Putting.do_put(state, "键名", "值")

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{"键名" => "值"}
    end

    test "preserves other state fields" do
      # Create transaction with existing data
      existing_tx = Tx.set(Tx.new(), "existing", "value")

      original_state = %State{
        state: :valid,
        gateway: self(),
        transaction_system_layout: %{test: "layout"},
        key_codec: TestKeyCodec,
        value_codec: TestValueCodec,
        tx: existing_tx,
        stack: [],
        fastest_storage_servers: %{range: :server}
      }

      {:ok, new_state} = Putting.do_put(original_state, "new_key", "new_value")

      # Verify writes were updated - should have both existing and new values
      result_writes = Tx.commit(new_state.tx).mutations |> mutations_to_writes
      assert result_writes == %{"existing" => "value", "new_key" => "new_value"}

      # Verify other fields preserved
      assert new_state.state == :valid
      assert new_state.gateway == original_state.gateway
      assert new_state.transaction_system_layout == original_state.transaction_system_layout
      assert new_state.stack == original_state.stack
      assert new_state.fastest_storage_servers == original_state.fastest_storage_servers
    end

    test "handles large keys and values" do
      state = create_test_state()
      large_key = String.duplicate("k", 1000)
      large_value = String.duplicate("v", 10_000)

      {:ok, new_state} = Putting.do_put(state, large_key, large_value)

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{
               large_key => large_value
             }
    end

    test "works with binary data containing null bytes" do
      state = create_test_state()
      binary_key = "\x00\x01\xFF\x02"
      binary_value = "\xFF\x00\x01\x02"

      {:ok, new_state} = Putting.do_put(state, binary_key, binary_value)

      assert Tx.commit(new_state.tx).mutations |> mutations_to_writes == %{
               binary_key => binary_value
             }
    end
  end

  describe "error propagation" do
    defmodule FailingKeyCodec do
      def encode_key(_), do: :key_error
    end

    defmodule FailingValueCodec do
      def encode_value(_), do: :value_error
    end

    test "propagates key encoding errors" do
      state = %{create_test_state() | key_codec: FailingKeyCodec}

      result = Putting.do_put(state, "any_key", "value")

      assert result == :key_error
    end

    test "propagates value encoding errors" do
      state = %{create_test_state() | value_codec: FailingValueCodec}

      result = Putting.do_put(state, "key", "any_value")

      assert result == :value_error
    end
  end
end
