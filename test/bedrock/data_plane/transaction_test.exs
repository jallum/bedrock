defmodule Bedrock.DataPlane.Log.TransactionTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Transaction

  describe "new/1" do
    test "creates a new transaction" do
      assert {0, %{key: "value"}} = Transaction.new(0, %{key: "value"})
    end
  end

  describe "version/1" do
    test "returns the version of the transaction" do
      tx = Transaction.new(0, %{key: "value"})
      assert 0 == Transaction.version(tx)
    end

    test "returns the version of an encoded transaction" do
      tx = Transaction.new(0, %{key: "value"})
      encoded_tx = Transaction.encode(tx)
      assert 0 == Transaction.version(encoded_tx)
    end
  end

  describe "key_values/1" do
    test "returns the key values of the transaction" do
      tx = Transaction.new(0, %{key: "value"})
      assert %{key: "value"} = Transaction.key_values(tx)
    end

    test "returns the key values of an encoded transaction" do
      tx = Transaction.new(0, %{key: "value"})
      encoded_tx = Transaction.encode(tx)
      assert %{key: "value"} = Transaction.key_values(encoded_tx)
    end
  end

  describe "encode/1" do
    test "encodes a transaction" do
      {encoded_version, encoded_key_values} =
        Transaction.new(0, %{key: "value"}) |> Transaction.encode()

      assert is_binary(encoded_version)
      assert is_binary(encoded_key_values)
    end

    test "encoding an already encoded transaction does nothing" do
      encoded_tx = Transaction.new(0, %{key: "value"}) |> Transaction.encode()
      assert encoded_tx == Transaction.encode(encoded_tx)
    end
  end

  describe "decode/1" do
    test "decodes a previously encoded transaction" do
      tx = Transaction.new(0, %{key: "value"})
      encoded_tx = Transaction.encode(tx)
      assert tx == Transaction.decode(encoded_tx)
    end

    test "decoding an already decoded transaction does nothing" do
      tx = Transaction.new(0, %{key: "value"})
      assert tx == Transaction.decode(tx)
    end
  end
end
