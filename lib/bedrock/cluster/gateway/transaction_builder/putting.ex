defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Putting do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @spec do_put(State.t(), Bedrock.key(), nil) :: {:ok, State.t()} | :key_error
  def do_put(t, key, nil) do
    with {:ok, encoded_key} <- t.key_codec.encode_key(key) do
      t.tx
      |> Tx.clear(encoded_key)
      |> then(&{:ok, %{t | tx: &1}})
    end
  end

  @spec do_put(State.t(), Bedrock.key(), Bedrock.value()) :: {:ok, State.t()} | :key_error
  def do_put(t, key, value) do
    with {:ok, encoded_key} <- t.key_codec.encode_key(key),
         {:ok, encoded_value} <- t.value_codec.encode_value(value) do
      t.tx
      |> Tx.set(encoded_key, encoded_value)
      |> then(&{:ok, %{t | tx: &1}})
    end
  end
end
