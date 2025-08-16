defmodule Bedrock.DataPlane.BedrockTransactionTestSupport do
  @moduledoc """
  Test support utilities for creating BedrockTransaction instances.

  This module provides convenience functions for tests that need to create
  simple transactions from key-value pairs. Production code should use the
  proper transaction builder system and BedrockTransaction.encode() directly.
  """

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Version

  @doc """
  Creates a log-style transaction for testing purposes.

  This creates a transaction with mutations from the given key-value pairs
  and sets the commit version. This is primarily intended for test scenarios
  where you need a simple transaction.

  ## Examples

      iex> encoded = new_log_transaction(0, %{"key1" => "value1"})
      iex> is_binary(encoded)
      true

      iex> encoded = new_log_transaction(42, %{"key1" => "value1", "key2" => nil})
      iex> BedrockTransaction.extract_log_version(encoded) == <<42::64>>
      true
  """
  @spec new_log_transaction(
          Bedrock.version() | integer(),
          %{Bedrock.key() => Bedrock.value() | nil} | [{Bedrock.key(), Bedrock.key() | nil}]
        ) :: BedrockTransaction.encoded()
  def new_log_transaction(version, key_values) when is_map(key_values) do
    mutations = convert_writes_to_mutations(key_values)
    transaction = %{mutations: mutations}
    encoded = BedrockTransaction.encode(transaction)

    # Ensure version is in binary format
    version_binary =
      if is_binary(version) and byte_size(version) == 8,
        do: version,
        else: Version.from_integer(version)

    {:ok, with_version} = BedrockTransaction.add_commit_version(encoded, version_binary)
    with_version
  end

  def new_log_transaction(version, key_values) when is_list(key_values) do
    new_log_transaction(version, Map.new(key_values))
  end

  @doc """
  Extracts the commit version from a transaction created with new_log_transaction.
  """
  @spec extract_log_version(BedrockTransaction.encoded()) :: binary() | nil
  def extract_log_version(encoded_transaction) do
    case BedrockTransaction.extract_commit_version(encoded_transaction) do
      {:ok, version} -> version
      {:error, _} -> nil
    end
  end

  @doc """
  Extracts the key-value writes from a transaction created with new_log_transaction.
  """
  @spec extract_log_writes(BedrockTransaction.encoded()) :: %{binary() => binary() | nil}
  def extract_log_writes(encoded_transaction) do
    case BedrockTransaction.stream_mutations(encoded_transaction) do
      {:ok, mutations_stream} ->
        mutations_stream
        |> Enum.reduce(%{}, fn
          {:set, key, value}, acc -> Map.put(acc, key, value)
          {:clear, key}, acc -> Map.put(acc, key, nil)
          {:clear_range, _start, _end}, acc -> acc
        end)

      {:error, _} ->
        %{}
    end
  end

  # Private helper function
  defp convert_writes_to_mutations(key_values) when is_map(key_values) do
    Enum.map(key_values, fn
      {key, nil} -> {:clear_range, key, key <> <<0>>}
      {key, value} -> {:set, key, value}
    end)
  end
end
