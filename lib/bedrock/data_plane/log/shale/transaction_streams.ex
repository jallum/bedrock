defmodule Bedrock.DataPlane.Log.Shale.TransactionStreams do
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log.Shale.Segment

  @wal_magic_number <<"BED0">>
  @wal_eof_version 0xFFFFFFFFFFFFFFFF

  @spec from_segments([Segment.t()], Bedrock.version()) ::
          {:ok, Enumerable.t()} | {:error, :not_found}
  def from_segments([], _target_version), do: {:error, :not_found}

  def from_segments([segment | segments], target_version)
      when segment.min_version > target_version do
    case from_segments(segments, target_version) do
      {:ok, stream} ->
        {:ok,
         Stream.concat(
           stream,
           from_list_of_transactions(fn ->
             segment
             |> Segment.transactions()
             |> Enum.reverse()
           end)
         )}

      error ->
        error
    end
  end

  def from_segments([segment | _segments], target_version) do
    segment
    |> Segment.transactions()
    |> Enum.reverse()
    |> Enum.drop_while(fn transaction ->
      case BedrockTransaction.extract_commit_version(transaction) do
        {:ok, version} -> version < target_version
        # Skip invalid transactions
        {:error, _} -> true
      end
    end)
    |> case do
      [transaction | rest] ->
        case BedrockTransaction.extract_commit_version(transaction) do
          {:ok, version} when version == target_version ->
            {:ok, from_list_of_transactions(fn -> rest end)}

          _ ->
            {:error, :not_found}
        end

      _ ->
        {:error, :not_found}
    end
  end

  @spec from_list_of_transactions((-> [BedrockTransaction.encoded()] | nil)) :: Enumerable.t()
  def from_list_of_transactions(transactions_fn) do
    Stream.resource(
      transactions_fn,
      fn
        transactions when is_list(transactions) ->
          {transactions, nil}

        nil ->
          {:halt, nil}
      end,
      fn nil -> :ok end
    )
  end

  @doc """
  Streams transactions from the segment.

  This function returns a Stream that iterates through the transactions
  in the given segment. Each transaction is validated using its checksum
  (CRC32), and the stream yields either valid transactions, identifies
  end-of-file markers, or flags corrupted data. Offsets are tracked so that
  append operations can be performed safely.
  """
  @spec from_file!(path_to_file :: String.t()) ::
          Enumerable.t({BedrockTransaction.encoded() | :eof | :corrupted, non_neg_integer()})
  def from_file!(path_to_file) do
    Stream.resource(
      fn ->
        <<@wal_magic_number, bytes::binary>> = File.read!(path_to_file)
        {4, bytes}
      end,
      fn
        {:error, _reason} = error ->
          {:halt, error}

        {offset,
         <<version::unsigned-big-64, size_in_bytes::unsigned-big-32,
           payload::binary-size(size_in_bytes), crc32::unsigned-big-32,
           remaining_bytes::binary>> = bytes} ->
          cond do
            @wal_eof_version == version ->
              {:halt, nil}

            :erlang.crc32(payload) == crc32 ->
              {[binary_part(bytes, 0, 16 + size_in_bytes)],
               {offset + 16 + size_in_bytes, remaining_bytes}}

            true ->
              nil
          end

        {_, offset} ->
          error = {:error, {:corrupted, offset}}
          {[error], error}
      end,
      fn _ -> :ok end
    )
  end

  @moduledoc """
  A module for handling transaction streams with operations like limiting,
  filtering, and halting based on conditions.
  """

  @spec until_version(Enumerable.t(), Bedrock.version()) :: Enumerable.t()
  def until_version(stream, nil), do: stream

  def until_version(stream, last_version) do
    Stream.transform(stream, last_version, fn
      encoded_transaction, last_version ->
        case BedrockTransaction.extract_commit_version(encoded_transaction) do
          {:ok, version} when version <= last_version ->
            {[encoded_transaction], last_version}

          _ ->
            {:halt, nil}
        end
    end)
  end

  @doc """
  Limits the number of transactions in the stream based on the given version.
  """
  @spec at_most(Enumerable.t(), pos_integer()) :: Enumerable.t()
  def at_most(stream, limit) do
    Stream.transform(stream, limit, fn
      transaction, 0 -> {:halt, transaction}
      transaction, n -> {[transaction], n - 1}
    end)
  end

  @doc """
  Filters transaction streams based on key range.
  """
  @spec filter_keys_in_range(Enumerable.t(), Bedrock.key_range()) :: Enumerable.t()
  def filter_keys_in_range(stream, nil), do: stream

  def filter_keys_in_range(stream, {start_key, end_key}) do
    Stream.map(stream, &filter_transaction_keys(&1, start_key, end_key))
  end

  @doc "Excludes transaction values when the flag is true."
  @spec exclude_values(Enumerable.t(), boolean()) :: Enumerable.t()
  def exclude_values(stream, false), do: stream

  def exclude_values(stream, true) do
    Stream.map(stream, &exclude_transaction_values/1)
  end

  # Helper functions to reduce nesting
  defp filter_transaction_keys(transaction, start_key, end_key) do
    case BedrockTransaction.stream_mutations(transaction) do
      {:ok, mutations_stream} ->
        filtered_mutations = filter_mutations_by_key_range(mutations_stream, start_key, end_key)
        rebuild_transaction_with_mutations(transaction, filtered_mutations)

      {:error, _} ->
        transaction
    end
  end

  defp exclude_transaction_values(transaction) do
    case BedrockTransaction.stream_mutations(transaction) do
      {:ok, mutations_stream} ->
        filtered_mutations = exclude_mutation_values(mutations_stream)
        rebuild_transaction_with_mutations(transaction, filtered_mutations)

      {:error, _} ->
        transaction
    end
  end

  defp filter_mutations_by_key_range(mutations_stream, start_key, end_key) do
    mutations_stream
    |> Enum.filter(fn
      {:set, key, _value} ->
        key >= start_key and key < end_key

      {:clear, key} ->
        key >= start_key and key < end_key

      {:clear_range, start_k, end_k} ->
        # Include if range overlaps with filter range
        start_k < end_key and end_k > start_key
    end)
  end

  defp exclude_mutation_values(mutations_stream) do
    mutations_stream
    |> Enum.map(fn
      {:set, key, _value} -> {:set, key, <<>>}
      {:clear, key} -> {:clear, key}
      {:clear_range, start_key, end_key} -> {:clear_range, start_key, end_key}
    end)
  end

  defp rebuild_transaction_with_mutations(transaction, filtered_mutations) do
    {:ok, commit_version} = BedrockTransaction.extract_commit_version(transaction)
    new_transaction = %{mutations: filtered_mutations}
    encoded = BedrockTransaction.encode(new_transaction)
    {:ok, with_version} = BedrockTransaction.add_commit_version(encoded, commit_version)
    with_version
  end
end
