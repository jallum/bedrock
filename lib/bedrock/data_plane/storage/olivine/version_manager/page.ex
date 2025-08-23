defmodule Bedrock.DataPlane.Storage.Olivine.VersionManager.Page do
  @moduledoc """
  Page management routines for the Olivine storage driver.

  This module handles all page-related operations including:
  - Page creation, encoding, and decoding
  - Page splitting and key management
  - Tree operations for page lookup
  - Page chain walking and range queries
  """

  @type id :: non_neg_integer()
  @type page_map :: %{
          id: id(),
          next_id: id(),
          key_versions: [{binary(), Bedrock.version()}]
        }
  @type t :: binary() | page_map()
  @type operation :: {:set, Bedrock.version()} | :clear

  @doc """
  Creates a page with key-version tuples and optional next_id.
  Returns the binary-encoded page.
  """
  @spec new(id :: id(), key_versions :: [{binary(), Bedrock.version()}], next_id :: id()) :: binary()
  def new(id, key_versions, next_id \\ 0) when is_list(key_versions) do
    versions = Enum.map(key_versions, fn {_key, version} -> version end)

    if !Enum.all?(versions, &(is_binary(&1) and byte_size(&1) == 8)) do
      raise ArgumentError, "All versions must be 8-byte binary values"
    end

    # Use direct encoding for maximum efficiency
    encode_page_direct(id, next_id || 0, key_versions)
  end

  @spec from_map(t()) :: binary()
  def from_map(%{key_versions: key_versions, id: id, next_id: next_id}) do
    encode_page_direct(id, next_id, key_versions)
  end

  def from_map(binary) when is_binary(binary), do: binary

  # Direct page encoding using the efficient binary approach
  @spec encode_page_direct(id(), id(), [{binary(), Bedrock.version()}]) :: binary()
  defp encode_page_direct(id, next_id, key_versions) do
    key_count = length(key_versions)

    # Encode entries directly as iodata
    {entries_iodata, last_key_offset} = encode_entries(key_versions, 32)

    header = <<
      id::unsigned-big-64,
      next_id::unsigned-big-64,
      key_count::unsigned-big-16,
      last_key_offset::unsigned-big-32,
      0::unsigned-big-80
    >>

    # Single conversion from iodata to binary
    IO.iodata_to_binary([header | entries_iodata])
  end

  # Encode key-version pairs directly as iodata
  @spec encode_entries([{binary(), Bedrock.version()}], non_neg_integer()) :: {iodata(), non_neg_integer()}
  defp encode_entries(key_versions, base_offset) do
    encode_entries(key_versions, base_offset, [], 0)
  end

  @spec encode_entries([{binary(), Bedrock.version()}], non_neg_integer(), iodata(), non_neg_integer()) ::
          {iodata(), non_neg_integer()}
  defp encode_entries([], _base_offset, acc, last_key_offset) do
    {Enum.reverse(acc), last_key_offset}
  end

  defp encode_entries([{key, version} | rest], base_offset, acc, _last_key_offset) do
    entry = encode_version_key_entry(version, key)
    entry_size = byte_size(entry)

    # Last key offset points to the start of the key (after version and length)
    new_last_key_offset = base_offset + 8 + 2

    encode_entries(rest, base_offset + entry_size, [entry | acc], new_last_key_offset)
  end

  @doc """
  Applies a map of operations to a binary page, returning the updated binary page.
  Operations can be {:set, version} or :clear.
  """
  @spec apply_operations(binary(), %{Bedrock.key() => operation()}) :: binary()
  def apply_operations(page_binary, operations) when map_size(operations) == 0, do: page_binary

  def apply_operations(page_binary, operations) when is_binary(page_binary) do
    <<
      id::unsigned-big-64,
      next_id::unsigned-big-64,
      key_count::unsigned-big-16,
      last_key_offset::unsigned-big-32,
      _padding::unsigned-big-80,
      entries::binary
    >> = page_binary

    {parts, key_count_delta, last_key_info} = cruise_entries(entries, Enum.sort(operations), 32, [32], 0)
    new_entries = convert_parts_to_iodata(parts, page_binary, [])

    new_last_key_offset =
      case last_key_info do
        :unchanged -> last_key_offset
        :no_last_key -> 0
        key_length -> 32 + IO.iodata_length(new_entries) - key_length
      end

    header =
      <<id::unsigned-big-64, next_id::unsigned-big-64, key_count + key_count_delta::unsigned-big-16,
        new_last_key_offset::unsigned-big-32, 0::unsigned-big-80>>

    IO.iodata_to_binary([header | new_entries])
  end

  # Convert parts list to iodata, processing in reverse order to get correct sequence
  defp convert_parts_to_iodata([part | rest], page_binary, acc) do
    binary_part =
      case part do
        start_offset when is_integer(start_offset) ->
          binary_part(page_binary, start_offset, byte_size(page_binary) - start_offset)

        {start_offset, length} ->
          binary_part(page_binary, start_offset, length)

        binary when is_binary(binary) ->
          binary
      end

    convert_parts_to_iodata(rest, page_binary, [binary_part | acc])
  end

  defp convert_parts_to_iodata([], _page_binary, acc), do: acc

  # Cruise through entries binary using rest pattern, emitting slice specs and new entries
  @spec cruise_entries(
          binary(),
          [{binary(), operation()}],
          non_neg_integer(),
          list(),
          integer()
        ) :: {list(), integer(), integer() | :unchanged | :no_last_key}

  defp cruise_entries(
         <<_version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>> = entries,
         operations,
         current_offset,
         acc,
         key_count_delta
       ) do
    entry_size = 8 + 2 + key_len
    next_offset = current_offset + entry_size

    case operations do
      [{op_key, op} | remaining_ops] when op_key < key ->
        handle_insert_operation(
          op,
          op_key,
          entries,
          remaining_ops,
          current_offset,
          acc,
          key_count_delta
        )

      [{^key, op} | remaining_ops] ->
        handle_match_operation(
          op,
          key,
          rest,
          remaining_ops,
          current_offset,
          next_offset,
          acc,
          key_count_delta
        )

      _ ->
        # No operation for this key, continue cruising
        cruise_entries(rest, operations, next_offset, acc, key_count_delta)
    end
  end

  defp cruise_entries(_entries, [], _current_offset, acc, key_count_delta) do
    # No more operations - last key didn't change, so offset delta is 0
    {acc, key_count_delta, :unchanged}
  end

  defp cruise_entries(_entries, operations, current_offset, acc, key_count_delta) do
    # End of entries, close current slice and add any remaining operations
    new_acc = pop_slice_or_emit(acc, current_offset)

    # Add any remaining operations as new entries
    {final_parts, final_key_delta, last_key_offset_delta} =
      add_remaining_operations_as_parts(operations, new_acc, key_count_delta)

    {final_parts, final_key_delta, last_key_offset_delta}
  end

  # Handle operations where op_key < current binary key (insertions)
  defp handle_insert_operation(:clear, _op_key, entries, remaining_ops, current_offset, acc, key_count_delta) do
    # Clear operations for non-existent keys do nothing
    cruise_entries(entries, remaining_ops, current_offset, acc, key_count_delta)
  end

  defp handle_insert_operation(
         {:set, new_version},
         op_key,
         entries,
         remaining_ops,
         current_offset,
         acc,
         key_count_delta
       ) do
    new_entry = encode_version_key_entry(new_version, op_key)
    new_acc = [new_entry | pop_slice_or_emit(acc, current_offset)]

    cruise_entries(
      entries,
      remaining_ops,
      current_offset,
      [current_offset | new_acc],
      key_count_delta + 1
    )
  end

  # Handle operations where op_key == current binary key (exact matches)
  defp handle_match_operation(:clear, _key, rest, remaining_ops, current_offset, next_offset, acc, key_count_delta) do
    new_acc = [next_offset | pop_slice_or_emit(acc, current_offset)]
    cruise_entries(rest, remaining_ops, next_offset, new_acc, key_count_delta - 1)
  end

  defp handle_match_operation(
         {:set, new_version},
         key,
         rest,
         remaining_ops,
         current_offset,
         next_offset,
         acc,
         key_count_delta
       ) do
    new_entry = encode_version_key_entry(new_version, key)
    new_acc = [next_offset, new_entry | pop_slice_or_emit(acc, current_offset)]

    cruise_entries(rest, remaining_ops, next_offset, new_acc, key_count_delta)
  end

  # Helper function to pop slice start and either emit slice or just return tail
  defp pop_slice_or_emit(acc, current_offset) do
    case acc do
      [^current_offset | tail] -> tail
      [slice_start | tail] when is_integer(slice_start) -> [{slice_start, current_offset - slice_start} | tail]
      [binary_entry | tail] when is_binary(binary_entry) -> [binary_entry | tail]
    end
  end

  # Add remaining operations as new entries (parts version)
  @spec add_remaining_operations_as_parts([{binary(), operation()}], list(), integer()) ::
          {list(), integer(), integer() | :unchanged | :no_last_key}
  defp add_remaining_operations_as_parts([{_key, :clear} | tail], acc, key_count_delta),
    do: add_remaining_operations_as_parts(tail, acc, key_count_delta)

  defp add_remaining_operations_as_parts([{key, {:set, version}} | tail], acc, key_count_delta) do
    new_entry = encode_version_key_entry(version, key)

    if [] == tail do
      {[new_entry | acc], key_count_delta + 1, byte_size(key)}
    else
      add_remaining_operations_as_parts(tail, [new_entry | acc], key_count_delta + 1)
    end
  end

  defp add_remaining_operations_as_parts([], acc, key_count_delta) do
    if acc == [] and key_count_delta < 0 do
      {acc, key_count_delta, :no_last_key}
    else
      {acc, key_count_delta, :unchanged}
    end
  end

  # Encode a single version-key entry
  @spec encode_version_key_entry(Bedrock.version(), binary()) :: binary()
  defp encode_version_key_entry(version, key) when is_binary(version) and byte_size(version) == 8 do
    key_len = byte_size(key)
    <<version::binary-size(8), key_len::unsigned-big-16, key::binary>>
  end

  @spec to_map(binary()) :: {:ok, t()} | {:error, :invalid_page}
  def to_map(binary) do
    with <<id::unsigned-big-64, next_id::unsigned-big-64, key_count::unsigned-big-16, _last_key_offset::unsigned-big-32,
           _reserved::unsigned-big-80, entries_data::binary>> <- binary,
         {:ok, key_versions} <- decode_entries(entries_data, key_count, []) do
      {:ok,
       %{
         id: id,
         next_id: next_id,
         key_versions: key_versions
       }}
    else
      _ -> {:error, :invalid_page}
    end
  end

  # Decode interleaved version-key entries
  @spec decode_entries(binary(), non_neg_integer(), [{binary(), Bedrock.version()}]) ::
          {:ok, [{binary(), Bedrock.version()}]} | {:error, :invalid_entries}
  defp decode_entries(_data, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_entries(
         <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         acc
       ) do
    decode_entries(rest, count - 1, [{key, version} | acc])
  end

  defp decode_entries(_, _, _), do: {:error, :invalid_entries}

  @spec id(t()) :: id()
  def id(<<page_id::unsigned-big-64, _rest::binary>>), do: page_id
  def id(%{id: page_id}), do: page_id

  @spec next_id(t()) :: id()
  def next_id(<<_page_id::unsigned-big-64, next_page_id::unsigned-big-64, _rest::binary>>), do: next_page_id
  def next_id(%{next_id: next_page_id}), do: next_page_id

  @spec key_versions(t()) :: [{binary(), Bedrock.version()}]
  def key_versions(
        <<_page_id::unsigned-big-64, _next_id::unsigned-big-64, key_count::unsigned-big-16,
          _last_key_offset::unsigned-big-32, _reserved::unsigned-big-80, entries_data::binary>>
      ) do
    {:ok, key_versions} = decode_entries(entries_data, key_count, [])
    key_versions
  end

  def key_versions(%{key_versions: kvs}), do: kvs

  @spec key_count(t()) :: non_neg_integer()
  def key_count(<<_page_id::unsigned-big-64, _next_id::unsigned-big-64, key_count::unsigned-big-16, _rest::binary>>),
    do: key_count

  def key_count(%{key_versions: kvs}), do: length(kvs)

  @spec keys(t()) :: [binary()]
  def keys(page_binary) when is_binary(page_binary) do
    page_binary |> key_versions() |> Enum.map(fn {key, _version} -> key end)
  end

  @spec extract_keys(t()) :: [binary()]
  def extract_keys(page), do: keys(page)

  @spec left_key(t()) :: binary() | nil
  def left_key(<<_page_id::unsigned-big-64, _next_id::unsigned-big-64, 0::unsigned-big-16, _rest::binary>>), do: nil

  def left_key(
        <<_page_id::unsigned-big-64, _next_id::unsigned-big-64, _key_count::unsigned-big-16,
          _last_key_offset::unsigned-big-32, _reserved::unsigned-big-80, _version::binary-size(8),
          key_len::unsigned-big-16, key::binary-size(key_len), _rest::binary>>
      ) do
    key
  end

  @spec right_key(t()) :: binary() | nil
  # O(1) access using last_key_offset - read from offset to end of binary
  def right_key(<<_page_id::unsigned-big-64, _next_id::unsigned-big-64, 0::unsigned-big-16, _rest::binary>>), do: nil

  def right_key(
        <<_page_id::unsigned-big-64, _next_id::unsigned-big-64, _key_count::unsigned-big-16,
          last_key_offset::unsigned-big-32, _reserved::unsigned-big-80, _data::binary-size(last_key_offset - 32),
          key_len::unsigned-big-16, key::binary-size(key_len)>>
      ) do
    key
  end

  def right_key(page_binary) when is_binary(page_binary) do
    # Fallback: decode all key_versions and get the last one
    case key_versions(page_binary) do
      [] ->
        nil

      key_versions ->
        {key, _version} = List.last(key_versions)
        key
    end
  end

  @spec find_version_for_key(t(), Bedrock.key()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def find_version_for_key(page_binary, key) when is_binary(page_binary), do: find_key_in_binary(page_binary, key)

  # Helper to search for a key in binary format
  @spec find_key_in_binary(binary(), Bedrock.key()) :: {:ok, Bedrock.version()} | {:error, :not_found} | :not_found
  defp find_key_in_binary(
         <<_page_id::unsigned-big-64, _next_id::unsigned-big-64, key_count::unsigned-big-16,
           _last_key_offset::unsigned-big-32, _reserved::unsigned-big-80, entries_data::binary>>,
         target_key
       ) do
    search_entries_for_key(entries_data, key_count, target_key)
  end

  @spec search_entries_for_key(binary(), non_neg_integer(), Bedrock.key()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  defp search_entries_for_key(_data, 0, _target_key), do: {:error, :not_found}

  defp search_entries_for_key(
         <<version::binary-size(8), key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         count,
         target_key
       ) do
    if key == target_key do
      {:ok, version}
    else
      search_entries_for_key(rest, count - 1, target_key)
    end
  end

  defp search_entries_for_key(_, _, _), do: :not_found

  # Alias for backward compatibility with tests

  @spec stream_key_versions_in_range(Enumerable.t(t()), Bedrock.key(), Bedrock.key()) ::
          Enumerable.t({Bedrock.key(), Bedrock.version()})
  def stream_key_versions_in_range(pages_stream, start_key, end_key) do
    pages_stream
    |> Stream.flat_map(fn page -> key_versions(page) end)
    |> Stream.drop_while(fn {key, _version} -> key < start_key end)
    |> Stream.take_while(fn {key, _version} -> key < end_key end)
  end

  @doc """
  Checks if a page contains a specific key.
  """
  @spec has_key?(t() | binary(), Bedrock.key()) :: boolean()
  def has_key?(page_binary, key) when is_binary(page_binary) do
    case find_key_in_binary(page_binary, key) do
      {:ok, _version} -> true
      _ -> false
    end
  end

  @doc """
  Checks if a page is empty (has no keys).
  """
  @spec empty?(t() | binary()) :: boolean()
  def empty?(page), do: key_count(page) == 0

  @doc """
  Checks if two pages are the same page (same ID).
  """
  @spec same_page?(t() | binary(), t() | binary()) :: boolean()
  def same_page?(page1, page2), do: id(page1) == id(page2)

  @doc """
  Checks if a page has the given ID.
  """
  @spec has_id?(t() | binary(), id()) :: boolean()
  def has_id?(page, page_id), do: id(page) == page_id

  @doc """
  Splits a page at the given key offset, creating two new pages.
  Returns {left_page, right_page} where left_page keeps the original ID
  and right_page gets the new_page_id.
  """
  @spec split_page(binary(), non_neg_integer(), id()) :: {binary(), binary()}
  def split_page(page_binary, key_offset, new_page_id) do
    <<page_id::64, next_id::64, _key_count::16, _last_key_offset::32, _reserved::80, _entries::binary>> = page_binary

    # Get all key-version pairs
    key_versions = key_versions(page_binary)

    # Split at the specified offset
    {left_key_versions, right_key_versions} = Enum.split(key_versions, key_offset)

    # Create two new pages
    left_page = new(page_id, left_key_versions, new_page_id)
    right_page = new(new_page_id, right_key_versions, next_id)

    {left_page, right_page}
  end
end
