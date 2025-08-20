defmodule Bedrock.DataPlane.Storage.Olivine.VersionManager.Page do
  @moduledoc """
  Page management routines for the Olivine storage driver.

  This module handles all page-related operations including:
  - Page creation, encoding, and decoding
  - Page splitting and key management
  - Tree operations for page lookup
  - Page chain walking and range queries
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Version

  @type id :: non_neg_integer()
  @type page :: %{
          id: id(),
          next_id: id(),
          keys: [binary()],
          versions: [Bedrock.version()]
        }

  @spec new(id :: id(), keys :: [binary()]) :: page()
  def new(id, keys), do: new(id, keys, Enum.map(keys, fn _ -> Version.zero() end))

  @spec new(id :: id(), keys :: [binary()], versions :: [Bedrock.version()]) :: page()
  def new(id, keys, versions) when length(keys) == length(versions) do
    if !Enum.all?(versions, &(is_binary(&1) and byte_size(&1) == 8)) do
      raise ArgumentError, "All versions must be 8-byte binary values"
    end

    %{
      id: id,
      next_id: 0,
      keys: keys,
      versions: versions
    }
  end

  @spec to_binary(page()) :: binary()
  def to_binary(page) do
    key_count = length(page.keys)
    encoded_keys = encode_keys(page.keys)
    last_key_offset = if key_count > 0, do: 32 + key_count * 8 + byte_size(encoded_keys) - 2, else: 0

    encoded_versions = for version <- page.versions, into: <<>>, do: version

    header = <<
      page.id::unsigned-big-64,
      page.next_id::unsigned-big-64,
      key_count::unsigned-big-32,
      last_key_offset::unsigned-big-32,
      0::unsigned-big-64
    >>

    <<header::binary, encoded_versions::binary, encoded_keys::binary>>
  end

  @spec encode_keys(keys :: [binary()]) :: binary()
  defp encode_keys(keys), do: for(key <- keys, into: <<>>, do: <<byte_size(key)::unsigned-big-16, key::binary>>)

  @spec from_binary(binary()) :: {:ok, page()} | {:error, :invalid_page}
  def from_binary(binary) do
    with <<id::unsigned-big-64, next_id::unsigned-big-64, key_count::unsigned-big-32, _last_key_offset::unsigned-big-32,
           _reserved::unsigned-big-64, encoded_versions::binary-size(key_count * 8), encoded_keys::binary>> <- binary,
         {:ok, versions} <- decode_versions(encoded_versions),
         {:ok, keys} <- decode_keys(encoded_keys) do
      {:ok,
       %{
         id: id,
         next_id: next_id,
         keys: keys,
         versions: versions
       }}
    else
      _ -> {:error, :invalid_page}
    end
  end

  @spec decode_keys(encoded_keys :: binary()) :: {:ok, [binary()]} | {:error, :invalid_keys}
  def decode_keys(encoded_keys), do: decode_keys(encoded_keys, [])

  defp decode_keys(<<key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>, acc),
    do: decode_keys(rest, [key | acc])

  defp decode_keys(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_keys(_, _acc), do: {:error, :invalid_keys}

  @spec decode_versions(encoded_versions :: binary()) :: {:ok, [Bedrock.version()]} | {:error, :invalid_versions}
  def decode_versions(encoded_versions), do: decode_versions(encoded_versions, [])

  defp decode_versions(<<version::binary-8, rest::binary>>, acc), do: decode_versions(rest, [version | acc])
  defp decode_versions(<<>>, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_versions(_, _acc), do: {:error, :invalid_versions}

  @spec key_count(page :: page()) :: non_neg_integer()
  def key_count(page), do: length(page.keys)

  @spec keys(page :: page()) :: [binary()]
  def keys(page), do: page.keys

  @spec lookup_key_in_page(page :: page(), key :: binary()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def lookup_key_in_page(page, key) do
    case Enum.find_index(page.keys, &(&1 == key)) do
      nil -> {:error, :not_found}
      index -> {:ok, Enum.at(page.versions, index)}
    end
  end

  @spec add_key_to_page(page :: page(), key :: binary(), version :: Bedrock.version()) :: page()
  def add_key_to_page(page, key, version) when is_binary(version) and byte_size(version) == 8 do
    case find_insertion_point(page.keys, key) do
      {index, :duplicate} ->
        new_versions = List.replace_at(page.versions, index, version)
        %{page | versions: new_versions}

      {index, :new} ->
        new_keys = List.insert_at(page.keys, index, key)
        new_versions = List.insert_at(page.versions, index, version)
        %{page | keys: new_keys, versions: new_versions}
    end
  end

  defp find_insertion_point(keys, key, index \\ 0)
  defp find_insertion_point([], _key, index), do: {index, :new}
  defp find_insertion_point([h | _], key, index) when key < h, do: {index, :new}
  defp find_insertion_point([h | _], key, index) when key == h, do: {index, :duplicate}
  defp find_insertion_point([_ | t], key, index), do: find_insertion_point(t, key, index + 1)

  @spec split_page_simple(page :: page(), version_manager :: any()) ::
          {{page(), page()}, any()} | {:error, :no_split_needed}
  def split_page_simple(page, version_manager) when length(page.keys) > 256 do
    keys = page.keys
    versions = page.versions
    mid_point = div(length(keys), 2)

    {left_keys, right_keys} = Enum.split(keys, mid_point)
    {left_versions, right_versions} = Enum.split(versions, mid_point)

    updated_vm = %{version_manager | max_page_id: max(version_manager.max_page_id, page.id)}
    {right_id, vm1} = next_id_from_vm(updated_vm)

    left_id = page.id

    left_page = new(left_id, left_keys, left_versions)
    right_page = %{new(right_id, right_keys, right_versions) | next_id: page.next_id}
    left_page = %{left_page | next_id: right_id}

    {{left_page, right_page}, vm1}
  end

  def split_page_simple(_page, _version_manager), do: {:error, :no_split_needed}

  defp next_id_from_vm(version_manager) do
    case version_manager.free_page_ids do
      [id | rest] ->
        {id, %{version_manager | free_page_ids: rest}}

      [] ->
        new_id = version_manager.max_page_id + 1
        {new_id, %{version_manager | max_page_id: new_id}}
    end
  end

  @doc """
  Walks the page chain starting from page 0 and returns page IDs in chain order.

  This function is useful for testing chain integrity and verifying that page
  operations maintain proper chain structure.

  ## Examples

      iex> page_map = %{0 => %{next_id: 2}, 2 => %{next_id: 5}, 5 => %{next_id: 0}}
      iex> Page.walk_page_chain(page_map)
      [0, 2, 5]

      iex> Page.walk_page_chain(%{})
      []
  """
  @spec walk_page_chain(map()) :: [non_neg_integer()]
  def walk_page_chain(page_map) when is_map_key(page_map, 0), do: walk_page_chain_from(page_map, 0, [])
  def walk_page_chain(_), do: []

  defp walk_page_chain_from(page_map, id, acc) do
    page = Map.fetch!(page_map, id)

    case page do
      %{next_id: 0} ->
        Enum.reverse([id | acc])

      %{next_id: next_id} ->
        walk_page_chain_from(page_map, next_id, [id | acc])
    end
  end

  @spec stream_keys_in_range(Enumerable.t(page()), Bedrock.key(), Bedrock.key()) ::
          Enumerable.t({Bedrock.key(), Bedrock.version()})
  def stream_keys_in_range(pages_stream, start_key, end_key) do
    pages_stream
    |> Stream.flat_map(fn page ->
      Enum.zip(page.keys, page.versions)
    end)
    |> Stream.drop_while(fn {key, _version} -> key < start_key end)
    |> Stream.take_while(fn {key, _version} -> key < end_key end)
  end

  @spec load_and_search_page_simple_with_loader(any(), id(), Bedrock.key(), map(), function()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  def load_and_search_page_simple_with_loader(_version_manager, id, key, page_map, load_value_fn) do
    page = Map.fetch!(page_map, id)

    case lookup_key_in_page(page, key) do
      {:ok, found_version} ->
        load_value_fn.(key, found_version)

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @spec load_and_search_page_range_simple_with_loader(
          id(),
          Bedrock.key(),
          Bedrock.key(),
          map(),
          (Bedrock.key(), Bedrock.version() -> {:ok, Bedrock.value()} | {:error, :not_found})
        ) :: [{Bedrock.key(), Bedrock.value()}]
  def load_and_search_page_range_simple_with_loader(id, start_key, end_key, page_map, load_value_fn) do
    page = Map.fetch!(page_map, id)

    keys_in_range =
      Enum.filter(page.keys, fn key -> key >= start_key and key <= end_key end)

    loaded_results =
      Enum.reduce(keys_in_range, [], fn key, acc ->
        case load_key_value_pair(page, key, load_value_fn) do
          {key, value} -> [{key, value} | acc]
          nil -> acc
        end
      end)

    Enum.reverse(loaded_results)
  end

  defp load_key_value_pair(page, key, load_value_fn) do
    case lookup_key_in_page(page, key) do
      {:ok, found_version} ->
        load_value_with_version(key, found_version, load_value_fn)

      {:error, :not_found} ->
        nil
    end
  end

  defp load_value_with_version(key, version, load_value_fn) do
    case load_value_fn.(key, version) do
      {:ok, value} -> {key, value}
      {:error, :not_found} -> nil
    end
  end

  @spec persist_page_to_database(any(), Database.t(), page :: page()) ::
          :ok | {:error, term()}
  def persist_page_to_database(_version_manager, database, page) do
    binary = to_binary(page)
    Database.store_page(database, page.id, binary)
  end

  @spec persist_pages_batch([page()], Database.t()) :: :ok | {:error, term()}
  def persist_pages_batch(pages, database) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      binary = to_binary(page)

      case Database.store_page(database, page.id, binary) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc """
  Determines if two ranges overlap using the exact algorithm from the specification.
  Returns true if ranges [AStart, AEnd] and [BStart, BEnd] overlap.
  """
  @spec ranges_overlap(Bedrock.key(), Bedrock.key(), Bedrock.key(), Bedrock.key()) :: boolean()
  def ranges_overlap(a_start, a_end, b_start, b_end) do
    not (a_end < b_start or b_end < a_start)
  end
end
