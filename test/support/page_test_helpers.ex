defmodule Bedrock.DataPlane.Storage.Olivine.PageTestHelpers do
  @moduledoc """
  Test helper functions for Page operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Page

  @doc """
  Alias for find_version_for_key/2 - looks up a key in a page.
  """
  @spec lookup_key_in_page(Page.t(), Bedrock.key()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def lookup_key_in_page(page, key), do: Page.find_version_for_key(page, key)

  @doc """
  Function for looking up key version using page map (used in tests).
  """
  @spec lookup_key_version(any(), Page.id(), Bedrock.key(), map()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def lookup_key_version(_version_manager, page_id, key, page_map) do
    case Map.fetch(page_map, page_id) do
      {:ok, page} -> Page.find_version_for_key(page, key)
      :error -> {:error, :not_found}
    end
  end

  @doc """
  Persists a single page to the database.
  """
  @spec persist_page_to_database(any(), Database.t(), Page.t()) :: :ok | {:error, term()}
  def persist_page_to_database(_version_manager, database, page) do
    binary = Page.from_map(page)
    Database.store_page(database, Page.id(page), binary)
  end

  @doc """
  Persists multiple pages to the database in batch.
  """
  @spec persist_pages_batch([Page.t()], Database.t()) :: :ok | {:error, term()}
  def persist_pages_batch(pages, database) do
    Enum.reduce_while(pages, :ok, fn page, :ok ->
      binary = Page.from_map(page)

      case Database.store_page(database, Page.id(page), binary) do
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

  @doc """
  Filters out the specified key from a page, returning key_versions without that key.
  """
  @spec filter_out_key(Page.t(), Bedrock.key()) :: [{binary(), Bedrock.version()}]
  def filter_out_key(page, key) do
    page
    |> Page.key_versions()
    |> Enum.reject(fn {page_key, _version} -> page_key == key end)
  end

  @doc """
  Checks if the page's keys are in sorted order.
  """
  @spec keys_are_sorted(Page.t() | binary()) :: boolean()
  def keys_are_sorted(page) do
    page_keys = Page.keys(page)
    page_keys == Enum.sort(page_keys)
  end

  @doc """
  Splits a page when it has more than 256 key-version pairs.
  Returns {{left_page, right_page}, updated_version_manager} or {:error, :no_split_needed}.
  """
  @spec split_page_simple(Page.t(), any()) :: {{Page.t(), Page.t()}, any()} | {:error, :no_split_needed}
  def split_page_simple(page, version_manager) do
    # Convert binary to struct first
    {:ok, page_struct} = Page.to_map(page)

    if length(page_struct.key_versions) <= 256 do
      {:error, :no_split_needed}
    else
      do_split_page_simple(page_struct, version_manager)
    end
  end

  defp do_split_page_simple(page_struct, version_manager) do
    key_versions = page_struct.key_versions
    mid_point = div(length(key_versions), 2)

    {left_key_versions, right_key_versions} = Enum.split(key_versions, mid_point)

    updated_vm = %{version_manager | max_page_id: max(version_manager.max_page_id, page_struct.id)}
    {right_id, vm1} = next_id_from_vm(updated_vm)

    left_id = page_struct.id

    left_page = Page.new(left_id, left_key_versions, right_id)
    right_page = Page.new(right_id, right_key_versions, page_struct.next_id)

    {{left_page, right_page}, vm1}
  end

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
  Adds a key-version pair to a page, maintaining sorted order.
  Updates existing keys or inserts new ones at the correct position.
  """
  @spec add_key_to_page(Page.t(), binary(), Bedrock.version()) :: Page.t()
  def add_key_to_page(page, key, version) when is_binary(version) and byte_size(version) == 8 do
    # Convert binary to struct, modify, then convert back
    {:ok, page_struct} = Page.to_map(page)

    case find_insertion_point(page_struct.key_versions, key) do
      {index, :duplicate} ->
        new_key_versions = List.replace_at(page_struct.key_versions, index, {key, version})
        Page.from_map(%{page_struct | key_versions: new_key_versions})

      {index, :new} ->
        new_key_versions = List.insert_at(page_struct.key_versions, index, {key, version})
        Page.from_map(%{page_struct | key_versions: new_key_versions})
    end
  end

  defp find_insertion_point(key_versions, key, index \\ 0)
  defp find_insertion_point([], _key, index), do: {index, :new}
  defp find_insertion_point([{h, _v} | _], key, index) when key < h, do: {index, :new}
  defp find_insertion_point([{h, _v} | _], key, index) when key == h, do: {index, :duplicate}
  defp find_insertion_point([_ | t], key, index), do: find_insertion_point(t, key, index + 1)

  @doc """
  Sets the next_id field of a page (binary or struct).
  """
  @spec set_next_id(Page.t(), Page.id()) :: Page.t()
  def set_next_id(page, next_id) when is_binary(page) do
    {:ok, page_struct} = Page.to_map(page)
    Page.from_map(%{page_struct | next_id: next_id})
  end

  def set_next_id(page, next_id) when is_map(page) do
    %{page | next_id: next_id}
  end

  @doc """
  Walks the page chain starting from page 0 and returns page IDs in chain order.

  This function is useful for testing chain integrity and verifying that page
  operations maintain proper chain structure.

  ## Examples

      iex> page_map = %{0 => %{next_id: 2}, 2 => %{next_id: 5}, 5 => %{next_id: 0}}
      iex> PageTestHelpers.walk_page_chain(page_map)
      [0, 2, 5]

      iex> PageTestHelpers.walk_page_chain(%{})
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
end
