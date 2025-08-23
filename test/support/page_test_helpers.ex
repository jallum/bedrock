defmodule Bedrock.DataPlane.Storage.Olivine.PageTestHelpers do
  @moduledoc """
  Test helper functions for Page operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Page

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
  Checks if the page's keys are in sorted order.
  """
  @spec keys_are_sorted(Page.t() | binary()) :: boolean()
  def keys_are_sorted(page) do
    page_keys = Page.keys(page)
    page_keys == Enum.sort(page_keys)
  end

  @doc """
  Adds a key-version pair to a page using the real Page.apply_operations.
  """
  @spec add_key_to_page(Page.t(), binary(), Bedrock.version()) :: Page.t()
  def add_key_to_page(page, key, version) when is_binary(version) and byte_size(version) == 8 do
    Page.apply_operations(page, %{key => {:set, version}})
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
