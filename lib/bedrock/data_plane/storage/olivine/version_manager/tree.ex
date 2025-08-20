defmodule Bedrock.DataPlane.Storage.Olivine.VersionManager.Tree do
  @moduledoc """
  Tree operations for the Olivine storage driver.

  This module handles all tree-related operations including:
  - Building trees from page maps
  - Finding pages containing keys
  - Adding, removing, and updating pages in trees
  - Range queries using tree structure
  """

  alias Bedrock.DataPlane.Storage.Olivine.VersionManager.Page

  @type t :: t()

  @type page_id :: Page.id()
  @type page :: Page.page()

  @doc """
  Builds a page tree from a page map by adding each page to an empty tree.
  """
  @spec from_page_map(page_map :: map()) :: t()
  def from_page_map(page_map) do
    Enum.reduce(page_map, :gb_trees.empty(), fn {_page_id, page}, tree ->
      add_page_to_tree(tree, page.id, page.keys)
    end)
  end

  @doc """
  Finds the page that contains a specific key using the interval tree.
  Returns the page ID if found, or nil if no page contains the key.
  Uses direct pattern matching on GB-trees internal structure.

  Tree structure: key = last_key, value = {page_id, first_key}
  """
  @spec page_for_key(t(), Bedrock.key()) :: page_id() | nil
  def page_for_key({size, tree_node}, key) when size > 0, do: find_page_for_key(tree_node, key)
  def page_for_key(_, _key), do: nil

  defp find_page_for_key({last_key, {page_id, first_key}, _l, _r}, key) when first_key <= key and key <= last_key,
    do: page_id

  defp find_page_for_key({last_key, {_page_id, _first_key}, l, _r}, key) when key < last_key,
    do: find_page_for_key(l, key)

  defp find_page_for_key({_last_key, {_page_id, _first_key}, _l, r}, key), do: find_page_for_key(r, key)
  defp find_page_for_key(nil, _key), do: nil

  @doc """
  Finds the best page for inserting a key. First tries to find a page containing the key,
  then falls back to the rightmost page for keys beyond all existing ranges.
  """
  @spec page_for_insertion(t(), Bedrock.key()) :: page_id()
  def page_for_insertion(tree, key) do
    case page_for_key(tree, key) || find_rightmost_page(tree) do
      nil -> 0
      page_id -> page_id
    end
  end

  @doc """
  Finds the rightmost page in the tree (the page with the highest last_key).
  """
  @spec find_rightmost_page(t()) :: page_id() | nil
  def find_rightmost_page({size, tree_node}) when size > 0, do: find_rightmost_node(tree_node)
  def find_rightmost_page(_), do: nil

  defp find_rightmost_node({_last_key, {page_id, _first_key}, _l, nil}), do: page_id
  defp find_rightmost_node({_last_key, {_page_id, _first_key}, _l, r}), do: find_rightmost_node(r)

  @doc """
  Updates the interval tree by adding a new page range.
  """
  @spec add_page_to_tree(t(), page_id(), [Bedrock.key()]) :: t()
  def add_page_to_tree(tree, _page_id, []), do: tree

  def add_page_to_tree(tree, page_id, [first_key | _] = keys),
    do: :gb_trees.insert(List.last(keys), {page_id, first_key}, tree)

  @doc """
  Updates the interval tree by removing a page range.
  """
  @spec remove_page_from_tree(t(), page_id(), [Bedrock.key()]) :: t()
  def remove_page_from_tree(tree, _page_id, []), do: tree
  def remove_page_from_tree(tree, _page_id, [_first_key | _] = keys), do: :gb_trees.delete_any(List.last(keys), tree)

  @doc """
  Updates a page's position in the tree by removing the old range and adding the new range.
  Only updates if the range actually changed for efficiency.
  """
  @spec update_page_in_tree(t(), page_id(), [binary()], [binary()]) :: t()
  def update_page_in_tree(tree, page_id, old_keys, new_keys) do
    old_range = first_and_last(old_keys)
    new_range = first_and_last(new_keys)

    if old_range == new_range do
      tree
    else
      tree
      |> remove_page_from_tree(page_id, old_keys)
      |> add_page_to_tree(page_id, new_keys)
    end
  end

  defp first_and_last([]), do: nil
  defp first_and_last([first]), do: {first, first}
  defp first_and_last([first | rest]), do: {first, List.last(rest)}

  @doc """
  Finds all pages that overlap with the given query range.
  Uses tree for initial lookup, then walks page chain for optimal performance.

  Algorithm:
  1. Use tree to find first page containing query_start (O(log n))
  2. Walk page chain until page.first_key > query_end (O(k) where k = overlapping pages)

  Since we start from a page containing query_start, we only need query_end to know when to stop.
  Much more efficient than tree iteration since we leverage the page chain structure.
  """
  @spec stream_pages_in_range(
          t(),
          %{page_id() => page()},
          Bedrock.key(),
          Bedrock.key()
        ) :: Enumerable.t()
  def stream_pages_in_range(tree, page_map, query_start, query_end) do
    case page_for_key(tree, query_start) do
      nil -> []
      first_page_id -> walk_page_chain_for_overlaps(page_map, first_page_id, query_end)
    end
  end

  defp walk_page_chain_for_overlaps(page_map, first_page_id, query_end) do
    Stream.unfold(first_page_id, fn
      nil ->
        nil

      current_page_id ->
        page_map
        |> Map.fetch!(current_page_id)
        |> case do
          %{keys: [first_key | _], next_id: next_id} when first_key <= query_end ->
            {current_page_id, if(next_id == 0, do: nil, else: next_id)}

          _ ->
            nil
        end
    end)
  end
end
