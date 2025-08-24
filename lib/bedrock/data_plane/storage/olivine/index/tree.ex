defmodule Bedrock.DataPlane.Storage.Olivine.Index.Tree do
  @moduledoc """
  Tree operations for the Olivine storage driver.

  This module handles all tree-related operations including:
  - Building trees from page maps
  - Finding pages containing keys
  - Adding, removing, and updating pages in trees
  - Range queries using tree structure
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  @type t :: t()

  @type page_id :: Page.id()
  @type page :: Page.t()

  @doc """
  Builds a page tree from a page map by adding each page to an empty tree.
  """
  @spec from_page_map(page_map :: map()) :: t()
  def from_page_map(page_map) do
    Enum.reduce(page_map, :gb_trees.empty(), fn {_page_id, page}, tree ->
      add_page_to_tree(tree, page)
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
  @spec add_page_to_tree(t(), page()) :: t()
  def add_page_to_tree(tree, page) do
    case {Page.left_key(page), Page.right_key(page)} do
      # Empty page, don't add to tree
      {nil, nil} -> tree
      {first_key, last_key} -> :gb_trees.insert(last_key, {Page.id(page), first_key}, tree)
    end
  end

  @doc """
  Updates the interval tree by removing a page range.
  """
  @spec remove_page_from_tree(t(), page()) :: t()
  def remove_page_from_tree(tree, page) do
    case Page.right_key(page) do
      # Empty page, nothing to remove
      nil -> tree
      last_key -> :gb_trees.delete_any(last_key, tree)
    end
  end

  @doc """
  Updates a page's position in the tree by removing the old range and adding the new range.
  Only updates if the range actually changed for efficiency.
  """
  @spec update_page_in_tree(t(), page(), page()) :: t()
  def update_page_in_tree(tree, old_page, new_page) do
    old_range = {Page.left_key(old_page), Page.right_key(old_page)}
    new_range = {Page.left_key(new_page), Page.right_key(new_page)}

    if old_range == new_range do
      tree
    else
      tree
      |> remove_page_from_tree(old_page)
      |> add_page_to_tree(new_page)
    end
  end

  @doc """
  Returns page IDs for all pages that overlap with the given query range.
  Uses boundary-based tree traversal with minimal comparisons for optimal efficiency.

  Algorithm:
  1. find_and_collect_overlapping: Navigate tree seeking pages that overlap [query_start, query_end)
  2. collect_all_until_boundary: Once found, collect all remaining pages until boundary (no overlap checks!)

  This is O(log n + k) where k is the number of overlapping pages.
  Minimizes comparisons by switching to collection mode after finding boundaries.
  """
  @spec page_ids_in_range(t(), Bedrock.key(), Bedrock.key()) :: Enumerable.t({Bedrock.key(), page_id()})
  def page_ids_in_range({size, tree_node}, query_start, query_end) when size > 0,
    do: collect_pages_in_range(tree_node, query_start, query_end)

  def page_ids_in_range(_, _query_start, _query_end), do: []

  defp collect_pages_in_range(tree_node, query_start, query_end) do
    tree_node
    |> find_and_collect_overlapping(query_start, query_end, [])
    |> Enum.reverse()
  end

  defp find_and_collect_overlapping(nil, _query_start, _query_end, acc), do: acc

  defp find_and_collect_overlapping({last_key, _, _left, right}, query_start, query_end, acc)
       when last_key < query_start,
       do: find_and_collect_overlapping(right, query_start, query_end, acc)

  defp find_and_collect_overlapping({last_key, {page_id, first_key}, left, right}, query_start, query_end, acc) do
    acc_after_left = find_and_collect_overlapping(left, query_start, query_end, acc)

    if first_key <= query_end and last_key >= query_start do
      acc_with_current = [page_id | acc_after_left]
      collect_all_until_boundary(right, query_end, acc_with_current)
    else
      find_and_collect_overlapping(right, query_start, query_end, acc_after_left)
    end
  end

  defp collect_all_until_boundary(nil, _query_end, acc), do: acc

  defp collect_all_until_boundary({_last_key, {page_id, first_key}, left, right}, query_end, acc) do
    if first_key > query_end do
      acc
    else
      acc_after_left = collect_all_until_boundary(left, query_end, acc)
      acc_with_current = [page_id | acc_after_left]
      collect_all_until_boundary(right, query_end, acc_with_current)
    end
  end
end
