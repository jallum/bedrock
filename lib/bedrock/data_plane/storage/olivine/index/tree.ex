defmodule Bedrock.DataPlane.Storage.Olivine.Index.Tree do
  @moduledoc """
  Tree operations for the Olivine storage driver.

  ## Structure

  The tree is a gb_trees structure with:
  - **Key**: Page's `last_key` (rightmost key in page)
  - **Value**: `page_id` (no first_key needed)
  - **Ordering**: Sorted by `last_key` for efficient range queries
  - **Coverage**: Pages cover the entire keyspace with no gaps

  ## Key Operations

  - `page_for_key/2`: Find page containing a specific key
  - `add_page_to_tree/2`: Add page to tree structure
  - `remove_page_from_tree/2`: Remove page from tree structure

  ## No Gaps Design

  Every point in the keyspace belongs to exactly one page:
  1. Page 0 starts at `<<>>` (empty binary)
  2. Each page ends at its `last_key`
  3. Next page implicitly starts after previous page's `last_key`
  4. Final page extends to `<<0xFF, 0xFF>>` (infinity marker)
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  @type t :: :gb_trees.tree()

  @type page_id :: Page.id()
  @type page :: Page.t()

  @doc """
  Builds a page tree from a page map by adding each page to an empty tree.
  """
  @spec from_page_map(page_map :: map()) :: t()
  def from_page_map(page_map) do
    Enum.reduce(page_map, :gb_trees.empty(), fn {_page_id, {page, _next_id}}, tree_acc ->
      add_page_to_tree(tree_acc, page)
    end)
  end

  @doc """
  Finds the page that contains a specific key. With no gaps, every key
  maps to exactly one page. Uses gb_trees iterator for efficiency.

  Tree structure: key = last_key, value = page_id
  """
  @spec page_for_key(t(), Bedrock.key()) :: page_id()
  def page_for_key(tree, key) do
    case :gb_trees.iterator_from(key, tree) do
      iter ->
        case :gb_trees.next(iter) do
          {_last_key, page_id, _next_iter} -> page_id
          # Key is beyond all pages in tree, return rightmost page (always 0)
          _none -> 0
        end
    end
  end

  @doc """
  Updates the interval tree by adding a new page range.
  """
  @spec add_page_to_tree(t(), page()) :: t()
  def add_page_to_tree(tree, page) do
    case Page.right_key(page) do
      nil ->
        # Empty page - only add page 0 to tree (starts at beginning of keyspace)
        if Page.id(page) == 0 do
          :gb_trees.enter(<<>>, 0, tree)
        else
          tree
        end

      last_key ->
        # Add page with its actual last_key - no infinity marker needed
        :gb_trees.enter(last_key, Page.id(page), tree)
    end
  end

  @doc """
  Updates the interval tree by removing a page range.
  """
  @spec remove_page_from_tree(t(), page()) :: t()
  def remove_page_from_tree(tree, page) do
    case Page.right_key(page) do
      nil ->
        # Empty page - only remove page 0 if it's in tree with <<>>
        if Page.id(page) == 0 do
          :gb_trees.delete_any(<<>>, tree)
        else
          tree
        end

      last_key ->
        :gb_trees.delete_any(last_key, tree)
    end
  end

  @doc """
  Updates a page's position in the tree by removing the old range and adding the new range.
  Only updates if the last_key actually changed for efficiency.
  """
  @spec update_page_in_tree(t(), page(), page()) :: t()
  def update_page_in_tree(tree, old_page, new_page) do
    old_last_key = Page.right_key(old_page)
    new_last_key = Page.right_key(new_page)

    if old_last_key == new_last_key do
      tree
    else
      tree
      |> remove_page_from_tree(old_page)
      |> add_page_to_tree(new_page)
    end
  end
end
