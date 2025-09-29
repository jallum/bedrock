defmodule Bedrock.Test.Storage.Olivine.IndexTestHelpers do
  @moduledoc """
  Test helper functions for Index operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page

  @doc """
  Rebuilds page chain consistency from the tree structure.

  This function is used in property tests to ensure that after manually building
  test indexes, the page chain pointers are consistent with the tree structure.
  """
  @spec rebuild_page_chain_consistency(Index.t()) :: Index.t()
  def rebuild_page_chain_consistency(%Index{tree: tree, page_map: page_map} = index) do
    updated_page_map = rebuild_page_chain_from_tree(tree, page_map)
    %{index | page_map: updated_page_map}
  end

  @spec rebuild_page_chain_from_tree(:gb_trees.tree(), map()) :: map()
  defp rebuild_page_chain_from_tree(tree, page_map) do
    tree_page_ids = tree |> :gb_trees.to_list() |> Enum.map(fn {_key, page_id} -> page_id end)

    case tree_page_ids do
      [] -> create_empty_page_0(page_map)
      [first_id | _] -> rebuild_chain_with_pages(tree_page_ids, page_map, first_id)
    end
  end

  defp create_empty_page_0(page_map) do
    empty_page = Page.new(0, [])
    Map.put(page_map, 0, {empty_page, 0})
  end

  defp rebuild_chain_with_pages(tree_page_ids, page_map, first_id) do
    # Update page 0's next_id in the tuple without modifying the page binary
    updated_page_map =
      case Map.get(page_map, 0) do
        nil ->
          empty_page_0 = Page.new(0, [])
          Map.put(page_map, 0, {empty_page_0, first_id})

        {existing_page_0, _old_next_id} ->
          Map.put(page_map, 0, {existing_page_0, first_id})
      end

    updated_page_map
    |> chain_pages_together(tree_page_ids)
    |> set_last_page_terminator(tree_page_ids)
  end

  defp chain_pages_together(page_map, tree_page_ids) do
    tree_page_ids
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.reduce(page_map, &update_page_next_id/2)
  end

  defp update_page_next_id([current_id, next_id], acc_map) do
    case Map.get(acc_map, current_id) do
      nil ->
        acc_map

      {current_page, _old_next_id} ->
        # Just update the next_id in the tuple without recreating the page binary
        Map.put(acc_map, current_id, {current_page, next_id})
    end
  end

  defp set_last_page_terminator(page_map, tree_page_ids) do
    last_id = List.last(tree_page_ids)

    case Map.get(page_map, last_id) do
      nil ->
        page_map

      {last_page, _old_next_id} ->
        # Just update the next_id to 0 in the tuple without recreating the page binary
        Map.put(page_map, last_id, {last_page, 0})
    end
  end
end
