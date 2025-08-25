defmodule Bedrock.DataPlane.Storage.Olivine.Index do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager

  @type operation :: IndexManager.operation()

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(),
          page_map: map()
        }

  defstruct [
    :tree,
    :page_map
  ]

  @doc """
  Creates a new empty Index with an initial page.
  """
  @spec new() :: t()
  def new do
    initial_page = Page.new(0, [])
    initial_tree = :gb_trees.empty()
    initial_page_map = %{0 => initial_page}

    %__MODULE__{
      tree: initial_tree,
      page_map: initial_page_map
    }
  end

  @doc """
  Loads an Index from the database by traversing the page chain and building the tree structure.
  Returns {:ok, index, max_page_id, free_page_ids} or an error.
  """
  @spec load_from(Database.t()) ::
          {:ok, t(), Page.id(), [Page.id()]} | {:error, :corrupted_page | :broken_chain | :cycle_detected | :no_chain}
  def load_from(database) do
    case load_page_chain(database, 0, %{}) do
      {:ok, page_map} ->
        tree = Tree.from_page_map(page_map)
        page_ids = page_map |> Map.keys() |> MapSet.new()
        max_page_id = max(0, Enum.max(page_ids))
        free_page_ids = calculate_free_page_ids(max_page_id, page_ids)

        # Create initial page_map - if empty database, include page 0 like new() does
        initial_page_map =
          if :gb_trees.is_empty(tree) and max_page_id == 0 do
            %{0 => Page.new(0, [])}
          else
            page_map
          end

        index = %__MODULE__{
          tree: tree,
          page_map: initial_page_map
        }

        {:ok, index, max_page_id, free_page_ids}

      {:error, :no_chain} ->
        {:ok, new(), 0, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_page_chain(_database, page_id, page_map) when is_map_key(page_map, page_id), do: {:error, :cycle_detected}

  defp load_page_chain(database, page_id, page_map) do
    with {:ok, page} <- Database.load_page(database, page_id),
         :ok <- Page.validate(page) do
      page_map
      |> Map.put(page_id, page)
      |> load_next_page_in_chain(database, page)
    else
      {:error, :not_found} when page_id == 0 ->
        {:error, :no_chain}

      _ ->
        {:error, :broken_chain}
    end
  end

  defp load_next_page_in_chain(page_map, database, page) do
    case Page.next_id(page) do
      0 -> {:ok, page_map}
      next_id -> load_page_chain(database, next_id, page_map)
    end
  end

  defp calculate_free_page_ids(0, _all_existing_page_ids), do: []

  defp calculate_free_page_ids(max_page_id, all_existing_page_ids) do
    0..max_page_id
    |> MapSet.new()
    |> MapSet.difference(all_existing_page_ids)
    |> Enum.sort()
  end

  @doc """
  Finds the page containing the given key in this index.
  Returns {:ok, Page.t()} if found, {:error, :not_found} if not found.
  """
  @spec page_for_key(t(), Bedrock.key()) :: {:ok, Page.t()} | {:error, :not_found}
  def page_for_key(%__MODULE__{tree: tree, page_map: page_map}, key) do
    tree
    |> Tree.page_for_key(key)
    |> case do
      nil -> {:error, :not_found}
      page_id -> {:ok, Map.fetch!(page_map, page_id)}
    end
  end

  @doc """
  Finds all pages that contain keys within the given range in this index.
  Returns {:ok, [Page.t()]} with the list of pages (may be empty).
  """
  @spec pages_for_range(t(), Bedrock.key(), Bedrock.key()) :: {:ok, [Page.t()]}
  def pages_for_range(%__MODULE__{tree: tree, page_map: page_map}, start_key, end_key) do
    {:ok,
     tree
     |> Tree.page_ids_in_range(start_key, end_key)
     |> Enum.map(&Map.fetch!(page_map, &1))}
  end

  @doc """
  Removes multiple pages from the index by their IDs.
  Updates both the tree structure and page_map.
  Returns the updated index.
  """
  @spec delete_pages(t(), [Page.id()]) :: t()
  def delete_pages(index, []), do: index

  def delete_pages(%__MODULE__{tree: tree, page_map: page_map} = index, page_ids) do
    # Remove pages from tree structure
    updated_tree =
      Enum.reduce(page_ids, tree, fn page_id, tree_acc ->
        case Map.fetch(page_map, page_id) do
          {:ok, page} -> Tree.remove_page_from_tree(tree_acc, page)
          :error -> tree_acc
        end
      end)

    # Remove pages from page_map
    updated_page_map = Map.drop(page_map, page_ids)

    %{index | tree: updated_tree, page_map: updated_page_map}
  end
end
