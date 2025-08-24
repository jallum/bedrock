defmodule Bedrock.DataPlane.Storage.Olivine.Index do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.Index.Tree
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager

  @type operation :: IndexManager.operation()

  @type t :: %__MODULE__{
          tree: :gb_trees.tree(),
          page_map: map(),
          deleted_page_ids: [Page.id()],
          modified_page_ids: [Page.id()],
          pending_operations: %{Page.id() => %{Bedrock.key() => operation()}}
        }

  defstruct [
    :tree,
    :page_map,
    :deleted_page_ids,
    :modified_page_ids,
    :pending_operations
  ]

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
          page_map: initial_page_map,
          deleted_page_ids: [],
          modified_page_ids: [],
          pending_operations: %{}
        }

        {:ok, index, max_page_id, free_page_ids}

      {:error, :no_chain} ->
        # Empty database - return empty index with page 0
        initial_page_map = %{0 => Page.new(0, [])}

        index = %__MODULE__{
          tree: :gb_trees.empty(),
          page_map: initial_page_map,
          deleted_page_ids: [],
          modified_page_ids: [],
          pending_operations: %{}
        }

        {:ok, index, 0, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_page_chain(_database, page_id, page_map) when is_map_key(page_map, page_id), do: {:error, :cycle_detected}

  defp load_page_chain(database, page_id, page_map) do
    case Database.load_page(database, page_id) do
      {:ok, page_binary} ->
        # Basic validation of binary page format
        case page_binary do
          <<_id::32, _next_id::32, _key_count::16, _last_key_offset::32, _reserved::16, _entries::binary>> ->
            page_map
            |> Map.put(page_id, page_binary)
            |> continue_page_chain(database, page_binary)

          _ ->
            {:error, :corrupted_page}
        end

      {:error, :not_found} when page_id == 0 ->
        {:error, :no_chain}

      {:error, :not_found} ->
        {:error, :broken_chain}
    end
  end

  defp continue_page_chain(page_map, database, page_binary) do
    case Page.next_id(page_binary) do
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
  Returns {:ok, page_binary} if found, {:error, :not_found} if not found.
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
  Returns {:ok, [page_binary]} with the list of pages (may be empty).
  """
  @spec pages_for_range(t(), Bedrock.key(), Bedrock.key()) :: {:ok, [Page.t()]}
  def pages_for_range(%__MODULE__{tree: tree, page_map: page_map}, start_key, end_key) do
    pages =
      tree
      |> Tree.page_ids_in_range(start_key, end_key)
      |> Enum.map(&Map.fetch!(page_map, &1))

    {:ok, pages}
  end
end
