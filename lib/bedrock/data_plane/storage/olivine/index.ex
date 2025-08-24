defmodule Bedrock.DataPlane.Storage.Olivine.Index do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager.Page

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
end
