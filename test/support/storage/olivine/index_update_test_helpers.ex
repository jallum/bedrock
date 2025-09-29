defmodule Bedrock.Test.Storage.Olivine.IndexUpdateTestHelpers do
  @moduledoc """
  Test helper functions for IndexUpdate operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate

  @doc """
  Returns a list of {page_id, page_binary} tuples for pages that were modified during the update.
  This function is useful for test assertions and debugging.
  """
  @spec modified_pages(IndexUpdate.t()) :: [{non_neg_integer(), binary()}]
  def modified_pages(%IndexUpdate{index: index, modified_page_ids: modified_page_ids}),
    do: Enum.map(modified_page_ids, fn page_id -> {page_id, index.page_map[page_id]} end)
end
