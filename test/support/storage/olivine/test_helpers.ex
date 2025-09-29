defmodule Bedrock.DataPlane.Storage.Olivine.TestHelpers do
  @moduledoc """
  Test helpers for Olivine storage testing.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Version

  @doc """
  Load version range metadata only (without loading all pages into memory).
  Returns version ranges with empty page maps for testing purposes.
  """
  @spec load_version_range_metadata(IndexDatabase.t()) :: [{Bedrock.version(), Bedrock.version()}]
  def load_version_range_metadata(index_db) do
    # Get all records except :durable_version, but only extract version metadata
    all_records =
      :dets.select(index_db.dets_storage, [{{:"$1", :"$2"}, [{:"/=", :"$1", :durable_version}], [{{:"$1", :"$2"}}]}])

    # Extract only version range metadata: {version, last_version}
    all_records
    |> Enum.map(fn {version, {last_version, _pages_map}} ->
      {version, last_version}
    end)
    |> Enum.sort_by(fn {version, _last_version} -> version end, &(Version.compare(&1, &2) != :lt))
  end
end
