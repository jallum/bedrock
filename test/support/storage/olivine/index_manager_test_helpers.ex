defmodule Bedrock.Test.Storage.Olivine.IndexManagerTestHelpers do
  @moduledoc """
  Test helper functions for IndexManager operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Version

  @doc """
  Returns the last committed version for the index manager.
  This function is useful for test assertions.
  """
  @spec last_committed_version(IndexManager.t()) :: Bedrock.version()
  def last_committed_version(index_manager), do: index_manager.current_version

  @doc """
  Checks if a version is within the window (not evicted).
  This function is useful for test assertions about version visibility.
  """
  @spec version_in_window?(Bedrock.version(), Bedrock.version()) :: boolean()
  def version_in_window?(version, window_start_version) do
    Version.compare(version, window_start_version) != :lt
  end

  @doc """
  Splits versions list at the window boundary.
  Returns {versions_in_window, evicted_versions}.
  This function is useful for testing window advancement logic.
  """
  @spec split_versions_at_window([{Bedrock.version(), term()}], Bedrock.version()) ::
          {[{Bedrock.version(), term()}], [{Bedrock.version(), term()}]}
  def split_versions_at_window(versions, window_start_version) do
    Enum.split_with(versions, fn {version, _index} ->
      version_in_window?(version, window_start_version)
    end)
  end

  @doc """
  Finds the best index to use for a fetch at the given version.
  Returns the index with the highest version that is <= target_version.
  This function is useful for testing version resolution logic.
  """
  @spec find_best_index_for_fetch([{Bedrock.version(), term()}], Bedrock.version()) ::
          {Bedrock.version(), term()} | nil
  def find_best_index_for_fetch([], _target_version), do: nil

  def find_best_index_for_fetch(versions, target_version) do
    versions
    |> Enum.filter(fn {version, _index} -> Version.compare(version, target_version) != :gt end)
    |> Enum.max_by(fn {version, _index} -> version end, fn -> nil end)
  end
end
