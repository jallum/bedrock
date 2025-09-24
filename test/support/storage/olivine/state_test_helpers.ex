defmodule Bedrock.Test.Storage.Olivine.StateTestHelpers do
  @moduledoc """
  Test helper functions for State operations that are only used in tests.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.State

  @doc """
  Returns true if the buffer tracking queue is empty.
  This function is useful for test assertions.
  """
  @spec buffer_tracking_queue_empty?(State.t()) :: boolean()
  def buffer_tracking_queue_empty?(t), do: Database.buffer_tracking_queue_empty?(t.database)

  @doc """
  Returns the size of the buffer tracking queue.
  This function is useful for test assertions and monitoring.
  """
  @spec buffer_tracking_queue_size(State.t()) :: non_neg_integer()
  def buffer_tracking_queue_size(t), do: Database.buffer_tracking_queue_size(t.database)
end
