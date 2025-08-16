defmodule Bedrock do
  @moduledoc """
  Core types and utilities for Bedrock, a distributed key-value store.

  This module defines the fundamental types used throughout the Bedrock system,
  including keys, values, versions, and time-related constructs for MVCC
  transaction processing.
  """

  alias Bedrock.Internal.Time.Interval

  @type key :: binary()
  @type key_range :: {min_inclusive :: key(), max_exclusive :: key() | :end}
  @type value :: binary()
  @type key_value :: {key(), value()}

  @type version :: Bedrock.DataPlane.Version.t()
  @type version_vector :: {oldest :: version(), newest :: version()}

  @type transaction :: %{
          optional(:mutations) => [mutation()],
          optional(:write_conflicts) => [key_range()],
          optional(:read_conflicts) => {version(), [key_range()]},
          optional(:commit_version) => version()
        }

  @type mutation ::
          {:set, key(), value()}
          | {:clear_range, key(), key()}

  @type epoch :: non_neg_integer()
  @type quorum :: pos_integer()
  @type timeout_in_ms :: :infinity | non_neg_integer()
  @type timestamp_in_ms :: integer()

  @type interval_in_ms :: :infinity | non_neg_integer()
  @type interval_in_us :: :infinity | non_neg_integer()

  @type time_unit :: Interval.unit()
  @type interval :: {Bedrock.time_unit(), non_neg_integer()}

  @type range_tag :: non_neg_integer()

  @type service :: :coordination | :log | :storage
  @type service_id :: String.t()
  @type lock_token :: binary()

  @doc """
  Creates a key range from a minimum inclusive key to a maximum exclusive key.

  ## Parameters

    - `min_key`: The minimum key value (inclusive).
    - `max_key_exclusive`: The maximum key value (exclusive).

  ## Returns

    - A tuple representing the key range.

  ## Examples

      iex> Bedrock.key_range("a", "z")
      {"a", "z"}

  """
  @spec key_range(Bedrock.key(), Bedrock.key() | :end) :: Bedrock.key_range()
  def key_range(min_key, max_key_exclusive)
      when min_key < max_key_exclusive or max_key_exclusive == :end,
      do: {min_key, max_key_exclusive}
end
