defmodule Bedrock.DataPlane.Log.Shale.Pulling do
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Transaction

  import Bedrock.DataPlane.Log.Shale.TransactionStreams

  @spec pull(
          t :: State.t(),
          from_version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            last_version: Bedrock.version(),
            key_range: Bedrock.key_range(),
            exclude_values: boolean(),
            recovery: boolean()
          ]
        ) ::
          {:ok, State.t(), [Transaction.t()]}
          | {:waiting_for, Bedrock.version()}
          | {:error, :not_ready}
          | {:error, :not_locked}
          | {:error, :invalid_last_version}
          | {:error, :version_too_old}
  def pull(t, from_version, opts \\ [])

  def pull(t, from_version, _) when from_version >= t.last_version,
    do: {:waiting_for, from_version}

  def pull(t, from_version, _) when from_version < t.oldest_version,
    do: {:error, :version_too_old}

  def pull(t, from_version, opts) do
    with :ok <- check_for_locked_outside_of_recovery(opts[:recovery] || false, t),
         {:ok, last_version} <- check_last_version(opts[:last_version], from_version),
         {:ok, [active_segment | remaining_segments] = all_segments} <-
           ensure_necessary_segments_are_loaded(
             last_version,
             [t.active_segment | t.segments]
           ),
         {:ok, transaction_stream} <- from_segments(all_segments, from_version) do
      transactions =
        transaction_stream
        |> until_version(last_version)
        |> at_most(determine_pull_limit(opts[:limit], t))
        |> filter_keys_in_range(opts[:key_range])
        |> exclude_values(opts[:exclude_values] || false)
        |> Enum.to_list()

      {:ok, %{t | active_segment: active_segment, segments: remaining_segments}, transactions}
    end
  end

  def ensure_necessary_segments_are_loaded(_, []), do: {:error, :version_too_old}

  def ensure_necessary_segments_are_loaded(last_version, [segment | remaining_segments])
      when segment.min_version <= last_version do
    with segment <- Segment.load_transactions(segment) do
      {:ok, [segment | remaining_segments]}
    end
  end

  def ensure_necessary_segments_are_loaded(last_version, [segment | remaining_segments]) do
    with segment <- Segment.load_transactions(segment),
         {:ok, remaining_segments} <-
           ensure_necessary_segments_are_loaded(last_version, remaining_segments) do
      {:ok, [segment | remaining_segments]}
    end
  end

  @spec check_for_locked_outside_of_recovery(boolean(), State.t()) ::
          :ok | {:error, :not_locked} | {:error, :not_ready}
  def check_for_locked_outside_of_recovery(in_recovery, t)
  def check_for_locked_outside_of_recovery(true, %{mode: :locked}), do: :ok
  def check_for_locked_outside_of_recovery(true, _), do: {:error, :not_locked}
  def check_for_locked_outside_of_recovery(false, %{mode: :locked}), do: {:error, :not_ready}
  def check_for_locked_outside_of_recovery(_, _), do: :ok

  @spec check_last_version(
          last_version :: Bedrock.version() | nil,
          from_version :: Bedrock.version()
        ) :: {:ok, Bedrock.version()} | {:error, :invalid_last_version}
  def check_last_version(nil, _), do: {:ok, nil}

  def check_last_version(last_version, from_version)
      when last_version >= from_version,
      do: {:ok, last_version}

  def check_last_version(_, _), do: {:error, :invalid_last_version}

  def determine_pull_limit(nil, t), do: t.params.default_pull_limit
  def determine_pull_limit(limit, t), do: min(limit, t.params.max_pull_limit)
end
