defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  @moduledoc false
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version

  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]

  @spec recover_from(
          State.t(),
          Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | {:error, :lock_required}
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
  def recover_from(t, _, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, source_log, first_version, last_version) do
    %{t | mode: :recovering}
    |> abort_all_waiting_pullers()
    |> close_writer()
    |> discard_all_segments()
    |> ensure_active_segment(first_version)
    |> open_writer()
    |> push_sentinel(first_version)
    |> pull_transactions(source_log, first_version, last_version)
    |> case do
      {:ok, t} ->
        {:ok, %{t | mode: :running, oldest_version: first_version, last_version: last_version}}

      error ->
        error
    end
  end

  @spec pull_transactions(
          t :: State.t(),
          log_ref :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
  def pull_transactions(t, _, first_version, last_version) when first_version == last_version,
    do: {:ok, t}

  def pull_transactions(t, log_ref, first_version, last_version) do
    case Log.pull(log_ref, first_version, recovery: true, last_version: last_version) do
      {:ok, []} ->
        {:ok, t}

      {:ok, transactions} ->
        transactions
        |> Enum.reduce_while({first_version, t}, fn bytes, acc ->
          process_transaction_bytes(bytes, acc)
        end)
        |> case do
          {:error, _reason} = error -> error
          {next_first, t} -> pull_transactions(t, log_ref, next_first, last_version)
        end

      {:error, :unavailable} ->
        {:error, {:source_log_unavailable, log_ref}}
    end
  end

  @spec process_transaction_bytes(BedrockTransaction.encoded(), {Bedrock.version(), State.t()}) ::
          {:cont, {Bedrock.version(), State.t()}} | {:halt, {:error, term()}}
  defp process_transaction_bytes(bytes, {last_version, t}) do
    case BedrockTransaction.extract_transaction_id(bytes) do
      {:ok, version} when is_binary(version) ->
        handle_valid_transaction_bytes(bytes, version, last_version, t)

      {:ok, nil} ->
        {:halt, {:error, :missing_transaction_id}}

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp process_transaction_bytes(_, _) do
    {:halt, {:error, :invalid_transaction}}
  end

  @spec handle_valid_transaction_bytes(
          BedrockTransaction.encoded(),
          Bedrock.version(),
          Bedrock.version(),
          State.t()
        ) ::
          {:cont, {Bedrock.version(), State.t()}} | {:halt, {:error, term()}}
  defp handle_valid_transaction_bytes(bytes, version, last_version, t) do
    # Validate that the transaction can be decoded (but pass the original bytes to push)
    with {:ok, _transaction} <- BedrockTransaction.decode(bytes),
         {:ok, t} <- push(t, last_version, bytes, fn _ -> :ok end) do
      {:cont, {version, t}}
    else
      {:wait, _t} -> {:halt, {:error, :tx_out_of_order}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  @spec abort_all_waiting_pullers(State.t()) :: State.t()
  def abort_all_waiting_pullers(%{waiting_pullers: waiting_pullers} = t) do
    waiting_pullers
    |> Enum.reduce(%{t | waiting_pullers: %{}}, fn {_version, puller_list}, t ->
      Enum.each(puller_list, fn {_timestamp, reply_to_fn, _opts} ->
        reply_to_fn.({:ok, []})
      end)

      t
    end)
  end

  @spec close_writer(State.t()) :: State.t()
  def close_writer(%{writer: nil} = t), do: t

  @spec close_writer(State.t()) :: State.t()
  def close_writer(%{writer: writer} = t) do
    :ok = Writer.close(writer)
    %{t | writer: nil}
  end

  @spec discard_all_segments(State.t()) :: State.t()
  def discard_all_segments(%{active_segment: nil, segments: segments} = t),
    do: %{t | segments: discard_segments(t.segment_recycler, segments)}

  @spec discard_all_segments(State.t()) :: State.t()
  def discard_all_segments(%{active_segment: active_segment, segments: segments} = t) do
    %{
      t
      | active_segment: nil,
        segments: discard_segments(t.segment_recycler, [active_segment | segments])
    }
  end

  @spec discard_segments(term(), [Segment.t()]) :: []
  def discard_segments(_segment_recycler, []), do: []

  @spec discard_segments(term(), [Segment.t()]) :: []
  def discard_segments(segment_recycler, [segment | remaining_segments]) do
    :ok = SegmentRecycler.check_in(segment_recycler, segment.path)
    discard_segments(segment_recycler, remaining_segments)
  end

  @spec ensure_active_segment(State.t(), Bedrock.version()) :: State.t()
  def ensure_active_segment(%{active_segment: nil} = t, version) do
    case Segment.allocate_from_recycler(t.segment_recycler, t.path, version) do
      {:ok, new_segment} -> %{t | active_segment: new_segment, last_version: version}
      {:error, :allocation_failed} -> raise "Failed to allocate new segment"
    end
  end

  @spec ensure_active_segment(State.t()) :: State.t()
  def ensure_active_segment(t), do: t

  @spec open_writer(State.t()) :: State.t()
  def open_writer(t) do
    case Writer.open(t.active_segment.path) do
      {:ok, new_writer} ->
        %{t | writer: new_writer}

      {:error, _} ->
        raise "Failed to open writer"
    end
  end

  @spec push_sentinel(State.t(), Bedrock.version()) :: State.t()
  def push_sentinel(t, version) do
    # Create empty BedrockTransaction sentinel
    sentinel_transaction = %{
      mutations: []
    }

    encoded_sentinel = BedrockTransaction.encode(sentinel_transaction)
    # Ensure version is in binary format
    version_binary = if is_binary(version), do: version, else: Version.from_integer(version)
    {:ok, sentinel} = BedrockTransaction.add_transaction_id(encoded_sentinel, version_binary)

    with sentinel <- sentinel,
         {:ok, t} <- push(t, version, sentinel, fn _ -> :ok end) do
      t
    else
      {:error, _} -> raise "Failed to push sentinel"
    end
  end
end
