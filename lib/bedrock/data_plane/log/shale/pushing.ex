defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  @moduledoc false
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer

  import Bedrock.DataPlane.Log.Telemetry

  @spec push(
          t :: State.t(),
          expected_version :: Bedrock.version(),
          encoded_transaction :: BedrockTransaction.encoded(),
          ack_fn :: (:ok | {:error, term()} -> :ok)
        ) :: {:ok | :wait, State.t()} | {:error, :tx_out_of_order} | {:error, :tx_too_large}
  def push(_, _, encoded_transaction, _ack_fn)
      when byte_size(encoded_transaction) > 10_000_000 do
    {:error, :tx_too_large}
  end

  def push(t, expected_version, encoded_transaction, ack_fn)
      when expected_version == t.last_version do
    case write_encoded_transaction(t, encoded_transaction) do
      {:ok, t} ->
        trace_push_transaction(encoded_transaction)
        :ok = ack_fn.(:ok)
        t |> do_pending_pushes()
    end
  end

  def push(t, expected_version, encoded_transaction, ack_fn)
      when expected_version > t.last_version do
    {:wait,
     t
     |> Map.update!(
       :pending_pushes,
       &Map.put(&1, expected_version, {encoded_transaction, ack_fn})
     )}
  end

  def push(t, expected_version, _, _) do
    trace_push_out_of_order(expected_version, t.last_version)
    {:error, :tx_out_of_order}
  end

  @spec do_pending_pushes(State.t()) ::
          {:ok | :wait, State.t()} | {:error, :tx_out_of_order} | {:error, :tx_too_large}
  def do_pending_pushes(t) do
    case Map.pop(t.pending_pushes, t.last_version) do
      {nil, _} ->
        {:ok, t}

      {{encoded_transaction, ack_fn}, pending_pushes} ->
        :ok = ack_fn.(:ok)

        %{t | pending_pushes: pending_pushes}
        |> push(t.last_version, encoded_transaction, ack_fn)
    end
  end

  @spec write_encoded_transaction(State.t(), BedrockTransaction.encoded()) ::
          {:ok, State.t()} | {:error, term()}
  def write_encoded_transaction(t, encoded_transaction)
      when is_nil(t.writer) do
    # Extract version from BedrockTransaction transaction_id section
    version =
      case BedrockTransaction.extract_transaction_id(encoded_transaction) do
        {:ok, version} ->
          version

        {:error, reason} ->
          raise "Failed to extract version: #{inspect(reason)}"
      end

    with {:ok, new_segment} <-
           Segment.allocate_from_recycler(
             t.segment_recycler,
             t.path,
             version
           ),
         {:ok, new_writer} <- Writer.open(new_segment.path) do
      %State{
        t
        | writer: new_writer,
          active_segment: new_segment,
          segments: [t.active_segment | t.segments]
      }
      |> write_encoded_transaction(encoded_transaction)
    end
  end

  def write_encoded_transaction(t, encoded_transaction) do
    case Writer.append(t.writer, encoded_transaction) do
      {:ok, writer} ->
        # Extract version from BedrockTransaction transaction_id section
        case BedrockTransaction.extract_transaction_id(encoded_transaction) do
          {:ok, version} ->
            {:ok, %{t | writer: writer, last_version: version}}

          {:error, reason} ->
            {:error, {:version_extraction_failed, reason}}
        end

      {:error, :segment_full} ->
        with :ok <- Writer.close(t.writer) do
          %{t | writer: nil}
          |> write_encoded_transaction(encoded_transaction)
        end
    end
  end
end
