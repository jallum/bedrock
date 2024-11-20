defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Writer

  @spec push(
          t :: State.t(),
          expected_version :: Bedrock.version(),
          encoded_transaction :: EncodedTransaction.t(),
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

  def push(_, _, _, _),
    do: {:error, :tx_out_of_order}

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

  @spec write_encoded_transaction(State.t(), EncodedTransaction.t()) ::
          {:ok, State.t()} | {:error, term()}
  def write_encoded_transaction(t, encoded_transaction)
      when is_nil(t.writer) do
    version = EncodedTransaction.version(encoded_transaction)

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
        version = EncodedTransaction.version(encoded_transaction)
        {:ok, %{t | writer: writer, last_version: version}}

      {:error, :segment_full} ->
        with :ok <- Writer.close(t.writer) do
          %{t | writer: nil}
          |> write_encoded_transaction(encoded_transaction)
        end
    end
  end
end
