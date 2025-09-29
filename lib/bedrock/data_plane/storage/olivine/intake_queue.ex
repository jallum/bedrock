defmodule Bedrock.DataPlane.Storage.Olivine.IntakeQueue do
  @moduledoc """
  Transaction intake queue for batching and processing encoded transactions.

  Manages a queue of {encoded_transaction, version, size} tuples with operations
  for adding transactions and taking batches by size limits.
  """

  alias Bedrock.DataPlane.Transaction

  @type t :: %__MODULE__{
          queue: :queue.queue({binary(), Bedrock.version(), pos_integer()})
        }

  defstruct queue: :queue.new()

  @doc """
  Creates a new empty intake queue.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Adds encoded transactions to the queue.
  Each transaction is stored with its version and byte size.
  """
  @spec add_transactions(t(), [binary()]) :: t()
  def add_transactions(%__MODULE__{} = intake_queue, encoded_transactions) do
    new_queue =
      Enum.reduce(encoded_transactions, intake_queue.queue, fn encoded_tx, queue ->
        version = Transaction.commit_version!(encoded_tx)
        size = byte_size(encoded_tx)
        :queue.in({encoded_tx, version, size}, queue)
      end)

    %{intake_queue | queue: new_queue}
  end

  @doc """
  Takes a batch of transactions up to the specified size limit.
  Always takes at least one transaction, even if it exceeds the size limit.

  Returns {batch, last_version, updated_intake_queue}.
  """
  @spec take_batch_by_size(t(), pos_integer()) :: {[binary()], Bedrock.version() | nil, t()}
  def take_batch_by_size(%__MODULE__{} = intake_queue, max_size_bytes) do
    {batch, last_version, new_queue} = take_batch_by_size_impl(intake_queue.queue, max_size_bytes, [], 0, nil)
    {batch, last_version, %{intake_queue | queue: new_queue}}
  end

  @doc """
  Takes a batch of transactions up to the specified count limit.

  Returns {batch, last_version, updated_intake_queue}.
  """
  @spec take_batch_by_count(t(), pos_integer()) :: {[binary()], Bedrock.version() | nil, t()}
  def take_batch_by_count(%__MODULE__{} = intake_queue, max_count) do
    {batch, last_version, new_queue} = take_batch_by_count_impl(intake_queue.queue, max_count, [], nil)
    {batch, last_version, %{intake_queue | queue: new_queue}}
  end

  @doc """
  Returns true if the queue is empty.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = intake_queue), do: :queue.is_empty(intake_queue.queue)

  @doc """
  Returns the number of transactions in the queue.
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = intake_queue), do: :queue.len(intake_queue.queue)

  # Private implementation

  # Always take at least one transaction, even if it exceeds the size limit
  defp take_batch_by_size_impl(queue, max_size, [_ | _] = acc, current_size, last_version)
       when current_size >= max_size do
    {Enum.reverse(acc), last_version, queue}
  end

  defp take_batch_by_size_impl(queue, max_size, acc, current_size, last_version) do
    case :queue.out(queue) do
      {{:value, {transaction, version, size}}, new_queue} ->
        new_size = current_size + size
        take_batch_by_size_impl(new_queue, max_size, [transaction | acc], new_size, version)

      {:empty, queue} ->
        {Enum.reverse(acc), last_version, queue}
    end
  end

  # Count-based implementation
  defp take_batch_by_count_impl(queue, 0, acc, last_version) do
    {Enum.reverse(acc), last_version, queue}
  end

  defp take_batch_by_count_impl(queue, remaining_count, acc, last_version) do
    case :queue.out(queue) do
      {{:value, {transaction, version, _size}}, new_queue} ->
        take_batch_by_count_impl(new_queue, remaining_count - 1, [transaction | acc], version)

      {:empty, queue} ->
        {Enum.reverse(acc), last_version, queue}
    end
  end
end
