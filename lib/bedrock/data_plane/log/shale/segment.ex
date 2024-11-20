defmodule Bedrock.DataPlane.Log.Shale.Segment do
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams

  @type t :: %__MODULE__{
          path: String.t(),
          min_version: Bedrock.version(),
          transactions: [EncodedTransaction.t()]
        }
  defstruct path: nil,
            min_version: nil,
            transactions: nil

  @wal_prefix "wal_"
  def file_prefix, do: @wal_prefix

  def encode_file_name(n) do
    log_number = n |> Integer.to_string(32) |> String.downcase() |> String.pad_leading(13, "0")
    @wal_prefix <> log_number
  end

  def decode_file_name(@wal_prefix <> log_number),
    do: log_number |> String.to_integer(32)

  def allocate_from_recycler(segment_recycler, path, version) do
    with path_to_file <- Path.join(path, encode_file_name(version)),
         :ok <- SegmentRecycler.check_out(segment_recycler, path_to_file) do
      {:ok,
       %__MODULE__{
         min_version: version,
         path: path_to_file
       }}
    end
  end

  def return_to_recycler(segment, segment_recycler),
    do: SegmentRecycler.check_in(segment_recycler, segment.path)

  @doc """
  Create a new segment from the given file path. We stat the file to get the
  size, ensuring that it exists.
  """
  @spec from_path(path_to_file :: String.t()) :: {:ok, t()} | {:error, :does_not_exist}
  def from_path(path_to_file) do
    with true <- File.exists?(path_to_file) || {:error, :does_not_exist} do
      {:ok,
       %__MODULE__{
         path: path_to_file,
         min_version: path_to_file |> Path.basename() |> decode_file_name()
       }}
    end
  end

  @spec load_transactions(t()) :: t()
  def load_transactions(%{transactions: nil} = segment) do
    TransactionStreams.from_file!(segment.path)
    |> Enum.reverse()
    |> case do
      [{:corrupted, offset} | transactions] ->
        "Corrupted segment at offset #{offset}"
        %{segment | transactions: transactions}

      transactions ->
        %{segment | transactions: transactions}
    end
  end

  def load_transactions(segment), do: segment

  def last_version(%{transactions: [<<version::unsigned-big-64, _::binary>> | _]}), do: version
  def last_version(%{min_version: min_version}), do: min_version
end
