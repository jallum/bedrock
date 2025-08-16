defmodule Bedrock.DataPlane.Log.Shale.Writer do
  @moduledoc """
  A struct that represents a writer for a segment.
  """

  alias Bedrock.DataPlane.BedrockTransaction

  defstruct [:fd, :write_offset, :bytes_remaining]

  @wal_eof_version 0xFFFFFFFFFFFFFFFF
  @eof_marker <<@wal_eof_version::unsigned-big-64, 0::unsigned-big-32, 0::unsigned-big-32>>
  @empty_segment_header <<"BED0">> <> @eof_marker

  @typedoc """
  A `Writer` is a handle to a segment that can be used to write transcations
  to the segment. It is a stateful object that keeps track of the current
  write offset and the number of bytes remaining in the segment.
  """
  @type t :: %__MODULE__{
          fd: File.file_descriptor(),
          write_offset: pos_integer(),
          bytes_remaining: pos_integer()
        }

  @spec open(path_to_file :: String.t()) :: {:ok, t()} | {:error, File.posix()}
  def open(path_to_file) do
    with {:ok, stat} <- File.stat(path_to_file),
         {:ok, fd} <- File.open(path_to_file, [:write, :read, :raw, :binary]),
         :ok <- :file.pwrite(fd, 0, @empty_segment_header) do
      {:ok,
       %__MODULE__{
         fd: fd,
         write_offset: 4,
         bytes_remaining: stat.size - 4 - 16
       }}
    end
  end

  @spec close(writer :: t() | nil) :: :ok | {:error, File.posix()}
  def close(nil), do: :ok
  def close(%__MODULE__{} = writer), do: :file.close(writer.fd)

  @spec append(t(), BedrockTransaction.encoded()) ::
          {:ok, t()} | {:error, :segment_full} | {:error, File.posix()}
  def append(%__MODULE__{} = writer, transaction)
      when writer.bytes_remaining < 16 + byte_size(transaction),
      do: {:error, :segment_full}

  def append(%__MODULE__{} = writer, transaction) do
    :file.pwrite(writer.fd, writer.write_offset, [transaction, @eof_marker])
    |> case do
      :ok ->
        size_of_transaction = byte_size(transaction)
        new_write_offset = writer.write_offset + size_of_transaction
        new_bytes_remaining = writer.bytes_remaining - size_of_transaction
        {:ok, %{writer | write_offset: new_write_offset, bytes_remaining: new_bytes_remaining}}

      {:error, _reason} = error ->
        error
    end
  end
end
