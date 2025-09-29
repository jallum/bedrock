defmodule Bedrock.DataPlane.Storage.Olivine.DataDatabase do
  @moduledoc false

  @type locator :: <<_::64>>

  @opaque t :: %__MODULE__{
            file: :file.fd(),
            file_offset: non_neg_integer(),
            file_name: [char()],
            window_size_in_microseconds: pos_integer(),
            buffer: :ets.tab()
          }

  defstruct [
    :file,
    :file_offset,
    :file_name,
    :window_size_in_microseconds,
    :buffer
  ]

  @spec open(file_path :: String.t(), opts :: keyword()) ::
          {:ok, t()} | {:error, File.posix()}
  def open(file_path, opts \\ []) do
    window_in_ms = Keyword.get(opts, :window_in_ms, 5_000)

    file_name = String.to_charlist(file_path <> ".data")

    with {:ok, file} <- :file.open(file_name, [:raw, :binary, :read, :append]),
         {:ok, offset} <- :file.position(file, {:eof, 0}) do
      buffer = :ets.new(:buffer, [:ordered_set, :protected, {:read_concurrency, true}])

      {:ok,
       %__MODULE__{
         file: file,
         file_offset: offset,
         file_name: file_name,
         window_size_in_microseconds: window_in_ms * 1_000,
         buffer: buffer
       }}
    end
  end

  @spec close(t()) :: :ok
  def close(db) do
    try do
      :ets.delete(db.buffer)
    catch
      _, _ -> :ok
    end

    try do
      :file.close(db.file)
    catch
      _, _ -> :ok
    end

    :ok
  end

  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) ::
          {:ok, locator(), t()}
  def store_value(db, _key, _version, value) do
    offset = db.file_offset
    size = byte_size(value)
    locator = <<offset::47, size::17>>
    :ets.insert(db.buffer, {locator, value})
    {:ok, locator, %{db | file_offset: offset + size}}
  end

  @spec load_value(t(), locator()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(db, locator) do
    case locator do
      <<_offset::47, 0::17>> ->
        {:ok, <<>>}

      <<offset::47, size::17>> = locator ->
        case :ets.lookup(db.buffer, locator) do
          [{^locator, value}] -> {:ok, value}
          [] -> load_from_file(db.file_name, offset, size)
        end
    end
  end

  @spec value_loader(t()) :: (locator() -> {:ok, Bedrock.value()} | {:error, :not_found} | {:error, :shutting_down})
  def value_loader(db) do
    file_name = db.file_name
    buffer = db.buffer

    fn
      <<_offset::47, 0::17>> ->
        {:ok, <<>>}

      <<offset::47, size::17>> = locator ->
        case :ets.lookup(buffer, locator) do
          [{^locator, value}] -> {:ok, value}
          [] -> load_from_file(file_name, offset, size)
        end
    end
  end

  @spec many_value_loader(t()) ::
          ([locator()] ->
             {:ok, %{locator() => Bedrock.value()}}
             | {:error, :not_found}
             | {:error, :shutting_down})
  def many_value_loader(db) do
    file_name = db.file_name
    buffer = db.buffer

    fn
      locators when is_list(locators) ->
        load_many_values(locators, buffer, file_name)
    end
  end

  @spec flush(t(), size_in_bytes :: pos_integer()) :: t()
  def flush(db, size_in_bytes) do
    mark = <<size_in_bytes::47, 0::17>>

    write_iolist =
      db.buffer
      |> :ets.select([{{:"$1", :"$2"}, [{:"=<", :"$1", mark}], [{{:"$1", :"$2"}}]}])
      |> Enum.reduce([], fn
        {locator, value}, iolist when is_binary(locator) and is_binary(value) ->
          [value | iolist]

        _, iolist ->
          iolist
      end)
      |> Enum.reverse()

    tx_size_bytes = :erlang.iolist_size(write_iolist)
    :file.pwrite(db.file, size_in_bytes - tx_size_bytes, write_iolist)

    # Clean up buffer entries that have been flushed
    :ets.select_delete(db.buffer, [{{:"$1", :_}, [{:<, :"$1", mark}], [true]}])
    db
  end

  # Private helper functions

  defp load_from_file(file_name, offset, size) do
    file_name
    |> :file.open([:raw, :binary, :read])
    |> case do
      {:ok, file} ->
        try do
          :file.pread(file, offset, size)
        after
          :file.close(file)
        end

      error ->
        error
    end
  end

  defp load_many_values(locators, buffer, file_name) do
    {result, not_in_buffer} = partition_locators_by_availability(locators, buffer)
    merge_with_disk_values(result, not_in_buffer, file_name)
  end

  defp partition_locators_by_availability(locators, buffer) do
    Enum.reduce(locators, {%{}, []}, fn
      <<_::47, 0::17>> = locator, {result, not_in_buffer} ->
        {Map.put(result, locator, <<>>), not_in_buffer}

      locator, {result, not_in_buffer} ->
        case :ets.lookup(buffer, locator) do
          [{^locator, value}] -> {Map.put(result, locator, value), not_in_buffer}
          [] -> {result, [locator | not_in_buffer]}
        end
    end)
  end

  defp merge_with_disk_values(result, [], _file_name), do: {:ok, result}

  defp merge_with_disk_values(result, not_in_buffer, file_name) do
    {:ok, values} = load_many_from_file(file_name, not_in_buffer)
    {:ok, Map.merge(result, not_in_buffer |> Enum.zip(values) |> Map.new())}
  end

  defp load_many_from_file(file_name, locators) do
    file_name
    |> :file.open([:raw, :binary, :read])
    |> case do
      {:ok, file} ->
        try do
          :file.pread(file, Enum.map(locators, fn <<offset::47, size::17>> -> {offset, size} end))
        after
          :file.close(file)
        end

      error ->
        error
    end
  end
end
