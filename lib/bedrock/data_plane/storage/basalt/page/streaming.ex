defmodule Bedrock.DataPlane.Storage.Basalt.Page.Streaming do
  import Bedrock.DataPlane.Storage.Basalt.Page.Scanning, only: [decode_key_and_value_offset: 2]

  @spec stream_from_key(page :: binary(), start_key :: binary()) :: Enumerable.t()
  def stream_from_key(page, start_key),
    do: stream_from_key(page, start_key, 0)

  defp stream_from_key(
         <<block_size::big-16, block::binary-size(block_size), rem_blocks::binary>>,
         key,
         offset
       ) do
    case decode_key_and_value_offset(block, <<>>) do
      {this_key, rel_value_offset} ->
        cond do
          key == this_key ->
            value_offset = offset + 2 + rel_value_offset

            Stream.concat(
              stream_lazily(fn ->
                [
                  {this_key, value_offset}
                  | rest_of_block(
                      binary_part(block, rel_value_offset, block_size - rel_value_offset),
                      value_offset,
                      this_key
                    )
                ]
                |> Enum.reverse()
              end),
              stream_from_rest_of_blocks(rem_blocks, offset + 2 + block_size)
            )

          key < this_key || rem_blocks == <<>> ->
            value_offset = offset + 2 + rel_value_offset

            Stream.concat(
              stream_lazily(fn ->
                find_in_rest_of_block(
                  binary_part(block, rel_value_offset, block_size - rel_value_offset),
                  value_offset,
                  this_key,
                  key
                )
                |> Enum.reverse()
              end),
              stream_from_rest_of_blocks(rem_blocks, offset + 2 + block_size)
            )

          true ->
            stream_from_key(rem_blocks, key, offset + 2 + block_size)
        end

      error ->
        error
    end
  end

  defp stream_from_key(_, _, _), do: :corrupted

  defp stream_lazily(fun) do
    Stream.resource(
      fun,
      fn
        :end -> {:halt, :end}
        list when is_list(list) -> {list, :end}
      end,
      fn _ -> :ok end
    )
  end

  defp stream_from_rest_of_blocks(blocks, offset) do
    Stream.resource(
      fn -> {blocks, offset} end,
      fn
        {<<block_size::big-16, block::binary-size(block_size), rem_blocks::binary>>, offset} ->
          case decode_key_and_value_offset(block, <<>>) do
            {this_key, rel_value_offset} ->
              value_offset = offset + 2 + rel_value_offset

              {[
                 {this_key, value_offset}
                 | rest_of_block(
                     binary_part(block, rel_value_offset, block_size - rel_value_offset),
                     value_offset,
                     this_key
                   )
               ]
               |> Enum.reverse(), {rem_blocks, offset + 2 + block_size}}

            error ->
              {[error], {<<>>, offset}}
          end

        {<<>>, _} ->
          {:halt, :ok}
      end,
      fn _ -> :ok end
    )
  end

  defp rest_of_block(
         <<_format::4, payload_size::12, _payload::binary-size(payload_size), rem_block::binary>>,
         offset,
         base_key
       ) do
    case decode_key_and_value_offset(rem_block, base_key) do
      {this_key, rel_value_offset} ->
        value_offset = offset + 2 + payload_size + rel_value_offset

        [
          {this_key, value_offset}
          | rest_of_block(
              binary_part(rem_block, rel_value_offset, byte_size(rem_block) - rel_value_offset),
              value_offset,
              this_key
            )
        ]

      :not_found ->
        []
    end
  end

  defp rest_of_block(_, _, _), do: [:corrupted]

  defp find_in_rest_of_block(
         <<_format::4, payload_size::12, _payload::binary-size(payload_size), rem_block::binary>>,
         offset,
         base_key,
         key
       ) do
    case decode_key_and_value_offset(rem_block, base_key) do
      {this_key, rel_value_offset} ->
        value_offset = offset + 2 + payload_size + rel_value_offset

        cond do
          key == this_key ->
            [{this_key, value_offset}]

          key < this_key ->
            [
              {this_key, value_offset}
              | find_in_rest_of_block(
                  binary_part(
                    rem_block,
                    rel_value_offset,
                    byte_size(rem_block) - rel_value_offset
                  ),
                  value_offset,
                  this_key,
                  key
                )
            ]

          true ->
            []
        end

      :not_found ->
        []
    end
  end

  defp find_in_rest_of_block(_, _, _, _), do: [:corrupted]
end
