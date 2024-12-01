defmodule Bedrock.DataPlane.Storage.Basalt.Page.Scanning do
  import Bitwise

  def lookup(page, key), do: find_in_blocks(page, 0, key)

  def find_in_blocks(
        <<block_size::big-16, block::binary-size(block_size), rem_blocks::binary>>,
        offset,
        key
      ) do
    case decode_key_and_value_offset(block, <<>>) do
      {this_key, value_offset} ->
        cond do
          key == this_key ->
            offset + 2 + value_offset

          key < this_key || rem_blocks == <<>> ->
            find_in_rest_of_block(
              binary_part(block, value_offset, block_size - value_offset),
              offset + 2 + value_offset,
              this_key,
              key
            )

          true ->
            find_in_blocks(rem_blocks, offset + 2 + block_size, key)
        end

      error ->
        error
    end
  end

  def find_in_blocks(_blocks, _offset, _key), do: :corrupted

  def find_in_rest_of_block(
        <<_format::4, payload_size::12, _payload::binary-size(payload_size), rem_block::binary>>,
        offset,
        base_key,
        key
      ) do
    case decode_key_and_value_offset(rem_block, base_key) do
      {this_key, value_offset} ->
        cond do
          key == this_key ->
            offset + 2 + payload_size + value_offset

          key < this_key ->
            find_in_rest_of_block(
              binary_part(rem_block, value_offset, byte_size(rem_block) - value_offset),
              offset + 2 + payload_size + value_offset,
              this_key,
              key
            )

          true ->
            :not_found
        end

      error ->
        error
    end
  end

  def find_in_rest_of_block(_, _offset, _base_key, _key), do: :corrupted

  def decode_key_and_value_offset(<<0x00::8, l1::8, l2::8, _::binary>> = encoded_kv, _base_key) do
    if 0x80 == band(l1, 0x80) do
      key_size = bor((l1 &&& 0x7F) <<< 7, l2)
      {binary_part(encoded_kv, 3, key_size), 3 + key_size}
    else
      {binary_part(encoded_kv, 2, l1), 2 + l1}
    end
  end

  def decode_key_and_value_offset(
        <<l1::8, l2::8, l3::8, l4::8, _::binary>> = encoded_kv,
        base_key
      ) do
    {prefix_size, suffix_offset, suffix_size} =
      if 0x80 == band(l1, 0x80) do
        prefix_size = bor((l1 &&& 0x7F) <<< 7, l2)

        if 0x80 == band(l3, 0x80) do
          {prefix_size, 4, bor((l3 &&& 0x7F) <<< 7, l4)}
        else
          {prefix_size, 3, l3}
        end
      else
        if 0x80 == band(l2, 0x80) do
          {l1, 3, bor((l2 &&& 0x7F) <<< 7, l3)}
        else
          {l1, 2, l2}
        end
      end

    key =
      if prefix_size > 0 do
        IO.iodata_to_binary([
          binary_part(base_key, 0, prefix_size),
          binary_part(encoded_kv, suffix_offset, suffix_size)
        ])
      else
        binary_part(encoded_kv, suffix_offset, suffix_size)
      end

    {key, suffix_offset + suffix_size}
  end

  def decode_key_and_value_offset(<<>>, _), do: :not_found
  def decode_key_and_value_offset(_, _), do: :corrupted
end
