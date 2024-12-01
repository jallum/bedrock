defmodule Bedrock.DataPlane.Storage.Basalt.Page.Building do
  import Bitwise

  @spec to_blocks([{key :: binary(), value :: binary()}], n_keys_per_block :: pos_integer()) ::
          [iodata()]
  def to_blocks([], _n_keys_per_block), do: []

  def to_blocks(kv, n_keys_per_block) do
    {block, rest} = Enum.split(kv, n_keys_per_block)
    [encode_block(block |> Enum.reverse()) | to_blocks(rest, n_keys_per_block)]
  end

  defp encode_block([{key, value} | rem_kv]) do
    block =
      [
        encode_key(key, <<>>),
        encode_value(value)
        | encode_block(rem_kv, key)
      ]

    [<<IO.iodata_length(block)::big-16>> | block]
  end

  defp encode_block([{key, value} | rem_kv], base_key) do
    [
      encode_key(key, base_key),
      encode_value(value)
      | encode_block(rem_kv, key)
    ]
  end

  defp encode_block([], _), do: []

  defp encode_key(key, <<>>) when byte_size(key) < 0x80, do: [<<0x00::8, byte_size(key)::8>>, key]

  defp encode_key(key, <<>>) when byte_size(key) < 0x8000,
    do: [
      <<0x00::8, bor(0x80, band(byte_size(key) >>> 7, 0x7F))::8, band(byte_size(key), 0x7F)::8>>,
      key
    ]

  defp encode_key(key, base_key) do
    prefix_size = :binary.longest_common_prefix([base_key, key])
    suffix_size = byte_size(key) - prefix_size

    [
      encode_variable_length(prefix_size),
      encode_variable_length(suffix_size),
      binary_part(key, prefix_size, suffix_size)
    ]
  end

  defp encode_variable_length(l) when l < 0x80, do: <<l::8>>

  defp encode_variable_length(l) when l < 0x8000,
    do: <<bor(0x80, band(l >>> 7, 0x7F))::8, band(l, 0x7F)::8>>

  defp encode_value(value) when byte_size(value) < 0x1000,
    do: [<<0::4, byte_size(value)::big-12>>, value]
end
