defmodule Bedrock.DataPlane.Storage.Basalt.Page do
  alias Bedrock.DataPlane.Storage.Basalt.Page.Building
  alias Bedrock.DataPlane.Storage.Basalt.Page.Scanning

  @type value_offset :: pos_integer()
  @type key_value :: {key :: binary(), value :: binary()}

  @spec lookup(page :: binary(), key :: binary()) :: value_offset() | :not_found | :corrupted
  def lookup(page, key),
    do: Scanning.lookup(page, key)

  @spec new([key_value()], n_keys_per_block :: pos_integer()) :: iodata()
  def new(kv, n_keys_per_block \\ 16), do: Building.to_blocks(kv, n_keys_per_block)

  def decode_value(
        <<format::4, payload_size::big-12, payload::binary-size(payload_size), _::binary>>
      ),
      do: {format, payload}
end
