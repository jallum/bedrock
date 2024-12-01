defmodule Bedrock.DataPlane.Storage.Basalt.PageBuildingAndLookupTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.DataPlane.Storage.Basalt.Page

  property "to_blocks correctly encodes key-value pairs into blocks" do
    check all(
            kvs <-
              list_of(
                {binary(min_length: 1, max_length: 200), binary(min_length: 1, max_length: 64)},
                min_length: 1,
                max_length: 1024
              ),
            n_keys_per_block <- positive_integer()
          ) do
      kvs_in_order = kvs |> Enum.sort()
      page = kvs_in_order |> Page.new(n_keys_per_block) |> IO.iodata_to_binary()

      for {key, value} <- kvs_in_order do
        value_offset = Page.lookup(page, key)

        assert value_offset != :not_found

        assert {0, ^value} =
                 Page.decode_value(
                   binary_part(page, value_offset, byte_size(page) - value_offset)
                 )
      end
    end
  end
end
