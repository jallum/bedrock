defmodule Bedrock.DataPlane.BedrockTransactionTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransaction

  doctest BedrockTransaction

  describe "encode/decode round-trip" do
    test "empty transaction with no mutations or conflicts" do
      transaction = %{
        mutations: [],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = BedrockTransaction.encode(transaction)
      assert is_binary(binary)
      assert {:ok, decoded} = BedrockTransaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with only mutations" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:set, "key2", "value2"},
          {:clear, "key3"},
          {:clear_range, "start", "end"}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, decoded} = BedrockTransaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with write conflicts but no read conflicts" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: [{"start1", "end1"}, {"start2", "end2"}]
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, decoded} = BedrockTransaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with read conflicts and read version" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, decoded} = BedrockTransaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with empty read conflicts encodes and decodes correctly" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, decoded} = BedrockTransaction.decode(binary)

      # Should decode with empty read conflicts tuple
      expected = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      assert decoded == expected
    end

    test "full transaction with all sections" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:clear, "key2"},
          {:clear_range, "start", "end"}
        ],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(98_765), [{"read1", "read2"}]},
        write_conflicts: [{"write1", "write2"}]
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, decoded} = BedrockTransaction.decode(binary)
      assert decoded == transaction
    end
  end

  describe "size optimization" do
    test "automatically selects compact SET variants" do
      # Small key and value should use 8+8 bit lengths (most compact)
      small_transaction = %{
        mutations: [{:set, "k", "v"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      small_binary = BedrockTransaction.encode(small_transaction)

      # Larger value should use 8+16 bit lengths
      medium_transaction = %{
        mutations: [{:set, "k", String.duplicate("x", 300)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      medium_binary = BedrockTransaction.encode(medium_transaction)

      # Large key should use 16+32 bit lengths
      large_transaction = %{
        mutations: [{:set, String.duplicate("k", 300), String.duplicate("v", 70_000)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      large_binary = BedrockTransaction.encode(large_transaction)

      # Verify all decode correctly
      assert {:ok, decoded_small} = BedrockTransaction.decode(small_binary)
      assert {:ok, decoded_medium} = BedrockTransaction.decode(medium_binary)
      assert {:ok, decoded_large} = BedrockTransaction.decode(large_binary)

      assert decoded_small == small_transaction
      assert decoded_medium == medium_transaction
      assert decoded_large == large_transaction

      # Small transaction should be most compact
      assert byte_size(small_binary) < byte_size(medium_binary)
      assert byte_size(medium_binary) < byte_size(large_binary)
    end

    test "automatically selects compact CLEAR variants" do
      # Small key should use 8-bit length
      small_clear = %{
        mutations: [{:clear, "k"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      # Large key should use 16-bit length
      large_clear = %{
        mutations: [{:clear, String.duplicate("k", 300)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      small_binary = BedrockTransaction.encode(small_clear)
      large_binary = BedrockTransaction.encode(large_clear)

      assert {:ok, decoded_small} = BedrockTransaction.decode(small_binary)
      assert {:ok, decoded_large} = BedrockTransaction.decode(large_binary)

      assert decoded_small == small_clear
      assert decoded_large == large_clear
      assert byte_size(small_binary) < byte_size(large_binary)
    end

    test "automatically selects compact CLEAR_RANGE variants" do
      # Small keys should use 8-bit lengths
      small_range = %{
        mutations: [{:clear_range, "a", "z"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      # Large keys should use 16-bit lengths
      large_range = %{
        mutations: [{:clear_range, String.duplicate("a", 300), String.duplicate("z", 300)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      small_binary = BedrockTransaction.encode(small_range)
      large_binary = BedrockTransaction.encode(large_range)

      assert {:ok, decoded_small} = BedrockTransaction.decode(small_binary)
      assert {:ok, decoded_large} = BedrockTransaction.decode(large_binary)

      assert decoded_small == small_range
      assert decoded_large == large_range
      assert byte_size(small_binary) < byte_size(large_binary)
    end
  end

  describe "validation" do
    test "validates binary format integrity" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, validated} = BedrockTransaction.validate(binary)
      assert validated == binary
    end

    test "detects corrupted magic number" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)

      # Corrupt magic number
      <<_::32, rest::binary>> = binary
      corrupted = <<0x00000000::32, rest::binary>>

      assert {:error, :invalid_format} = BedrockTransaction.validate(corrupted)
      assert {:error, :invalid_format} = BedrockTransaction.decode(corrupted)
    end

    test "detects section CRC corruption" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)

      # Corrupt a byte in the middle (should affect section CRC)
      <<prefix::binary-size(20), _byte, suffix::binary>> = binary
      corrupted = <<prefix::binary, 0xFF, suffix::binary>>

      assert {:error, {:section_checksum_mismatch, _tag}} = BedrockTransaction.validate(corrupted)
    end
  end

  describe "section operations" do
    test "extracts sections by tag" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = BedrockTransaction.encode(transaction)

      # Should always find MUTATIONS section
      assert {:ok, mutations_payload} = BedrockTransaction.extract_section(binary, 0x01)
      assert is_binary(mutations_payload)
      assert byte_size(mutations_payload) > 0

      # Should find READ_CONFLICTS section when present
      assert {:ok, read_conflicts_payload} = BedrockTransaction.extract_section(binary, 0x02)
      assert is_binary(read_conflicts_payload)

      # Should find WRITE_CONFLICTS section when present
      assert {:ok, write_conflicts_payload} = BedrockTransaction.extract_section(binary, 0x03)
      assert is_binary(write_conflicts_payload)

      # Should not find TRANSACTION_ID section unless added
      assert {:error, :section_not_found} = BedrockTransaction.extract_section(binary, 0x04)
    end

    test "lists all present sections" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, tags} = BedrockTransaction.list_sections(binary)

      # MUTATIONS always present
      assert 0x01 in tags
      # READ_CONFLICTS present when conflicts and read version exist
      assert 0x02 in tags
      # WRITE_CONFLICTS present when write conflicts exist
      assert 0x03 in tags
      # TRANSACTION_ID not present by default
      refute 0x04 in tags
    end

    test "adds transaction ID section" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, nil} = BedrockTransaction.extract_transaction_id(binary)

      # Add transaction ID
      version = Bedrock.DataPlane.Version.from_integer(98_765)
      assert {:ok, stamped} = BedrockTransaction.add_transaction_id(binary, version)
      assert {:ok, ^version} = BedrockTransaction.extract_transaction_id(stamped)

      # Original transaction should decode the same
      assert {:ok, decoded} = BedrockTransaction.decode(stamped)
      assert decoded == transaction
    end

    test "removes sections" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [{"write_start", "write_end"}],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, write_conflicts} = BedrockTransaction.extract_write_conflicts(binary)
      assert write_conflicts == [{"write_start", "write_end"}]

      # Remove WRITE_CONFLICTS section
      assert {:ok, without_conflicts} = BedrockTransaction.remove_section(binary, 0x03)
      assert {:ok, []} = BedrockTransaction.extract_write_conflicts(without_conflicts)

      # Should decode with empty write conflicts
      assert {:ok, decoded} = BedrockTransaction.decode(without_conflicts)
      assert decoded.write_conflicts == []
    end
  end

  describe "convenience functions" do
    test "extracts read version" do
      # Transaction with read version
      with_version = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: []
      }

      binary_with_version = BedrockTransaction.encode(with_version)
      expected_version = Bedrock.DataPlane.Version.from_integer(12_345)

      assert {:ok, ^expected_version} =
               BedrockTransaction.extract_read_version(binary_with_version)

      # Transaction without read version
      without_version = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary_without_version = BedrockTransaction.encode(without_version)
      assert {:ok, nil} = BedrockTransaction.extract_read_version(binary_without_version)
    end

    test "extracts conflicts" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(12_345), [{"read1", "read2"}]},
        write_conflicts: [{"write1", "write2"}]
      }

      binary = BedrockTransaction.encode(transaction)

      expected_version = Bedrock.DataPlane.Version.from_integer(12_345)

      assert {:ok, {^expected_version, [{"read1", "read2"}]}} =
               BedrockTransaction.extract_read_conflicts(binary)

      assert {:ok, [{"write1", "write2"}]} = BedrockTransaction.extract_write_conflicts(binary)

      # Empty conflicts
      empty_transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      empty_binary = BedrockTransaction.encode(empty_transaction)
      assert {:ok, {nil, []}} = BedrockTransaction.extract_read_conflicts(empty_binary)
      assert {:ok, []} = BedrockTransaction.extract_write_conflicts(empty_binary)
    end

    test "streams mutations" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:set, "key2", "value2"},
          {:clear, "key3"},
          {:clear_range, "start", "end"}
        ],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)
      assert {:ok, stream} = BedrockTransaction.stream_mutations(binary)

      mutations = stream |> Enum.to_list()
      assert mutations == transaction.mutations
    end
  end

  describe "error handling" do
    test "handles invalid binary format" do
      assert {:error, :invalid_format} = BedrockTransaction.decode(<<>>)
      assert {:error, :invalid_format} = BedrockTransaction.decode(<<1, 2, 3>>)
      assert {:error, :invalid_format} = BedrockTransaction.validate(<<>>)
    end

    test "handles truncated data" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)

      # Truncate the binary
      truncated = binary_part(binary, 0, byte_size(binary) - 5)

      assert {:error, _} = BedrockTransaction.decode(truncated)
    end

    test "handles section extraction from non-existent sections" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)

      # Try to extract non-existent sections
      assert {:error, :section_not_found} = BedrockTransaction.extract_section(binary, 0x02)
      assert {:error, :section_not_found} = BedrockTransaction.extract_section(binary, 0x03)
      assert {:error, :section_not_found} = BedrockTransaction.extract_section(binary, 0x04)
    end

    test "handles adding duplicate sections" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = BedrockTransaction.encode(transaction)

      # Try to add MUTATIONS section (already exists)
      assert {:error, :section_already_exists} =
               BedrockTransaction.add_section(binary, 0x01, <<>>)
    end
  end

  describe "binary format structure" do
    test "has correct magic number and version" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      <<magic::unsigned-big-32, version, _flags, _section_count::unsigned-big-16, _rest::binary>> =
        BedrockTransaction.encode(transaction)

      # "BRDT"
      assert magic == 0x42524454
      assert version == 0x01
    end

    test "sections are order independent" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      # Encode multiple times - sections might appear in different orders
      binaries = for _ <- 1..10, do: BedrockTransaction.encode(transaction)

      # All should decode to the same result regardless of section order
      for binary <- binaries do
        assert {:ok, decoded} = BedrockTransaction.decode(binary)
        assert decoded == transaction
      end
    end
  end
end
