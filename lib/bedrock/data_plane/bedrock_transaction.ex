defmodule Bedrock.DataPlane.BedrockTransaction do
  import Bitwise, only: [>>>: 2, &&&: 2, <<<: 2, |||: 2]

  @moduledoc """
  Tagged binary transaction encoding for Bedrock that supports efficient operations
  and extensibility through self-describing sections with embedded CRC validation.

  This module implements a comprehensive tagged binary format that replaces the
  simple map structure from `Tx.commit/1` with an efficient binary format featuring:

  - **Tagged sections**: Self-describing sections with type, size, and embedded CRC
  - **Order independence**: Sections can appear in any order for better extensibility
  - **Elegant opcode format**: 5-bit operations + 3-bit variants with size optimization
  - **Section omission**: Empty sections are completely omitted to save space
  - **Efficient operations**: Extract specific sections without full decode
  - **Robust validation**: Self-validating CRCs detect any bit corruption

  ## Transaction Structure

  Input/output transaction map:
  ```elixir
  %{
    mutations: [Tx.mutation()],              # optional - {:set, binary(), binary()} | {:clear_range, binary(), binary()}
    read_conflicts: [{binary(), binary()}],  # optional - omit section if empty
    write_conflicts: [{binary(), binary()}], # optional - omit section if empty
    read_version: Bedrock.version(),         # optional - stored in READ_CONFLICTS section
    commit_version: Bedrock.version()        # optional - assigned by commit proxy
  }
  ```

  ## Binary Format

  ```text
  [OVERALL HEADER - 8 bytes]
    - 4 bytes: Magic number (0x42524454 = "BRDT")
    - 1 byte: Format version (0x01)
    - 1 byte: Flags (reserved, set to 0)
    - 2 bytes: Section count (big-endian)

  [SECTION 1: header + payload]
  [SECTION 2: header + payload]
  ...
  [SECTION N: header + payload]
  ```

  ## Section Format

  Each section has an 8-byte header with CRC validation:
  ```text
  [SECTION HEADER - 8 bytes]
    - 1 byte: Section tag
    - 3 bytes: Section payload size (24-bit big-endian, max 16MB)
    - 4 bytes: Section CRC32 (standard CRC32 over tag + size + payload)

  [SECTION PAYLOAD - variable size]
    - Section-specific data format
  ```

  ## CRC Validation

  Uses standard CRC32 calculation and validation:
  - **Encoding**: Calculate CRC32 over tag + size + payload, store value directly
  - **Validation**: Recalculate CRC32 and compare with stored value
  - **Coverage**: CRC covers tag, size, and payload for robust error detection

  ## Section Types

  - 0x01: MUTATIONS - Always present (may be empty)
  - 0x02: READ_CONFLICTS - Only present when read conflicts exist AND read version available
  - 0x03: WRITE_CONFLICTS - Only present when write conflicts exist
  - 0x04: COMMIT_VERSION - Only present when stamped by commit proxy

  ## Flexible Section Usage

  All sections of BedrockTransaction are optional and used differently throughout the system:

  **Transaction Builder → Commit Proxy:**
  - mutations: Contains all mutations ({:set, key, value}, {:clear_range, start, end})
  - read_conflicts: Contains read conflict ranges (if any reads were performed)
  - write_conflicts: Contains write conflict ranges for all mutations
  - read_version: Present if transaction performed reads
  - commit_version: Not present (will be assigned by commit proxy)

  **Commit Proxy → Resolver:**
  - mutations: Not needed for conflict resolution
  - read_conflicts: Required for conflict detection
  - write_conflicts: Required for conflict detection
  - read_version: Used for conflict resolution if present
  - commit_version: Present after assignment by commit proxy

  **Commit Proxy → Logs:**
  - mutations: Required for storage operations
  - read_conflicts: Not needed for storage
  - write_conflicts: Not needed for storage
  - read_version: Not needed for storage
  - commit_version: Present for transaction ordering

  This flexible design allows each component to work with only the sections it needs,
  improving efficiency and reducing data transfer overhead.

  ## Elegant Opcode Format

  Mutations use a 5-bit operation + 3-bit variant structure for optimal size:

  **SET Operation (0x00 << 3):**
  - 0x00: 16-bit key + 32-bit value lengths
  - 0x01: 8-bit key + 16-bit value lengths
  - 0x02: 8-bit key + 8-bit value lengths (most compact)

  **CLEAR Operation (0x01 << 3):**
  - 0x08: Single key, 16-bit length
  - 0x09: Single key, 8-bit length
  - 0x0A: Range, 16-bit lengths
  - 0x0B: Range, 8-bit lengths (most compact)

  Size optimization automatically selects the most compact variant.
  """

  @type transaction_map :: Bedrock.transaction()

  @type encoded :: binary()

  @type section_tag :: 0x01..0xFF
  @type opcode :: 0x00..0xFF

  # Magic number "BRDT" for Bedrock Transaction
  @magic_number 0x42524454
  @format_version 0x01

  # Section tags
  @mutations_tag 0x01
  @read_conflicts_tag 0x02
  @write_conflicts_tag 0x03
  @commit_version_tag 0x04

  # Operation types (upper 5 bits) - not used directly but document the design

  # SET variants (lower 3 bits)
  # 16-bit key + 32-bit value
  @set_16_32 0x00
  # 8-bit key + 16-bit value
  @set_8_16 0x01
  # 8-bit key + 8-bit value
  @set_8_8 0x02

  # CLEAR variants (lower 3 bits + base operation)
  # Single key, 16-bit length
  @clear_single_16 0x08
  # Single key, 8-bit length
  @clear_single_8 0x09
  # Range, 16-bit lengths
  @clear_range_16 0x0A
  # Range, 8-bit lengths
  @clear_range_8 0x0B

  # Operation types (for dynamic opcode construction)
  @set_operation 0x00
  @clear_operation 0x01

  # ============================================================================
  # CORE API
  # ============================================================================

  @doc """
  Encodes a transaction map into the tagged binary format.

  Automatically selects the most compact opcode variants based on data sizes
  and omits empty sections for optimal space efficiency.

  ## Options
  - `:include_commit_version` - Include placeholder commit version section
  - `:include_transaction_id` - DEPRECATED: Use `:include_commit_version` instead
  """
  @spec encode(transaction_map()) :: binary()
  def encode(transaction) do
    sections =
      []
      |> add_mutations_section(transaction)
      |> add_read_conflicts_section(transaction)
      |> add_write_conflicts_section(transaction)
      |> add_commit_version_section(transaction)

    overall_header = encode_overall_header(length(sections))

    IO.iodata_to_binary([overall_header | sections])
  end

  defp add_mutations_section(sections, %{mutations: mutations}) do
    payload = encode_mutations_payload(mutations)
    section = encode_section(@mutations_tag, payload)
    [section | sections]
  end

  defp add_mutations_section(sections, _), do: sections

  defp add_read_conflicts_section(sections, %{
         read_conflicts: {read_version, read_conflicts}
       }) do
    # Validate coupling rules: both must be nil/empty or both must be non-nil/non-empty
    case {read_version, read_conflicts} do
      {nil, []} ->
        # Both are nil/empty - don't include section
        sections

      {nil, [_ | _]} ->
        # Error: read_version is nil but read_conflicts is non-empty
        raise ArgumentError, "read_version is nil but read_conflicts is non-empty"

      {_non_nil_version, []} ->
        # Error: read_version is non-nil but read_conflicts is empty
        raise ArgumentError, "read_version is non-nil but read_conflicts is empty"

      {_non_nil_version, [_ | _]} ->
        # Both are non-nil/non-empty - include section
        payload = encode_read_conflicts_payload(read_conflicts, read_version)
        section = encode_section(@read_conflicts_tag, payload)
        [section | sections]
    end
  end

  defp add_read_conflicts_section(sections, _), do: sections

  defp add_write_conflicts_section(sections, %{write_conflicts: []}), do: sections

  defp add_write_conflicts_section(sections, %{write_conflicts: write_conflicts}) do
    payload = encode_write_conflicts_payload(write_conflicts)
    section = encode_section(@write_conflicts_tag, payload)
    [section | sections]
  end

  defp add_write_conflicts_section(sections, _), do: sections

  defp add_commit_version_section(sections, %{commit_version: commit_version}) do
    payload = <<commit_version::unsigned-big-64>>
    section = encode_section(@commit_version_tag, payload)
    [section | sections]
  end

  defp add_commit_version_section(sections, _), do: sections

  @doc """
  Decodes a tagged binary transaction back to the transaction map format.

  Validates all section CRCs and handles missing sections appropriately.
  """
  @spec decode(binary()) :: {:ok, transaction_map()} | {:error, reason :: term()}
  def decode(
        <<@magic_number::unsigned-big-32, @format_version, _flags, section_count::unsigned-big-16,
          sections_data::binary>>
      ) do
    with {:ok, sections} <- parse_sections(sections_data, section_count) do
      build_transaction_from_sections(sections)
    end
  end

  def decode(_), do: {:error, :invalid_format}

  @doc """
  Decodes a tagged binary transaction back to the transaction map format.

  Raises on error. Use this variant when you're confident the binary is valid
  or in test scenarios where you want to fail fast on invalid data.
  """
  @spec decode!(binary()) :: transaction_map()
  def decode!(binary_transaction) do
    case decode(binary_transaction) do
      {:ok, transaction} -> transaction
      {:error, reason} -> raise "Failed to decode BedrockTransaction: #{inspect(reason)}"
    end
  end

  @doc """
  Validates the binary format integrity using section CRCs.

  Each section validates independently using standard CRC validation
  by recalculating CRC32 over tag + size + payload and comparing with stored CRC.
  """
  @spec validate(binary()) :: {:ok, binary()} | {:error, reason :: term()}
  def validate(
        <<@magic_number::unsigned-big-32, @format_version, _flags, section_count::unsigned-big-16,
          sections_data::binary>> = transaction
      ) do
    case validate_all_sections(sections_data, section_count) do
      :ok -> {:ok, transaction}
      error -> error
    end
  end

  def validate(_), do: {:error, :invalid_format}

  # ============================================================================
  # SECTION OPERATIONS API
  # ============================================================================

  @doc """
  Extracts a specific section payload by tag without full transaction decode.
  """
  @spec extract_section(binary(), section_tag()) :: {:ok, binary()} | {:error, reason :: term()}
  def extract_section(encoded_transaction, target_tag) do
    case parse_transaction_header(encoded_transaction) do
      {:ok, {section_count, sections_data}} ->
        find_section_by_tag(sections_data, target_tag, section_count)

      error ->
        error
    end
  end

  @doc """
  Lists all section tags present in the transaction.
  """
  @spec list_sections(binary()) :: {:ok, [section_tag()]} | {:error, reason :: term()}
  def list_sections(encoded_transaction) do
    case parse_transaction_header(encoded_transaction) do
      {:ok, {section_count, sections_data}} ->
        collect_section_tags(sections_data, section_count, [])

      error ->
        error
    end
  end

  @doc """
  Adds a new section to the transaction.

  Returns error if section already exists. Use for adding COMMIT_VERSION section
  after commit proxy processing.
  """
  @spec add_section(binary(), section_tag(), binary()) ::
          {:ok, binary()} | {:error, reason :: term()}
  def add_section(encoded_transaction, section_tag, payload) do
    case parse_all_sections(encoded_transaction) do
      {:ok, {_overall_header, sections_map}} ->
        if Map.has_key?(sections_map, section_tag) do
          {:error, :section_already_exists}
        else
          new_sections_map = Map.put(sections_map, section_tag, payload)
          rebuild_transaction(new_sections_map)
        end

      error ->
        error
    end
  end

  @doc """
  Removes a section from the transaction.
  """
  @spec remove_section(binary(), section_tag()) :: {:ok, binary()} | {:error, reason :: term()}
  def remove_section(encoded_transaction, section_tag) do
    case parse_all_sections(encoded_transaction) do
      {:ok, {_overall_header, sections_map}} ->
        new_sections_map = Map.delete(sections_map, section_tag)
        rebuild_transaction(new_sections_map)

      error ->
        error
    end
  end

  # ============================================================================
  # CONVENIENCE FUNCTIONS
  # ============================================================================

  @doc """
  Extracts the read version from the READ_CONFLICTS section.

  Returns nil if no READ_CONFLICTS section exists (write-only transaction).
  """
  @spec extract_read_version(binary()) ::
          {:ok, Bedrock.version() | nil} | {:error, reason :: term()}
  def extract_read_version(encoded_transaction) do
    case extract_section(encoded_transaction, @read_conflicts_tag) do
      {:ok, payload} ->
        case decode_read_conflicts_payload(payload) do
          {:ok, {read_version, _conflicts}} -> {:ok, read_version}
          error -> error
        end

      {:error, :section_not_found} ->
        {:ok, nil}

      error ->
        error
    end
  end

  @doc """
  Extracts read conflicts and read version from READ_CONFLICTS section.

  Returns {nil, []} if no READ_CONFLICTS section exists.
  """
  @spec extract_read_conflicts(binary()) ::
          {:ok, {Bedrock.version() | nil, [{binary(), binary()}]}} | {:error, reason :: term()}
  def extract_read_conflicts(encoded_transaction) do
    case extract_section(encoded_transaction, @read_conflicts_tag) do
      {:ok, payload} ->
        decode_read_conflicts_payload(payload)

      {:error, :section_not_found} ->
        {:ok, {nil, []}}

      error ->
        error
    end
  end

  @doc """
  Extracts write conflicts from WRITE_CONFLICTS section.

  Returns empty list if no WRITE_CONFLICTS section exists.
  """
  @spec extract_write_conflicts(binary()) ::
          {:ok, [{binary(), binary()}]} | {:error, reason :: term()}
  def extract_write_conflicts(encoded_transaction) do
    case extract_section(encoded_transaction, @write_conflicts_tag) do
      {:ok, payload} ->
        decode_write_conflicts_payload(payload)

      {:error, :section_not_found} ->
        {:ok, []}

      error ->
        error
    end
  end

  @doc """
  Creates a stream of mutations from the MUTATIONS section.

  Enables processing large transactions without loading all mutations into memory.
  """
  @spec stream_mutations(binary()) :: {:ok, Enumerable.t()} | {:error, reason :: term()}
  def stream_mutations(encoded_transaction) do
    case extract_section(encoded_transaction, @mutations_tag) do
      {:ok, mutations_payload} ->
        stream =
          Stream.resource(
            fn -> mutations_payload end,
            &stream_next_mutation/1,
            fn _ -> :ok end
          )

        {:ok, stream}

      error ->
        error
    end
  end

  @doc """
  Adds a transaction ID to an existing transaction.

  DEPRECATED: Use add_commit_version/2 instead. This function is kept for backward compatibility.
  """
  @spec add_transaction_id(binary(), binary()) :: {:ok, binary()} | {:error, reason :: term()}
  def add_transaction_id(encoded_transaction, transaction_id)
      when is_binary(transaction_id) and byte_size(transaction_id) == 8 do
    add_section(encoded_transaction, @commit_version_tag, transaction_id)
  end

  @doc """
  Extracts the transaction ID if present.

  DEPRECATED: Use extract_commit_version/1 instead. This function is kept for backward compatibility.

  Returns nil if no COMMIT_VERSION section exists.
  """
  @spec extract_transaction_id(binary()) :: {:ok, binary() | nil} | {:error, reason :: term()}
  def extract_transaction_id(encoded_transaction) do
    case extract_section(encoded_transaction, @commit_version_tag) do
      {:ok, <<_::unsigned-big-64>> = tx_id} -> {:ok, tx_id}
      {:error, :section_not_found} -> {:ok, nil}
      error -> error
    end
  end

  @doc """
  Adds a commit version to an existing transaction.

  This is the preferred name for adding commit versions. The old function name
  add_transaction_id/2 is kept for backward compatibility.
  """
  @spec add_commit_version(binary(), binary()) :: {:ok, binary()} | {:error, reason :: term()}
  def add_commit_version(encoded_transaction, commit_version)
      when is_binary(commit_version) and byte_size(commit_version) == 8 do
    add_section(encoded_transaction, @commit_version_tag, commit_version)
  end

  @doc """
  Extracts the commit version if present.

  This is the preferred name for extracting commit versions. The old function name
  extract_transaction_id/1 is kept for backward compatibility.

  Returns nil if no COMMIT_VERSION section exists.
  """
  @spec extract_commit_version(binary()) :: {:ok, binary() | nil} | {:error, reason :: term()}
  def extract_commit_version(encoded_transaction) do
    case extract_section(encoded_transaction, @commit_version_tag) do
      {:ok, <<_::unsigned-big-64>> = version} -> {:ok, version}
      {:error, :section_not_found} -> {:ok, nil}
      error -> error
    end
  end

  # ============================================================================
  # DYNAMIC OPCODE CONSTRUCTION
  # ============================================================================

  @doc """
  Builds an opcode dynamically using bitwise operations.

  Uses (operation <<< 3) ||| variant for elegant 5-bit operation + 3-bit variant format.
  """
  @spec build_opcode(operation :: 0..31, variant :: 0..7) :: opcode()
  def build_opcode(operation, variant) when operation <= 31 and variant <= 7 do
    operation <<< 3 ||| variant
  end

  @doc """
  Extracts the operation type from an opcode.

  Uses >>> to extract the upper 5 bits.
  """
  @spec extract_operation(opcode()) :: 0..31
  def extract_operation(opcode) when opcode <= 255 do
    opcode >>> 3
  end

  @doc """
  Extracts the variant from an opcode.

  Uses &&& to extract the lower 3 bits.
  """
  @spec extract_variant(opcode()) :: 0..7
  def extract_variant(opcode) when opcode <= 255 do
    opcode &&& 0x07
  end

  @doc """
  Selects the optimal opcode variant for SET operations based on key and value sizes.

  Automatically chooses the most compact variant that can hold the data.
  """
  @spec optimize_set_opcode(key_size :: non_neg_integer(), value_size :: non_neg_integer()) ::
          opcode()
  def optimize_set_opcode(key_size, value_size) do
    cond do
      key_size <= 255 and value_size <= 255 ->
        # SET_8_8
        build_opcode(@set_operation, 2)

      key_size <= 255 and value_size <= 65_535 ->
        # SET_8_16
        build_opcode(@set_operation, 1)

      true ->
        # SET_16_32
        build_opcode(@set_operation, 0)
    end
  end

  @doc """
  Selects the optimal opcode variant for CLEAR operations based on key sizes.

  Automatically chooses the most compact variant that can hold the data.
  """
  @spec optimize_clear_opcode(key_size :: non_neg_integer(), is_range :: boolean()) :: opcode()
  def optimize_clear_opcode(key_size, false = _is_range) when key_size <= 255 do
    # CLEAR_SINGLE_8
    build_opcode(@clear_operation, 1)
  end

  def optimize_clear_opcode(_key_size, false = _is_range) do
    # CLEAR_SINGLE_16
    build_opcode(@clear_operation, 0)
  end

  def optimize_clear_opcode(max_key_size, true = _is_range) when max_key_size <= 255 do
    # CLEAR_RANGE_8 (variant 3 = 0x0B)
    build_opcode(@clear_operation, 3)
  end

  def optimize_clear_opcode(_max_key_size, true = _is_range) do
    # CLEAR_RANGE_16 (variant 2 = 0x0A)
    build_opcode(@clear_operation, 2)
  end

  # ============================================================================
  # ENCODING IMPLEMENTATION
  # ============================================================================

  defp encode_overall_header(section_count) do
    <<
      # Magic number "BRDT"
      @magic_number::unsigned-big-32,
      # Format version 1
      @format_version,
      # Flags (all reserved)
      0x00,
      # Section count
      section_count::unsigned-big-16
    >>
  end

  defp encode_section(tag, payload) do
    payload_size = byte_size(payload)

    # Calculate CRC32 over tag + size + payload (standard approach)
    section_content = <<tag, payload_size::unsigned-big-24, payload::binary>>
    section_crc = :erlang.crc32(section_content)

    <<
      # 1 byte tag
      tag,
      # 3 byte size (max 16MB)
      payload_size::unsigned-big-24,
      # 4 byte CRC32
      section_crc::unsigned-big-32,
      # Variable size payload
      payload::binary
    >>
  end

  defp encode_mutations_payload(mutations) do
    mutations_data = Enum.map(mutations, &encode_mutation_opcode/1)
    IO.iodata_to_binary(mutations_data)
  end

  # Elegant opcode encoding with automatic size optimization using dynamic opcode construction
  defp encode_mutation_opcode({:set, key, value}) do
    key_len = byte_size(key)
    value_len = byte_size(value)
    opcode = optimize_set_opcode(key_len, value_len)

    case extract_variant(opcode) do
      2 ->
        # SET_8_8: 8-bit key length + 8-bit value length (most compact)
        <<opcode, key_len::unsigned-8, key::binary, value_len::unsigned-8, value::binary>>

      1 ->
        # SET_8_16: 8-bit key length + 16-bit value length
        <<opcode, key_len::unsigned-8, key::binary, value_len::unsigned-big-16, value::binary>>

      0 ->
        # SET_16_32: 16-bit key length + 32-bit value length (full size)
        <<opcode, key_len::unsigned-big-16, key::binary, value_len::unsigned-big-32,
          value::binary>>
    end
  end

  defp encode_mutation_opcode({:clear, key}) do
    key_len = byte_size(key)
    opcode = optimize_clear_opcode(key_len, false)

    case extract_variant(opcode) do
      1 ->
        # CLEAR_SINGLE_8: Single key (8-bit key length)
        <<opcode, key_len::unsigned-8, key::binary>>

      0 ->
        # CLEAR_SINGLE_16: Single key (16-bit key length)
        <<opcode, key_len::unsigned-big-16, key::binary>>
    end
  end

  defp encode_mutation_opcode({:clear_range, start_key, end_key}) do
    start_len = byte_size(start_key)
    end_len = byte_size(end_key)
    # Use the maximum key size to determine the optimal variant
    max_key_len = max(start_len, end_len)
    opcode = optimize_clear_opcode(max_key_len, true)

    case extract_variant(opcode) do
      3 ->
        # CLEAR_RANGE_8: Range (8-bit start/end lengths)
        <<opcode, start_len::unsigned-8, start_key::binary, end_len::unsigned-8, end_key::binary>>

      2 ->
        # CLEAR_RANGE_16: Range (16-bit start/end lengths)
        <<opcode, start_len::unsigned-big-16, start_key::binary, end_len::unsigned-big-16,
          end_key::binary>>
    end
  end

  defp encode_read_conflicts_payload(read_conflicts, read_version) do
    conflict_count = length(read_conflicts)
    conflicts_data = Enum.map(read_conflicts, &encode_conflict_range/1)

    # Convert read_version to integer for encoding
    read_version_value =
      case read_version do
        nil -> -1
        version when is_integer(version) -> version
        version -> Bedrock.DataPlane.Version.to_integer(version)
      end

    IO.iodata_to_binary([
      <<read_version_value::signed-big-64, conflict_count::unsigned-big-32>>,
      conflicts_data
    ])
  end

  defp encode_write_conflicts_payload(write_conflicts) do
    conflict_count = length(write_conflicts)
    conflicts_data = Enum.map(write_conflicts, &encode_conflict_range/1)

    IO.iodata_to_binary([
      <<conflict_count::unsigned-big-32>>,
      conflicts_data
    ])
  end

  defp encode_conflict_range({start_key, end_key}) do
    start_len = byte_size(start_key)
    end_len = byte_size(end_key)
    <<start_len::unsigned-big-16, start_key::binary, end_len::unsigned-big-16, end_key::binary>>
  end

  # ============================================================================
  # DECODING IMPLEMENTATION
  # ============================================================================

  defp parse_sections(data, section_count) do
    parse_sections(data, section_count, %{})
  end

  defp parse_sections(_data, 0, sections_map) do
    {:ok, sections_map}
  end

  defp parse_sections(
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32,
           payload::binary-size(payload_size), rest::binary>>,
         remaining_count,
         sections_map
       ) do
    # Verify section CRC using standard approach
    section_content = <<tag, payload_size::unsigned-big-24, payload::binary>>
    calculated_crc = :erlang.crc32(section_content)

    if calculated_crc == stored_crc do
      new_sections_map = Map.put(sections_map, tag, payload)
      parse_sections(rest, remaining_count - 1, new_sections_map)
    else
      {:error, {:section_checksum_mismatch, tag}}
    end
  end

  defp parse_sections(_, _, _) do
    {:error, :truncated_sections}
  end

  defp build_transaction_from_sections(sections) do
    transaction = %{
      mutations: [],
      write_conflicts: [],
      read_conflicts: {nil, []}
    }

    with {:ok, transaction} <-
           maybe_decode_mutations(transaction, Map.get(sections, @mutations_tag)),
         {:ok, transaction} <-
           maybe_decode_read_conflicts(transaction, Map.get(sections, @read_conflicts_tag)) do
      maybe_decode_write_conflicts(transaction, Map.get(sections, @write_conflicts_tag))
    end
  end

  defp maybe_decode_mutations(transaction, nil), do: {:ok, transaction}

  defp maybe_decode_mutations(transaction, payload) do
    with {:ok, mutations} <- decode_mutations_payload(payload) do
      {:ok, %{transaction | mutations: mutations}}
    end
  end

  defp decode_mutations_payload(payload) do
    payload
    |> stream_mutations_opcodes([])
    |> case do
      {:ok, mutations} -> {:ok, Enum.reverse(mutations)}
      error -> error
    end
  end

  defp stream_mutations_opcodes(<<>>, mutations) do
    {:ok, mutations}
  end

  # SET operations (0x00 << 3)
  defp stream_mutations_opcodes(
         <<@set_16_32, key_len::unsigned-big-16, key::binary-size(key_len),
           value_len::unsigned-big-32, value::binary-size(value_len), rest::binary>>,
         mutations
       ) do
    mutation = {:set, key, value}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(
         <<@set_8_16, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-big-16,
           value::binary-size(value_len), rest::binary>>,
         mutations
       ) do
    mutation = {:set, key, value}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(
         <<@set_8_8, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-8,
           value::binary-size(value_len), rest::binary>>,
         mutations
       ) do
    mutation = {:set, key, value}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  # CLEAR operations (0x01 << 3)
  defp stream_mutations_opcodes(
         <<@clear_single_16, key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear, key}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(
         <<@clear_single_8, key_len::unsigned-8, key::binary-size(key_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear, key}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(
         <<@clear_range_16, start_len::unsigned-big-16, start_key::binary-size(start_len),
           end_len::unsigned-big-16, end_key::binary-size(end_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear_range, start_key, end_key}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(
         <<@clear_range_8, start_len::unsigned-8, start_key::binary-size(start_len),
           end_len::unsigned-8, end_key::binary-size(end_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear_range, start_key, end_key}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  # Reserved opcodes
  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations)
       when opcode in 0x03..0x07 do
    {:error, {:reserved_set_variant, opcode}}
  end

  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations)
       when opcode in 0x0C..0x0F do
    {:error, {:reserved_clear_variant, opcode}}
  end

  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations)
       when opcode in 0x10..0xFF do
    # Extract upper 5 bits
    operation_type = opcode >>> 3
    # Extract lower 3 bits
    variant = opcode &&& 0x07
    {:error, {:unsupported_operation, operation_type, variant}}
  end

  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations) do
    {:error, {:invalid_opcode, opcode}}
  end

  defp stream_mutations_opcodes(_truncated_data, _mutations) do
    {:error, :truncated_mutation_data}
  end

  defp maybe_decode_read_conflicts(transaction, nil) do
    # No READ_CONFLICTS section present - this means no read conflicts exist
    {:ok, %{transaction | read_conflicts: {nil, []}}}
  end

  defp maybe_decode_read_conflicts(transaction, payload) do
    case decode_read_conflicts_payload(payload) do
      {:ok, read_conflicts} ->
        {:ok, %{transaction | read_conflicts: read_conflicts}}

      error ->
        error
    end
  end

  defp decode_read_conflicts_payload(
         <<read_version_value::signed-big-64, conflict_count::unsigned-big-32,
           conflicts_data::binary>>
       ) do
    read_version =
      if read_version_value == -1 do
        nil
      else
        Bedrock.DataPlane.Version.from_integer(read_version_value)
      end

    case decode_conflict_ranges(conflicts_data, conflict_count, []) do
      {:ok, conflicts} -> {:ok, {read_version, Enum.reverse(conflicts)}}
      error -> error
    end
  end

  defp maybe_decode_write_conflicts(transaction, nil) do
    # No WRITE_CONFLICTS section present - this means no write conflicts exist
    {:ok, %{transaction | write_conflicts: []}}
  end

  defp maybe_decode_write_conflicts(transaction, payload) do
    with {:ok, write_conflicts} <- decode_write_conflicts_payload(payload) do
      {:ok, %{transaction | write_conflicts: write_conflicts}}
    end
  end

  defp decode_write_conflicts_payload(<<conflict_count::unsigned-big-32, conflicts_data::binary>>) do
    conflicts_data
    |> decode_conflict_ranges(conflict_count, [])
    |> case do
      {:ok, conflicts} -> {:ok, Enum.reverse(conflicts)}
      error -> error
    end
  end

  defp decode_conflict_ranges(_data, 0, conflicts) do
    {:ok, conflicts}
  end

  defp decode_conflict_ranges(
         <<start_len::unsigned-big-16, start_key::binary-size(start_len),
           end_len::unsigned-big-16, end_key::binary-size(end_len), rest::binary>>,
         remaining_count,
         conflicts
       ) do
    decode_conflict_ranges(rest, remaining_count - 1, [{start_key, end_key} | conflicts])
  end

  defp decode_conflict_ranges(_, _, _) do
    {:error, :truncated_conflict_data}
  end

  # ============================================================================
  # VALIDATION IMPLEMENTATION
  # ============================================================================

  defp validate_all_sections(_data, 0), do: :ok

  defp validate_all_sections(
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32,
           payload::binary-size(payload_size), rest::binary>>,
         remaining_count
       ) do
    # Validate section CRC using standard approach
    section_content = <<tag, payload_size::unsigned-big-24, payload::binary>>
    calculated_crc = :erlang.crc32(section_content)

    if calculated_crc == stored_crc do
      validate_all_sections(rest, remaining_count - 1)
    else
      {:error, {:section_checksum_mismatch, tag}}
    end
  end

  defp validate_all_sections(_, _), do: {:error, :truncated_sections}

  # ============================================================================
  # SECTION OPERATIONS IMPLEMENTATION
  # ============================================================================

  defp parse_transaction_header(
         <<@magic_number::unsigned-big-32, @format_version, _flags,
           section_count::unsigned-big-16, sections_data::binary>>
       ) do
    {:ok, {section_count, sections_data}}
  end

  defp parse_transaction_header(_), do: {:error, :invalid_format}

  defp find_section_by_tag(_data, _target_tag, 0) do
    {:error, :section_not_found}
  end

  defp find_section_by_tag(
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32,
           payload::binary-size(payload_size), rest::binary>>,
         target_tag,
         remaining_count
       ) do
    if tag == target_tag do
      # Verify section CRC using standard approach
      section_content = <<tag, payload_size::unsigned-big-24, payload::binary>>
      calculated_crc = :erlang.crc32(section_content)

      if calculated_crc == stored_crc do
        {:ok, payload}
      else
        {:error, {:section_checksum_mismatch, tag}}
      end
    else
      find_section_by_tag(rest, target_tag, remaining_count - 1)
    end
  end

  defp find_section_by_tag(_, _, _) do
    {:error, :truncated_sections}
  end

  defp collect_section_tags(_data, 0, tags) do
    {:ok, Enum.reverse(tags)}
  end

  defp collect_section_tags(
         <<tag, payload_size::unsigned-big-24, _section_crc::unsigned-big-32,
           _payload::binary-size(payload_size), rest::binary>>,
         remaining_count,
         tags
       ) do
    collect_section_tags(rest, remaining_count - 1, [tag | tags])
  end

  defp collect_section_tags(_, _, _) do
    {:error, :truncated_sections}
  end

  defp parse_all_sections(encoded_transaction) do
    case parse_transaction_header(encoded_transaction) do
      {:ok, {section_count, sections_data}} ->
        case parse_sections(sections_data, section_count) do
          {:ok, sections_map} ->
            overall_header = encode_overall_header(map_size(sections_map))
            {:ok, {overall_header, sections_map}}

          error ->
            error
        end

      error ->
        error
    end
  end

  defp rebuild_transaction(sections_map) do
    sections =
      sections_map
      |> Enum.map(fn {tag, payload} -> encode_section(tag, payload) end)

    section_count = length(sections)
    overall_header = encode_overall_header(section_count)

    transaction = IO.iodata_to_binary([overall_header | sections])
    {:ok, transaction}
  end

  # ============================================================================
  # STREAMING IMPLEMENTATION
  # ============================================================================

  defp stream_next_mutation(<<>>), do: {:halt, <<>>}

  # SET operations (0x00 << 3)
  defp stream_next_mutation(
         <<@set_16_32, key_len::unsigned-big-16, key::binary-size(key_len),
           value_len::unsigned-big-32, value::binary-size(value_len), rest::binary>>
       ) do
    mutation = {:set, key, value}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@set_8_16, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-big-16,
           value::binary-size(value_len), rest::binary>>
       ) do
    mutation = {:set, key, value}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@set_8_8, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-8,
           value::binary-size(value_len), rest::binary>>
       ) do
    mutation = {:set, key, value}
    {[mutation], rest}
  end

  # CLEAR operations (0x01 << 3)
  defp stream_next_mutation(
         <<@clear_single_16, key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>
       ) do
    mutation = {:clear, key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_single_8, key_len::unsigned-8, key::binary-size(key_len), rest::binary>>
       ) do
    mutation = {:clear, key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_range_16, start_len::unsigned-big-16, start_key::binary-size(start_len),
           end_len::unsigned-big-16, end_key::binary-size(end_len), rest::binary>>
       ) do
    mutation = {:clear_range, start_key, end_key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_range_8, start_len::unsigned-8, start_key::binary-size(start_len),
           end_len::unsigned-8, end_key::binary-size(end_len), rest::binary>>
       ) do
    mutation = {:clear_range, start_key, end_key}
    {[mutation], rest}
  end

  # Unknown or unsupported opcodes - halt stream
  defp stream_next_mutation(<<opcode, _rest::binary>>) when opcode >= 0x10 do
    # Extract upper 5 bits
    operation_type = opcode >>> 3
    # Extract lower 3 bits
    variant = opcode &&& 0x07
    # For streaming, we halt on unknown opcodes rather than error
    {:halt, {:unknown_opcode, operation_type, variant}}
  end

  defp stream_next_mutation(_invalid_data) do
    {:halt, <<>>}
  end

  # ============================================================================
  # BINARY OPERATIONS
  # ============================================================================

  @doc """
  Combines multiple binary transactions into a single transaction.

  Merges mutations, read conflicts, and write conflicts from all transactions.
  Uses the earliest read_version if multiple transactions have read versions.
  """
  @spec combine_binary_transactions([binary()]) :: {:ok, binary()} | {:error, reason :: term()}
  def combine_binary_transactions([]), do: {:error, :empty_transaction_list}

  def combine_binary_transactions([single_transaction]), do: {:ok, single_transaction}

  def combine_binary_transactions(transactions) when is_list(transactions) do
    with {:ok, decoded_transactions} <- decode_all_transactions(transactions),
         {:ok, combined_transaction} <- merge_transactions(decoded_transactions) do
      {:ok, encode(combined_transaction)}
    end
  end

  @doc """
  Extracts the conflicts section (READ_CONFLICTS or WRITE_CONFLICTS) for resolver operations.

  Returns the raw section payload for efficient conflict resolution processing.
  """
  @spec extract_conflicts_section(binary()) ::
          {:ok, %{read_conflicts: binary() | nil, write_conflicts: binary() | nil}}
          | {:error, reason :: term()}
  def extract_conflicts_section(encoded_transaction) do
    with {:ok, read_conflicts} <-
           extract_section_or_nil(encoded_transaction, @read_conflicts_tag),
         {:ok, write_conflicts} <-
           extract_section_or_nil(encoded_transaction, @write_conflicts_tag) do
      {:ok, %{read_conflicts: read_conflicts, write_conflicts: write_conflicts}}
    end
  end

  # Helper function to extract section payload or return nil if not found
  defp extract_section_or_nil(encoded_transaction, tag) do
    case extract_section(encoded_transaction, tag) do
      {:ok, payload} -> {:ok, payload}
      {:error, :section_not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Combines conflict sections from multiple transactions for efficient resolver operations.

  Takes raw conflict section payloads and merges them without full transaction decode.
  """
  @spec combine_conflicts([binary()]) :: {:ok, binary()} | {:error, reason :: term()}
  def combine_conflicts([]), do: {:error, :empty_conflicts_list}

  def combine_conflicts([single_conflict]), do: {:ok, single_conflict}

  def combine_conflicts(conflict_payloads) when is_list(conflict_payloads) do
    with {:ok, decoded_conflicts} <- decode_all_conflicts(conflict_payloads),
         {:ok, merged_conflicts} <- merge_conflict_ranges(decoded_conflicts) do
      {:ok, encode_write_conflicts_payload(merged_conflicts)}
    end
  end

  # ============================================================================
  # BINARY OPERATIONS IMPLEMENTATION
  # ============================================================================

  defp decode_all_transactions(transactions) do
    transactions
    |> Enum.reduce_while({:ok, []}, fn transaction, {:ok, acc} ->
      case decode(transaction) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, decoded_list} -> {:ok, Enum.reverse(decoded_list)}
      error -> error
    end
  end

  defp merge_transactions(transactions) do
    initial = %{
      mutations: [],
      read_conflicts: [],
      write_conflicts: [],
      read_version: nil
    }

    merged =
      Enum.reduce(transactions, initial, fn transaction, acc ->
        %{
          mutations: acc.mutations ++ transaction.mutations,
          read_conflicts: merge_and_dedupe_ranges(acc.read_conflicts, transaction.read_conflicts),
          write_conflicts:
            merge_and_dedupe_ranges(acc.write_conflicts, transaction.write_conflicts),
          read_version: earliest_read_version(acc.read_version, transaction.read_version)
        }
      end)

    {:ok, merged}
  end

  defp decode_all_conflicts(conflict_payloads) do
    conflict_payloads
    |> Enum.reduce_while({:ok, []}, fn payload, {:ok, acc} ->
      case decode_write_conflicts_payload(payload) do
        {:ok, conflicts} -> {:cont, {:ok, [conflicts | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, conflicts_list} -> {:ok, Enum.reverse(conflicts_list)}
      error -> error
    end
  end

  defp merge_conflict_ranges(conflicts_lists) do
    all_conflicts = Enum.flat_map(conflicts_lists, & &1)
    deduplicated = Enum.uniq(all_conflicts)
    {:ok, deduplicated}
  end

  defp merge_and_dedupe_ranges(ranges1, ranges2) do
    (ranges1 ++ ranges2)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp earliest_read_version(nil, version2), do: version2
  defp earliest_read_version(version1, nil), do: version1
  defp earliest_read_version(version1, version2), do: min(version1, version2)
end
