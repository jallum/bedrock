defmodule Bedrock.DataPlane.Transaction do
  @moduledoc """
  Tagged binary transaction encoding for Bedrock that supports efficient operations
  and extensibility through self-describing sections with embedded CRC validation.

  This module implements a comprehensive tagged binary format that encodes the
  simple map structure from `Tx.commit/1` with an efficient binary format featuring:

  - **Tagged sections**: Self-describing sections with type, size, and embedded CRC
  - **Order independence**: Sections can appear in any order for better extensibility
  - **Section omission**: All sections are optional - empty sections are completely omitted to save space
  - **Efficient operations**: Extract specific sections without full decode
  - **Robust validation**: Self-validating CRCs detect any bit corruption
  - **Simple opcode format**: 5-bit operations + 3-bit variants with size optimization

  ## Transaction Structure

  Input/output transaction map:
  ```elixir
  %{
    commit_version: Bedrock.version()               # assigned by commit proxy
    mutations: [Tx.mutation()],                     # {:set, binary(), binary()} | {:clear_range, binary(), binary()}
    read_conflicts: {read_version, [key_range()]},
    write_conflicts: [key_range()}],
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

  - 0x01: MUTATIONS - Nearly always present (may be omitted if empty)
  - 0x02: READ_CONFLICTS - Present when read conflicts exist AND read version available
  - 0x03: WRITE_CONFLICTS - Present when write conflicts exist
  - 0x04: COMMIT_VERSION - Present when stamped by commit proxy

  ## Section Usage by Component

  **Logs:** mutations, commit_version
  **Resolver:** write_conflicts, read_conflicts (if any), commit_version
  **Commit Proxy:** mutations, read_conflicts (if reads performed), write_conflicts

  ## Opcode Format

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

  import Bitwise, only: [>>>: 2, &&&: 2, <<<: 2, |||: 2]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @type transaction_map :: Bedrock.transaction()

  @type encoded :: binary()

  @type section_tag :: 0x01..0xFF
  @type opcode :: 0x00..0xFF

  @magic_number 0x42524454
  @format_version 0x01

  @mutations_tag 0x01
  @read_conflicts_tag 0x02
  @write_conflicts_tag 0x03
  @commit_version_tag 0x04

  @set_16_32 0x00
  @set_8_16 0x01
  @set_8_8 0x02

  @clear_single_16 0x08
  @clear_single_8 0x09
  @clear_range_16 0x0A
  @clear_range_8 0x0B

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

  defp add_mutations_section(sections, %{mutations: mutations}) when is_list(mutations) do
    payload = encode_mutations_payload(mutations)
    section = encode_section(@mutations_tag, payload)
    [section | sections]
  end

  defp add_mutations_section(sections, _), do: sections

  defp add_read_conflicts_section(sections, %{read_conflicts: {read_version, read_conflicts}}) do
    case {read_version, read_conflicts} do
      {nil, []} ->
        sections

      {nil, [_ | _]} ->
        raise ArgumentError, "read_version is nil but read_conflicts is non-empty"

      {_non_nil_version, []} ->
        raise ArgumentError, "read_version is non-nil but read_conflicts is empty"

      {_non_nil_version, [_ | _]} ->
        payload = encode_read_conflicts_payload(read_conflicts, read_version)
        section = encode_section(@read_conflicts_tag, payload)
        [section | sections]
    end
  end

  defp add_read_conflicts_section(sections, _), do: sections

  defp add_write_conflicts_section(sections, %{write_conflicts: []}), do: sections

  defp add_write_conflicts_section(sections, %{write_conflicts: write_conflicts}) when is_list(write_conflicts) do
    payload = encode_write_conflicts_payload(write_conflicts)
    section = encode_section(@write_conflicts_tag, payload)
    [section | sections]
  end

  defp add_write_conflicts_section(sections, _), do: sections

  defp add_commit_version_section(sections, %{commit_version: <<commit_version::binary-size(8)>>}) do
    section = encode_section(@commit_version_tag, commit_version)
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
      {:error, reason} -> raise "Failed to decode Transaction: #{inspect(reason)}"
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
  @spec stream_mutations(binary()) :: {:ok, Enumerable.t(Tx.mutation())} | {:error, reason :: term()}
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
  Streams mutations from the transaction, raising if the transaction is invalid.

  Returns a stream of mutations. Use this when you're confident the transaction
  is valid or want to fail fast on invalid data.
  """
  @spec stream_mutations!(binary()) :: Enumerable.t(Tx.mutation())
  def stream_mutations!(encoded_transaction) do
    case stream_mutations(encoded_transaction) do
      {:ok, stream} -> stream
      {:error, reason} -> raise "Failed to stream mutations: #{inspect(reason)}"
    end
  end

  @doc """
  Adds a commit version to an existing transaction.
  """
  @spec add_commit_version(binary(), binary()) :: {:ok, binary()} | {:error, reason :: term()}
  def add_commit_version(encoded_transaction, commit_version)
      when is_binary(commit_version) and byte_size(commit_version) == 8 do
    add_section(encoded_transaction, @commit_version_tag, commit_version)
  end

  @doc """
  Extracts the commit version if present.

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

  @doc """
  Extracts the commit version from the COMMIT_VERSION section, raising if not present.

  Raises if the commit version is not present or if there's an error.
  Use this when you require a commit version to be present.
  """
  @spec extract_commit_version!(binary()) :: binary()
  def extract_commit_version!(encoded_transaction) do
    case extract_commit_version(encoded_transaction) do
      {:ok, version} when is_binary(version) -> version
      {:ok, nil} -> raise "Transaction missing required commit version"
      {:error, reason} -> raise "Failed to extract commit version: #{inspect(reason)}"
    end
  end

  # ============================================================================
  # DYNAMIC OPCODE CONSTRUCTION
  # ============================================================================

  @spec build_opcode(operation :: 0..31, variant :: 0..7) :: opcode()
  defp build_opcode(operation, variant) when operation <= 31 and variant <= 7 do
    operation <<< 3 ||| variant
  end

  @spec extract_variant(opcode()) :: 0..7
  defp extract_variant(opcode) when opcode <= 255 do
    opcode &&& 0x07
  end

  @spec optimize_set_opcode(key_size :: non_neg_integer(), value_size :: non_neg_integer()) ::
          opcode()
  defp optimize_set_opcode(key_size, value_size) do
    cond do
      key_size <= 255 and value_size <= 255 ->
        build_opcode(@set_operation, 2)

      key_size <= 255 and value_size <= 65_535 ->
        build_opcode(@set_operation, 1)

      true ->
        build_opcode(@set_operation, 0)
    end
  end

  @spec optimize_clear_opcode(key_size :: non_neg_integer(), is_range :: boolean()) :: opcode()
  defp optimize_clear_opcode(key_size, false = _is_range) when key_size <= 255 do
    build_opcode(@clear_operation, 1)
  end

  defp optimize_clear_opcode(_key_size, false = _is_range) do
    build_opcode(@clear_operation, 0)
  end

  defp optimize_clear_opcode(max_key_size, true = _is_range) when max_key_size <= 255 do
    build_opcode(@clear_operation, 3)
  end

  defp optimize_clear_opcode(_max_key_size, true = _is_range) do
    build_opcode(@clear_operation, 2)
  end

  # ============================================================================
  # ENCODING IMPLEMENTATION
  # ============================================================================

  defp encode_overall_header(section_count) do
    <<
      @magic_number::unsigned-big-32,
      @format_version,
      0x00,
      section_count::unsigned-big-16
    >>
  end

  defp encode_section(tag, payload) do
    payload_size = byte_size(payload)
    section_content = <<tag, payload_size::unsigned-big-24, payload::binary>>
    section_crc = :erlang.crc32(section_content)

    <<
      tag,
      payload_size::unsigned-big-24,
      section_crc::unsigned-big-32,
      payload::binary
    >>
  end

  defp encode_mutations_payload(mutations) do
    mutations_data = Enum.map(mutations, &encode_mutation_opcode/1)
    IO.iodata_to_binary(mutations_data)
  end

  defp encode_mutation_opcode({:set, key, value}) do
    key_len = byte_size(key)
    value_len = byte_size(value)
    opcode = optimize_set_opcode(key_len, value_len)

    case extract_variant(opcode) do
      2 ->
        <<opcode, key_len::unsigned-8, key::binary, value_len::unsigned-8, value::binary>>

      1 ->
        <<opcode, key_len::unsigned-8, key::binary, value_len::unsigned-big-16, value::binary>>

      0 ->
        <<opcode, key_len::unsigned-big-16, key::binary, value_len::unsigned-big-32, value::binary>>
    end
  end

  defp encode_mutation_opcode({:clear, key}) do
    key_len = byte_size(key)
    opcode = optimize_clear_opcode(key_len, false)

    case extract_variant(opcode) do
      1 ->
        <<opcode, key_len::unsigned-8, key::binary>>

      0 ->
        <<opcode, key_len::unsigned-big-16, key::binary>>
    end
  end

  defp encode_mutation_opcode({:clear_range, start_key, end_key}) do
    start_len = byte_size(start_key)
    end_len = byte_size(end_key)
    max_key_len = max(start_len, end_len)
    opcode = optimize_clear_opcode(max_key_len, true)

    case extract_variant(opcode) do
      3 ->
        <<opcode, start_len::unsigned-8, start_key::binary, end_len::unsigned-8, end_key::binary>>

      2 ->
        <<opcode, start_len::unsigned-big-16, start_key::binary, end_len::unsigned-big-16, end_key::binary>>
    end
  end

  defp encode_read_conflicts_payload(read_conflicts, read_version) do
    conflict_count = length(read_conflicts)
    conflicts_data = Enum.map(read_conflicts, &encode_conflict_range/1)

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
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32, payload::binary-size(payload_size),
           rest::binary>>,
         remaining_count,
         sections_map
       ) do
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

  defp stream_mutations_opcodes(
         <<@set_16_32, key_len::unsigned-big-16, key::binary-size(key_len), value_len::unsigned-big-32,
           value::binary-size(value_len), rest::binary>>,
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
         <<@clear_range_16, start_len::unsigned-big-16, start_key::binary-size(start_len), end_len::unsigned-big-16,
           end_key::binary-size(end_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear_range, start_key, end_key}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(
         <<@clear_range_8, start_len::unsigned-8, start_key::binary-size(start_len), end_len::unsigned-8,
           end_key::binary-size(end_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear_range, start_key, end_key}
    stream_mutations_opcodes(rest, [mutation | mutations])
  end

  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations) when opcode in 0x03..0x07 do
    {:error, {:reserved_set_variant, opcode}}
  end

  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations) when opcode in 0x0C..0x0F do
    {:error, {:reserved_clear_variant, opcode}}
  end

  defp stream_mutations_opcodes(<<opcode, _rest::binary>>, _mutations) when opcode in 0x10..0xFF do
    operation_type = opcode >>> 3
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
         <<read_version_value::signed-big-64, conflict_count::unsigned-big-32, conflicts_data::binary>>
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
         <<start_len::unsigned-big-16, start_key::binary-size(start_len), end_len::unsigned-big-16,
           end_key::binary-size(end_len), rest::binary>>,
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
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32, payload::binary-size(payload_size),
           rest::binary>>,
         remaining_count
       ) do
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
         <<@magic_number::unsigned-big-32, @format_version, _flags, section_count::unsigned-big-16,
           sections_data::binary>>
       ) do
    {:ok, {section_count, sections_data}}
  end

  defp parse_transaction_header(_), do: {:error, :invalid_format}

  defp find_section_by_tag(_data, _target_tag, 0) do
    {:error, :section_not_found}
  end

  defp find_section_by_tag(
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32, payload::binary-size(payload_size),
           rest::binary>>,
         target_tag,
         remaining_count
       ) do
    if tag == target_tag do
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
      Enum.map(sections_map, fn {tag, payload} -> encode_section(tag, payload) end)

    section_count = length(sections)
    overall_header = encode_overall_header(section_count)

    transaction = IO.iodata_to_binary([overall_header | sections])
    {:ok, transaction}
  end

  # ============================================================================
  # STREAMING IMPLEMENTATION
  # ============================================================================

  defp stream_next_mutation(<<>>), do: {:halt, <<>>}

  defp stream_next_mutation(
         <<@set_16_32, key_len::unsigned-big-16, key::binary-size(key_len), value_len::unsigned-big-32,
           value::binary-size(value_len), rest::binary>>
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

  defp stream_next_mutation(<<@clear_single_16, key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>) do
    mutation = {:clear, key}
    {[mutation], rest}
  end

  defp stream_next_mutation(<<@clear_single_8, key_len::unsigned-8, key::binary-size(key_len), rest::binary>>) do
    mutation = {:clear, key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_range_16, start_len::unsigned-big-16, start_key::binary-size(start_len), end_len::unsigned-big-16,
           end_key::binary-size(end_len), rest::binary>>
       ) do
    mutation = {:clear_range, start_key, end_key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_range_8, start_len::unsigned-8, start_key::binary-size(start_len), end_len::unsigned-8,
           end_key::binary-size(end_len), rest::binary>>
       ) do
    mutation = {:clear_range, start_key, end_key}
    {[mutation], rest}
  end

  defp stream_next_mutation(<<opcode, _rest::binary>>) when opcode >= 0x10 do
    operation_type = opcode >>> 3
    variant = opcode &&& 0x07
    {:halt, {:unknown_opcode, operation_type, variant}}
  end

  defp stream_next_mutation(_invalid_data) do
    {:halt, <<>>}
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
end
