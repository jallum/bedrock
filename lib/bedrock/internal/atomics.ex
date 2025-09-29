defmodule Bedrock.Internal.Atomics do
  @moduledoc """
  Centralized atomic operations for variable-length little-endian binary values.

  This module implements atomic operations that mirror FoundationDB's semantics,
  operating on variable-length binary values interpreted as little-endian integers.
  All operations handle missing values (empty binaries) according to their specific
  semantics.

  ## Value Interpretation
  - Empty binary (`<<>>`) represents missing/unset values
  - All other binaries are interpreted as little-endian unsigned integers
  - Operations automatically handle size differences through padding/truncation

  ## Operation Semantics
  - **Addition**: Little-endian carry propagation, missing = 0
  - **Bitwise operations**: Byte-by-byte operations with appropriate defaults
  - **Min/Max**: Little-endian integer comparison with V2 semantics
  - **Byte min/max**: Lexicographic comparison
  - **Append**: Concatenation with size limit
  - **Compare and clear**: Clear if values match
  """

  import Bitwise, only: [&&&: 2, |||: 2, bxor: 2, >>>: 2]
  import Kernel, except: [min: 2, max: 2]

  @doc "Addition with little-endian carry propagation"
  def add(<<>>, op), do: op
  def add(_, <<>>), do: <<>>

  def add(ex, op) do
    # Pad existing value to match operand length when existing is shorter
    ex_padded = pad_or_truncate(ex, byte_size(op))
    ex_padded |> add(op, 0, []) |> Enum.reverse() |> :binary.list_to_bin()
  end

  defp add(<<e::8, ex::binary>>, <<o::8, op::binary>>, c, acc) do
    s = e + o + c
    add(ex, op, s >>> 8, [s &&& 0xFF | acc])
  end

  defp add(<<>>, <<o::8, op::binary>>, c, acc) do
    s = o + c
    add(<<>>, op, s >>> 8, [s &&& 0xFF | acc])
  end

  defp add(<<>>, <<>>, c, acc) when c > 0 do
    # If there's still a carry, add it as a new byte
    [c | acc]
  end

  defp add(_, _, _, acc), do: acc

  @doc "Bitwise AND - V2 version (missing existing = return operand)"
  # V2 behavior: return operand if existing missing
  def bit_and(<<>>, op), do: op
  def bit_and(_, <<>>), do: <<>>
  def bit_and(ex, op), do: ex |> bit_and(op, []) |> Enum.reverse() |> :binary.list_to_bin()

  defp bit_and(<<e::8, ex::binary>>, <<o::8, op::binary>>, acc) do
    bit_and(ex, op, [e &&& o | acc])
  end

  defp bit_and(<<>>, <<_::8, op::binary>>, acc) do
    # Pad missing bytes with 0
    bit_and(<<>>, op, [0 | acc])
  end

  defp bit_and(_, _, acc), do: acc

  @doc "Bitwise OR operation"
  # Missing = 0, so 0 | anything = anything
  def bit_or(<<>>, op), do: op
  def bit_or(_, <<>>), do: <<>>
  def bit_or(ex, op), do: ex |> bit_or(op, []) |> Enum.reverse() |> :binary.list_to_bin()

  defp bit_or(<<e::8, ex::binary>>, <<o::8, op::binary>>, acc) do
    bit_or(ex, op, [e ||| o | acc])
  end

  defp bit_or(<<>>, <<o::8, op::binary>>, acc) do
    # 0 | o = o
    bit_or(<<>>, op, [o | acc])
  end

  defp bit_or(_, _, acc), do: acc

  @doc "Bitwise XOR operation"
  # Missing = 0, so 0 ⊕ anything = anything
  def bit_xor(<<>>, op), do: op
  def bit_xor(_, <<>>), do: <<>>
  def bit_xor(ex, op), do: ex |> bit_xor(op, []) |> Enum.reverse() |> :binary.list_to_bin()

  defp bit_xor(<<e::8, ex::binary>>, <<o::8, op::binary>>, acc) do
    bit_xor(ex, op, [bxor(e, o) | acc])
  end

  defp bit_xor(<<>>, <<o::8, op::binary>>, acc) do
    # 0 ⊕ o = o
    bit_xor(<<>>, op, [o | acc])
  end

  defp bit_xor(_, _, acc), do: acc

  @doc "Maximum operation (little-endian integer comparison)"
  def max(<<>>, op), do: op
  def max(_, <<>>), do: <<>>

  def max(ex, op) do
    # For max, if existing is shorter, pad to operand length before comparison
    ex_padded = pad_or_truncate(ex, byte_size(op))

    case compare_le_integers(ex_padded, op) do
      :greater -> ex_padded
      _ -> op
    end
  end

  @doc "Minimum operation - V2 version (missing existing = return operand)"
  # V2 behavior: return operand if existing missing
  def min(<<>>, op), do: op
  def min(_, <<>>), do: <<>>

  def min(ex, op) do
    # For min, if existing is missing, use zeros padded to operand length
    ex_padded = pad_or_truncate(ex, byte_size(op))

    case compare_le_integers(ex_padded, op) do
      :less -> ex_padded
      _ -> op
    end
  end

  @doc "Byte-wise maximum (lexicographic comparison)"
  def byte_max(<<>>, op), do: op
  def byte_max(ex, op) when ex > op, do: pad_or_truncate(ex, byte_size(op))
  def byte_max(_, op), do: op

  @doc "Byte-wise minimum (lexicographic comparison)"
  def byte_min(<<>>, op), do: op
  def byte_min(ex, op) when ex < op, do: pad_or_truncate(ex, byte_size(op))
  def byte_min(_, op), do: op

  @doc "Append if fits within size limit"
  def append_if_fits(<<>>, op), do: op
  def append_if_fits(ex, <<>>), do: ex

  def append_if_fits(ex, op) do
    # FDB VALUE_SIZE_LIMIT
    if byte_size(ex) + byte_size(op) <= 131_072 do
      ex <> op
    else
      # Return existing if would exceed limit
      ex
    end
  end

  @doc "Compare and clear - clear if values match"
  # Both empty, clear
  def compare_and_clear(<<>>, <<>>), do: <<>>
  # Match, clear
  def compare_and_clear(ex, op) when ex == op, do: <<>>
  # No match, keep existing
  def compare_and_clear(ex, _op), do: ex

  @doc """
  Apply an atomic operation to an existing value with an operand.

  Returns the result of the operation, or nil for compare_and_clear when cleared.

  ## Examples

      iex> Atomics.apply_operation(:add, <<5>>, <<3>>)
      <<8>>

      iex> Atomics.apply_operation(:min, <<10>>, <<5>>)
      <<5>>

      iex> Atomics.apply_operation(:compare_and_clear, <<5>>, <<5>>)
      nil
  """
  @spec apply_operation(atom(), binary(), binary()) :: binary() | nil
  def apply_operation(:add, existing_value, operand), do: add(existing_value, operand)
  def apply_operation(:min, existing_value, operand), do: min(existing_value, operand)
  def apply_operation(:max, existing_value, operand), do: max(existing_value, operand)
  def apply_operation(:bit_and, existing_value, operand), do: bit_and(existing_value, operand)
  def apply_operation(:bit_or, existing_value, operand), do: bit_or(existing_value, operand)
  def apply_operation(:bit_xor, existing_value, operand), do: bit_xor(existing_value, operand)
  def apply_operation(:byte_min, existing_value, operand), do: byte_min(existing_value, operand)
  def apply_operation(:byte_max, existing_value, operand), do: byte_max(existing_value, operand)
  def apply_operation(:append_if_fits, existing_value, operand), do: append_if_fits(existing_value, operand)

  def apply_operation(:compare_and_clear, existing_value, expected) do
    case compare_and_clear(existing_value, expected) do
      # Return nil when cleared
      <<>> -> nil
      result -> result
    end
  end

  # Helper functions
  defp pad_or_truncate(value, target_size) do
    current_size = byte_size(value)

    cond do
      current_size == target_size -> value
      current_size < target_size -> value <> :binary.copy(<<0>>, target_size - current_size)
      current_size > target_size -> binary_part(value, 0, target_size)
    end
  end

  defp compare_le_integers(a, b) do
    # Pad to same size for comparison
    max_size = Kernel.max(byte_size(a), byte_size(b))
    a_padded = pad_or_truncate(a, max_size)
    b_padded = pad_or_truncate(b, max_size)

    # Compare as little-endian integers (compare from high bytes down)
    compare_bytes_le(a_padded, b_padded, max_size - 1)
  end

  defp compare_bytes_le(_a, _b, -1), do: :equal

  defp compare_bytes_le(a, b, pos) do
    a_byte = :binary.at(a, pos)
    b_byte = :binary.at(b, pos)

    cond do
      a_byte > b_byte -> :greater
      a_byte < b_byte -> :less
      true -> compare_bytes_le(a, b, pos - 1)
    end
  end
end
