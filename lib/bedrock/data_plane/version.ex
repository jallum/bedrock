defmodule Bedrock.DataPlane.Version do
  @moduledoc """
  Version utilities for Bedrock's MVCC system.
  Versions are 8-byte big-endian unsigned integers.
  """

  @type t :: binary()

  # Version constants
  @zero_version <<0::unsigned-big-64>>

  @doc "Creates a zero version (initial version)"
  @spec zero() :: t()
  def zero, do: @zero_version

  @doc "Checks if version is the first version (zero)"
  @spec first?(t()) :: boolean()
  def first?(version), do: version == @zero_version

  @doc "Creates a version from a non-negative integer"
  @spec from_integer(non_neg_integer()) :: t()
  def from_integer(int) when is_integer(int) and int >= 0 and int <= 0xFFFFFFFFFFFFFFFF,
    do: <<int::unsigned-big-64>>

  @doc "Creates a version from a binary representation"
  @spec from_bytes(binary()) :: t()
  def from_bytes(version) when byte_size(version) == 8, do: version

  @doc "Converts a version to integer representation"
  @spec to_integer(t()) :: non_neg_integer()
  def to_integer(<<version::unsigned-big-64>>), do: version

  @doc "Converts a version to string representation for display"
  @spec to_string(t() | nil) :: String.t()
  def to_string(nil), do: "nil"

  def to_string(version) do
    "<#{version |> :erlang.binary_to_list() |> Enum.map_join(",", &Integer.to_string/1)}>"
  end

  @doc "Increments a version by 1"
  @spec increment(t()) :: t()
  def increment(<<version::unsigned-big-64>>) when version < 0xFFFFFFFFFFFFFFFF do
    <<version + 1::unsigned-big-64>>
  end

  @doc "Checks if first version is newer than second version"
  @spec newer?(t(), t()) :: boolean()
  def newer?(version, last_version), do: version > last_version

  @doc "Checks if first version is older than second version"
  @spec older?(t(), t()) :: boolean()
  def older?(version, last_version), do: version < last_version

  @doc "Compares two versions"
  @spec compare(t(), t()) :: :lt | :eq | :gt
  def compare(v1, v2) when v1 < v2, do: :lt
  def compare(v1, v2) when v1 > v2, do: :gt
  def compare(v1, v2) when v1 == v2, do: :eq

  @doc "Validates if value is a valid version binary"
  @spec valid?(any()) :: boolean()
  def valid?(<<_::unsigned-big-64>>), do: true
  def valid?(_), do: false

  @doc "Adds an integer offset to a version"
  @spec add(t(), integer()) :: t()
  def add(<<version::unsigned-big-64>>, offset) when is_integer(offset) do
    new_version = version + offset

    if new_version >= 0 and new_version <= 0xFFFFFFFFFFFFFFFF do
      <<new_version::unsigned-big-64>>
    else
      raise ArgumentError, "version arithmetic overflow: #{version} + #{offset} = #{new_version}"
    end
  end

  @doc "Subtracts an integer offset from a version"
  @spec subtract(t(), integer()) :: t()
  def subtract(version, offset), do: add(version, -offset)

  @doc "Calculates the integer difference between two versions (v1 - v2)"
  @spec distance(t(), t()) :: integer()
  def distance(<<v1::unsigned-big-64>>, <<v2::unsigned-big-64>>), do: v1 - v2

  @doc "Generates a sequence of versions starting from base + 1, base + 2, ..., base + count"
  @spec sequence(t(), pos_integer()) :: [t()]
  def sequence(base_version, count) when is_integer(count) and count > 0 do
    <<base::unsigned-big-64>> = base_version
    max_value = base + count

    if max_value <= 0xFFFFFFFFFFFFFFFF do
      for i <- 1..count, do: <<base + i::unsigned-big-64>>
    else
      raise ArgumentError,
            "version sequence overflow: base #{base} + count #{count} exceeds maximum"
    end
  end
end
