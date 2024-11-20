defmodule Bedrock.DataPlane.Log.Transaction do
  @type t :: {Bedrock.version(), %{Bedrock.key() => Bedrock.value()}}

  @doc """
  Create a new transaction.
  """
  @spec new(Bedrock.version(), keyword()) :: t()
  def new(version, [{_key, _value} | _] = key_values), do: {version, Map.new(key_values)}
  def new(version, []), do: {version, %{}}

  @spec new(Bedrock.version(), map()) :: t()
  def new(version, key_values) when is_map(key_values), do: {version, key_values}

  @doc """
  Get the version from the transaction.
  """
  @spec version(t()) :: Bedrock.version()
  def version({version, _}) when is_integer(version), do: version

  @doc """
  Get the key-values from the transaction. If they have been previously encoded,
  they will be decoded before being returned.
  """
  @spec key_values(t()) :: %{Bedrock.key() => Bedrock.value() | nil, Bedrock.key_range() => nil}
  def key_values({_, %{} = key_values}), do: key_values
end
