defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Tx do
  @moduledoc """
  Opaque transaction type for building and committing database operations.

  This module provides an immutable transaction structure that accumulates
  reads, writes, and range operations. Transactions can be committed to
  produce the final mutation list and conflict ranges for resolution.
  """

  alias Bedrock.DataPlane.BedrockTransaction

  @type key :: binary()
  @type value :: binary()
  @type range :: {start :: binary(), end_ex :: binary()}

  @type mutation ::
          {:set, key(), value()}
          | {:clear, key()}
          | {:clear_range, start :: binary(), end_ex :: binary()}

  @type t :: %__MODULE__{
          mutations: [mutation()],
          writes: %{key() => value() | :clear},
          reads: %{key() => value() | :clear},
          range_writes: [range()],
          range_reads: [range()]
        }
  defstruct mutations: [],
            writes: %{},
            reads: %{},
            range_writes: [],
            range_reads: []

  def new, do: %__MODULE__{}

  @spec get(
          t(),
          key(),
          fetch_fn :: (key(), state -> {{:ok, value()} | {:error, reason}, state}),
          state :: any()
        ) :: {t(), {:ok, value()} | {:error, reason}, state}
        when reason: term(), state: term()
  def get(t, k, fetch_fn, state) when is_binary(k) do
    case get_write(t, k) || get_read(t, k) do
      nil -> fetch(t, k, fetch_fn, state)
      :clear -> {t, {:error, :not_found}, state}
      value -> {t, {:ok, value}, state}
    end
  end

  @spec get_range(
          t(),
          start :: key(),
          end_ex :: key(),
          (start :: key(), end_ex :: key(), opts :: keyword(), state -> {[range()], state}),
          state,
          opts :: keyword()
        ) :: {t(), [{key(), value()}], state}
        when state: term()
  def get_range(t, s, e, read_range_fn, state, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1000)

    tx_visible =
      t.writes
      |> Enum.filter(fn {k, v} ->
        k >= s and k < e and v != :clear
      end)
      |> Map.new()

    cleared_ranges =
      t.mutations
      |> Enum.filter(fn
        {:clear_range, _, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {:clear_range, cs, ce} -> {cs, ce} end)

    {db_results, new_state} =
      fetch_missing_data_if_needed(
        tx_visible,
        limit,
        t,
        cleared_ranges,
        read_range_fn,
        state,
        s,
        e
      )

    merged = Map.merge(db_results, tx_visible)

    result =
      merged
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.take(limit)

    new_t = %{
      t
      | range_reads: add_or_merge(t.range_reads, s, e),
        reads: Map.merge(t.reads, Map.new(result))
    }

    {new_t, result, new_state}
  end

  def set(t, k, v) when is_binary(k) and is_binary(v) do
    t
    |> remove_ops_in_range(k, next_key(k))
    |> put_write(k, v)
    |> record_mutation({:set, k, v})
  end

  def clear(t, k) when is_binary(k) do
    t
    |> remove_ops_in_range(k, next_key(k))
    |> put_clear(k)
    |> record_mutation({:clear, k})
  end

  def clear_range(t, s, e) when is_binary(s) and is_binary(e) do
    t
    |> remove_ops_in_range(s, e)
    |> remove_writes_in_range(s, e)
    |> clear_reads_in_range(s, e)
    |> add_write_range(s, e)
    |> record_mutation({:clear_range, s, e})
  end

  @doc """
  Commits the transaction and returns the transaction map format.

  This is useful for testing and cases where the raw transaction structure
  is needed without binary encoding.
  """
  def commit(t, read_version \\ nil) do
    write_conflicts =
      t.writes
      |> Map.keys()
      |> Enum.reduce(t.range_writes, fn k, acc -> add_or_merge(acc, k, next_key(k)) end)

    read_conflicts =
      t.reads
      |> Map.keys()
      |> Enum.reduce(t.range_reads, fn k, acc -> add_or_merge(acc, k, next_key(k)) end)

    # Enforce read_version/read_conflicts coupling: if no reads, ignore read_version
    read_conflicts_tuple =
      case read_conflicts do
        [] -> {nil, []}
        non_empty when read_version != nil -> {read_version, non_empty}
        _non_empty when read_version == nil -> {nil, []}
      end

    %{
      mutations: t.mutations |> Enum.reverse(),
      write_conflicts: write_conflicts,
      read_conflicts: read_conflicts_tuple
    }
  end

  @doc """
  Commits the transaction and returns binary encoded format.

  This is useful for commit proxy operations and other cases where
  efficient binary format is preferred.
  """
  def commit_binary(t, read_version \\ nil),
    do: t |> commit(read_version) |> BedrockTransaction.encode()

  defp remove_ops_in_range(t, s, e) do
    %{
      t
      | mutations:
          Enum.reject(t.mutations, fn
            {:set, k, _} -> k >= s && k < e
            {:clear, k} -> k >= s && k < e
            _ -> false
          end)
    }
  end

  defp remove_writes_in_range(t, s, e) do
    %{
      t
      | writes:
          t.writes
          |> Enum.reject(fn {k, _} -> k >= s && k < e end)
          |> Map.new()
    }
  end

  defp clear_reads_in_range(t, s, e) do
    %{
      t
      | reads:
          t.reads
          |> Map.new(fn
            {k, _} when k >= s and k < e -> {k, :clear}
            kv -> kv
          end)
    }
  end

  defp add_write_range(t, s, e) do
    %{
      t
      | range_writes: t.range_writes |> add_or_merge(s, e)
    }
  end

  def add_or_merge([], s, e), do: [{s, e}]
  def add_or_merge([{hs, he} | rest], s, e) when e < hs, do: [{s, e}, {hs, he} | rest]
  def add_or_merge([{hs, he} | rest], s, e) when he < s, do: [{hs, he} | add_or_merge(rest, s, e)]
  def add_or_merge([{hs, he} | rest], s, e), do: add_or_merge(rest, min(hs, s), max(he, e))

  @spec get_write(t(), k :: binary()) :: binary() | :clear | nil
  defp get_write(t, k), do: Map.get(t.writes, k)

  @spec get_read(t(), k :: binary()) :: binary() | :clear | nil
  defp get_read(t, k), do: Map.get(t.reads, k)

  @spec put_clear(t(), k :: binary()) :: t()
  defp put_clear(t, k), do: %{t | writes: Map.put(t.writes, k, :clear)}

  @spec put_write(t(), k :: binary(), v :: binary()) :: t()
  defp put_write(t, k, v), do: %{t | writes: Map.put(t.writes, k, v)}

  defp fetch(t, k, fetch_fn, state) do
    {result, new_state} = fetch_fn.(k, state)

    case result do
      {:ok, v} ->
        {%{t | reads: Map.put(t.reads, k, v)}, result, new_state}

      {:error, :not_found} ->
        {%{t | reads: Map.put(t.reads, k, :clear)}, result, new_state}

      result ->
        {t, result, new_state}
    end
  end

  @spec record_mutation(t(), mutation()) :: t()
  defp record_mutation(t, op) do
    %{t | mutations: [op | t.mutations]}
  end

  defp next_key(k), do: k <> <<0>>

  # Helper function to reduce nesting in get_range
  defp fetch_missing_data_if_needed(
         tx_visible,
         limit,
         t,
         cleared_ranges,
         read_range_fn,
         state,
         s,
         e
       )
       when map_size(tx_visible) < limit do
    fetch_and_filter_range_data(t, cleared_ranges, read_range_fn, state, s, e, limit)
  end

  defp fetch_missing_data_if_needed(
         _tx_visible,
         _limit,
         _t,
         _cleared_ranges,
         _read_range_fn,
         state,
         _s,
         _e
       ) do
    {%{}, state}
  end

  defp fetch_and_filter_range_data(t, cleared_ranges, read_range_fn, state, s, e, limit) do
    {range_data, updated_state} = read_range_fn.(state, s, e, limit: limit)

    filtered_data =
      range_data
      |> Enum.reject(&should_skip_key?(&1, t.writes, cleared_ranges))
      |> Map.new()

    {filtered_data, updated_state}
  end

  defp should_skip_key?({k, _v}, writes, cleared_ranges) do
    # Skip if we already have it in writes or if it's in a cleared range
    Map.has_key?(writes, k) or
      Enum.any?(cleared_ranges, fn {cs, ce} -> k >= cs and k < ce end)
  end
end
