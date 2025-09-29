defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Tx do
  @moduledoc """
  Opaque transaction type for building and committing database operations.

  This module provides an immutable transaction structure that accumulates
  reads, writes, and range operations. Transactions can be committed to
  produce the final mutation list and conflict ranges for resolution.
  """

  import Bedrock.DataPlane.Version, only: [is_version: 1]

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Atomics
  alias Bedrock.Key
  alias Bedrock.KeyRange

  @type key :: binary()
  @type value :: binary()
  @type range :: {start :: binary(), end_ex :: binary()}

  @type mutation ::
          {:set, key(), value()}
          | {:clear, key()}
          | {:clear_range, start :: binary(), end_ex :: binary()}
          | {:atomic, atom(), key(), binary()}

  @type fetch_fn :: (key(), term() -> {{:ok, value()} | {:error, term()}, term()})

  @type t :: %__MODULE__{
          mutations: [mutation()],
          writes:
            :gb_trees.tree(
              key(),
              value()
              | :clear
              | {:add, binary()}
              | {:min, binary()}
              | {:max, binary()}
              | {:bit_and, binary()}
              | {:bit_or, binary()}
              | {:bit_xor, binary()}
              | {:byte_min, binary()}
              | {:byte_max, binary()}
              | {:append_if_fits, binary()}
              | {:compare_and_clear, binary()}
            ),
          reads: %{key() => value() | :clear},
          range_writes: [range()],
          range_reads: [range()]
        }
  defstruct mutations: [],
            writes: :gb_trees.empty(),
            reads: %{},
            range_writes: [],
            range_reads: []

  # =============================================================================
  # Constructor Functions
  # =============================================================================

  def new, do: %__MODULE__{}

  # =============================================================================
  # Primary Transaction Operations
  # =============================================================================

  @spec get(t(), key(), fetch_fn(), state) :: {t(), {:ok, value()} | {:error, :not_found}, state}
        when state: term()
  def get(t, k, fetch_fn, state) when is_binary(k) do
    case get_write(t, k) || get_read(t, k) do
      nil -> fetch_and_record(t, k, fetch_fn, state)
      :clear -> {t, {:error, :not_found}, state}
      value -> {t, {:ok, value}, state}
    end
  end

  def set(t, k, v, opts \\ []) when is_binary(k) and is_binary(v) do
    no_write_conflict = Keyword.get(opts, :no_write_conflict, false)

    t
    |> remove_ops_in_range(k, Key.key_after(k))
    |> put_write(k, v)
    |> add_write_conflict_key_unless(k, no_write_conflict)
    |> record_mutation({:set, k, v})
  end

  def clear(t, k, opts \\ []) when is_binary(k) do
    no_write_conflict = Keyword.get(opts, :no_write_conflict, false)

    t
    |> remove_ops_in_range(k, Key.key_after(k))
    |> put_clear(k)
    |> add_write_conflict_key_unless(k, no_write_conflict)
    |> record_mutation({:clear, k})
  end

  def clear_range(t, s, e, opts \\ [])

  # Empty range - just record the mutation without doing anything else
  def clear_range(t, s, e, _opts) when s >= e, do: t

  def clear_range(t, s, e, opts) when is_binary(s) and is_binary(e) do
    no_write_conflict = Keyword.get(opts, :no_write_conflict, false)

    t
    |> remove_ops_in_range(s, e)
    |> remove_writes_in_range(s, e)
    |> clear_reads_in_range(s, e)
    |> add_write_conflict_range_unless(s, e, no_write_conflict)
    |> record_mutation({:clear_range, s, e})
  end

  # Generic atomic operation implementation
  @spec atomic_operation(t(), key(), atom(), binary()) :: t()
  def atomic_operation(t, k, operation, value) when is_binary(k) and is_binary(value) do
    t
    |> remove_ops_in_range(k, Key.key_after(k))
    |> put_atomic(k, operation, value)
    |> record_mutation({:atomic, operation, k, value})
  end

  @spec record_mutation(t(), mutation()) :: t()
  defp record_mutation(t, op), do: %{t | mutations: [op | t.mutations]}

  # =============================================================================
  # Storage Integration Functions
  # =============================================================================

  @doc """
  Merge a storage read result into the transaction state for conflict tracking.

  This function is used after KeySelector resolution to merge the resolved key
  and value into the transaction's read state, ensuring proper conflict detection.
  """
  @spec merge_storage_read(t(), key(), value() | :not_found) :: t()
  def merge_storage_read(t, key, :not_found) when is_binary(key), do: %{t | reads: Map.put(t.reads, key, :clear)}

  def merge_storage_read(t, key, value) when is_binary(key) and is_binary(value),
    do: %{t | reads: Map.put(t.reads, key, value)}

  @doc """
  Merge storage range read results into the transaction state for conflict tracking.

  This function is used after KeySelector range resolution to merge resolved keys
  and values into the transaction's read state, and add the range to range_reads.
  """
  @spec merge_storage_range_read(t(), key(), key(), [{key(), value()}]) :: t()
  def merge_storage_range_read(t, resolved_start_key, resolved_end_key, key_values)
      when is_binary(resolved_start_key) and is_binary(resolved_end_key) do
    # Add all individual key-value pairs to reads for conflict tracking
    updated_reads =
      Enum.reduce(key_values, t.reads, fn {key, value}, acc ->
        Map.put(acc, key, value)
      end)

    # Add the resolved range to range_reads for conflict tracking
    updated_range_reads = add_or_merge(t.range_reads, resolved_start_key, resolved_end_key)

    %{t | reads: updated_reads, range_reads: updated_range_reads}
  end

  @doc """
  Enhanced version of merge_storage_range_with_writes that handles pending writes
  correctly based on shard boundaries and has_more flag.

  When has_more = false, this indicates the storage server has given us all data
  in its authoritative range, so we should include pending writes beyond the
  storage results up to the boundary of the query range and shard range.
  """
  @spec merge_storage_range_with_writes(
          t(),
          [{key(), value()}],
          has_more :: boolean(),
          query_range :: {key(), key()},
          shard_range :: Bedrock.key_range()
        ) :: {t(), [{key(), value()}]}
  def merge_storage_range_with_writes(tx, storage_results, has_more, query_range, shard_range) do
    {query_start, query_end} = query_range
    {shard_start, shard_end} = shard_range

    # Calculate the effective range to scan for pending writes
    effective_start = max(query_start, shard_start)

    effective_end =
      case shard_end do
        # Unbounded shard, use query end
        :end -> query_end
        shard_end_key -> min(query_end, shard_end_key)
      end

    case {storage_results, has_more} do
      {[], false} ->
        # Empty storage and no more data - scan all pending writes in effective range
        scan_pending_writes(tx, effective_start, effective_end)

      {[], true} ->
        # Empty storage but more data available - just return empty with conflict tracking
        {add_range_conflict(tx, effective_start, effective_end), []}

      {[{_first_key, _} | _], false} ->
        # Have storage data and no more data - merge storage with writes within storage bounds,
        # then scan for additional writes beyond storage up to effective_end
        {last_key, _} = List.last(storage_results)

        # First merge storage results with overlapping writes (bounded)
        {tx_after_merge, merged_results} = merge_storage_with_bounded_writes(tx, storage_results, effective_end)

        # Then scan for additional pending writes beyond the merged range
        scan_start = Key.key_after(last_key)

        {tx_final, additional_writes} =
          if scan_start < effective_end do
            scan_pending_writes(tx_after_merge, scan_start, effective_end)
          else
            {tx_after_merge, []}
          end

        final_results = merged_results ++ additional_writes
        {tx_final, final_results}

      {storage_results, true} ->
        # Have storage data and more available - only merge overlapping writes within storage bounds
        # Cannot include ANY writes beyond the last storage key because storage might have
        # more data between last storage key and our next write
        [{_first_key, _} | _] = storage_results
        {last_key, _} = List.last(storage_results)
        # Use exact Key.key_after boundary - no writes beyond this point
        merge_storage_with_bounded_writes(tx, storage_results, Key.key_after(last_key))
    end
  end

  # =============================================================================
  # Conflict Tracking Functions
  # =============================================================================

  @doc """
  Add a read conflict range to the transaction.

  This function adds the specified range to the transaction's read conflict tracking.
  Overlapping and adjacent ranges are automatically merged for efficiency.

  ## Parameters
    - `t` - The transaction
    - `start_key` - The inclusive start key of the range
    - `end_key` - The exclusive end key of the range

  ## Examples

      iex> tx = Tx.new()
      iex> tx = Tx.add_read_conflict_range(tx, "a", "z")
      iex> tx.range_reads
      [{"a", "z"}]

      iex> tx = Tx.new()
      iex> tx = Tx.add_read_conflict_range(tx, "a", "m")
      iex> tx = Tx.add_read_conflict_range(tx, "k", "z")
      iex> tx.range_reads
      [{"a", "z"}]
  """
  @spec add_read_conflict_range(t(), key(), key()) :: t()
  def add_read_conflict_range(t, start_key, end_key) when is_binary(start_key) and is_binary(end_key) do
    %{t | range_reads: add_or_merge(t.range_reads, start_key, end_key)}
  end

  @doc """
  Add a write conflict range to the transaction.

  This function adds the specified range to the transaction's write conflict tracking.
  Overlapping and adjacent ranges are automatically merged for efficiency.

  ## Parameters
    - `t` - The transaction
    - `start_key` - The inclusive start key of the range
    - `end_key` - The exclusive end key of the range

  ## Examples

      iex> tx = Tx.new()
      iex> tx = Tx.add_write_conflict_range(tx, "a", "z")
      iex> tx.range_writes
      [{"a", "z"}]

      iex> tx = Tx.new()
      iex> tx = Tx.add_write_conflict_range(tx, "a", "m")
      iex> tx = Tx.add_write_conflict_range(tx, "k", "z")
      iex> tx.range_writes
      [{"a", "z"}]
  """
  @spec add_write_conflict_range(t(), key(), key()) :: t()
  def add_write_conflict_range(t, start_key, end_key)
      when is_binary(start_key) and is_binary(end_key) and start_key < end_key do
    %{t | range_writes: add_or_merge(t.range_writes, start_key, end_key)}
  end

  # Empty range - don't add any conflict
  def add_write_conflict_range(t, _start_key, _end_key), do: t

  @doc """
  Add a single key read conflict to the transaction.

  This is a convenience function that adds a read conflict for a single key by
  converting it to a single-key range (key to Key.key_after(key)).

  ## Parameters
    - `t` - The transaction
    - `key` - The key to add as a read conflict

  ## Examples

      iex> tx = Tx.new()
      iex> tx = Tx.add_read_conflict_key(tx, "my_key")
      iex> tx.range_reads
      [{"my_key", "my_key\\0"}]
  """
  @spec add_read_conflict_key(t(), key()) :: t()
  def add_read_conflict_key(t, key) when is_binary(key), do: add_read_conflict_range(t, key, Key.key_after(key))

  @doc """
  Add a single key write conflict to the transaction.

  This is a convenience function that adds a write conflict for a single key by
  converting it to a single-key range (key to Key.key_after(key)).

  ## Parameters
    - `t` - The transaction
    - `key` - The key to add as a write conflict

  ## Examples

      iex> tx = Tx.new()
      iex> tx = Tx.add_write_conflict_key(tx, "my_key")
      iex> tx.range_writes
      [{"my_key", "my_key\\0"}]
  """
  @spec add_write_conflict_key(t(), key()) :: t()
  def add_write_conflict_key(t, key) when is_binary(key), do: add_write_conflict_range(t, key, Key.key_after(key))

  def add_write_conflict_key_unless(t, key, false), do: add_write_conflict_key(t, key)
  def add_write_conflict_key_unless(t, _key, true), do: t

  def add_write_conflict_range_unless(t, min_key, max_key_ex, false),
    do: add_write_conflict_range(t, min_key, max_key_ex)

  def add_write_conflict_range_unless(t, _min_key, _max_key_ex, true), do: t

  # =============================================================================
  # Transaction Finalization
  # =============================================================================

  @doc """
  Get the repeatable read value for a key within the transaction.

  Checks both writes and reads, returning the value if the key has been
  accessed in this transaction, or nil if the key is unknown to the transaction.
  This ensures repeatable read semantics - the same key returns the same value
  throughout the transaction.
  """
  @spec repeatable_read(t(), key()) :: value() | :clear | nil
  def repeatable_read(t, key) do
    case :gb_trees.lookup(key, t.writes) do
      {:value, {op, operand}} ->
        base_value = get_current_binary_value(t, key)
        Atomics.apply_operation(op, base_value, operand) || <<>>

      {:value, v} ->
        v

      :none ->
        Map.get(t.reads, key)
    end
  end

  @spec commit(t(), Bedrock.version() | nil) :: Transaction.encoded()
  def commit(%__MODULE__{reads: reads, range_reads: range_reads}, nil) when reads != %{} or range_reads != [] do
    raise ArgumentError, "cannot commit transaction with read conflicts but nil read_version"
  end

  def commit(t, read_version) when is_nil(read_version) or is_version(read_version) do
    # Write conflicts are only those explicitly added during operations
    write_conflicts = t.range_writes

    read_conflicts =
      t.reads
      |> Map.keys()
      |> Enum.reduce(t.range_reads, fn k, acc -> add_or_merge(acc, k, Key.key_after(k)) end)

    # Read conflicts tuple: nil when no conflicts, {version, conflicts} when conflicts exist
    read_conflicts_tuple =
      case read_conflicts do
        [] -> nil
        non_empty -> {read_version, non_empty}
      end

    Transaction.encode(%{
      mutations: Enum.reverse(t.mutations),
      write_conflicts: write_conflicts,
      read_conflicts: read_conflicts_tuple
    })
  end

  # =============================================================================
  # Private Helper Functions - Data Access
  # =============================================================================

  @spec get_write(t(), k :: binary()) :: binary() | :clear | nil
  defp get_write(t, k) do
    case :gb_trees.lookup(k, t.writes) do
      {:value, v} -> v
      :none -> nil
    end
  end

  @spec get_read(t(), k :: binary()) :: binary() | :clear | nil
  defp get_read(t, k), do: Map.get(t.reads, k)

  @spec put_clear(t(), k :: binary()) :: t()
  defp put_clear(t, k), do: %{t | writes: :gb_trees.enter(k, :clear, t.writes)}

  @spec put_write(t(), k :: binary(), v :: binary()) :: t()
  defp put_write(t, k, v), do: %{t | writes: :gb_trees.enter(k, v, t.writes)}

  @spec put_atomic(t(), binary(), atom(), binary()) :: t()
  defp put_atomic(t, k, op, value), do: %{t | writes: :gb_trees.enter(k, {op, value}, t.writes)}

  @spec get_current_binary_value(t(), key()) :: binary()
  defp get_current_binary_value(t, key) do
    case Map.get(t.reads, key) do
      nil ->
        # Key not found - return empty binary for atomic operations
        <<>>

      :clear ->
        # Key was cleared - return empty binary for atomic operations
        <<>>

      value when is_binary(value) ->
        # Return the existing binary value
        value
    end
  end

  defp fetch_and_record(t, k, fetch_fn, state) do
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

  # =============================================================================
  # Private Helper Functions - Range Processing
  # =============================================================================

  defp remove_ops_in_range(t, s, e), do: %{t | mutations: Enum.reject(t.mutations, &key_in_range?(&1, s, e))}

  defp key_in_range?({:set, k, _}, s, e), do: k >= s && k < e
  defp key_in_range?({:clear, k}, s, e), do: k >= s && k < e
  defp key_in_range?({:atomic, _op, k, _}, s, e), do: k >= s && k < e
  defp key_in_range?(_, _s, _e), do: false

  defp remove_writes_in_range(t, s, e) do
    # Directly fold over keys in range, deleting as we go
    new_writes =
      s
      |> :gb_trees.iterator_from(t.writes)
      |> gb_trees_delete_range(e, t.writes)

    %{t | writes: new_writes}
  end

  defp gb_trees_delete_range(iterator, end_key, tree) do
    case :gb_trees.next(iterator) do
      {key, _value, next_iterator} when key < end_key ->
        gb_trees_delete_range(next_iterator, end_key, :gb_trees.delete_any(key, tree))

      _ ->
        tree
    end
  end

  defp clear_reads_in_range(t, s, e) do
    updated_reads =
      t.reads
      |> Enum.filter(fn {k, _v} -> k >= s and k < e end)
      |> Enum.reduce(t.reads, fn {k, _v}, acc -> Map.put(acc, k, :clear) end)

    %{t | reads: updated_reads}
  end

  # =============================================================================
  # Private Helper Functions - Atomic Operations
  # =============================================================================

  # Apply atomic operation to storage value for range queries
  # storage_value is the value from storage servers (nil if not found)
  @doc false
  def apply_atomic_to_storage_value({op, operand}, storage_value),
    do: Atomics.apply_operation(op, storage_value || <<>>, operand) || <<>>

  @doc false
  def apply_atomic_to_storage_value(value, _storage_value), do: value

  # =============================================================================
  # Private Helper Functions - Storage Merging
  # =============================================================================

  # Helper functions for the enhanced merge_storage_range_with_writes

  defp scan_pending_writes(tx, start_key, end_key) when start_key >= end_key do
    # Empty range - just add conflict tracking
    {add_range_conflict(tx, start_key, end_key), []}
  end

  defp scan_pending_writes(tx, start_key, end_key) do
    # Scan transaction writes in the specified range using the same merge logic
    tx_iterator = :gb_trees.iterator_from(start_key, tx.writes)
    {writes_in_range_reversed, _} = merge_ordered_results_bounded([], tx_iterator, [], [], end_key)
    writes_in_range = Enum.reverse(writes_in_range_reversed)

    # Add read conflict tracking
    updated_tx =
      case writes_in_range do
        [] ->
          # No writes found, still need to track the range for conflicts
          add_range_conflict(tx, start_key, end_key)

        [{first_write_key, _} | _] ->
          # Add individual writes to reads and track range
          updated_reads =
            Enum.reduce(writes_in_range, tx.reads, fn {key, value}, acc ->
              Map.put(acc, key, value)
            end)

          {last_write_key, _} = List.last(writes_in_range)
          updated_range_reads = add_or_merge(tx.range_reads, first_write_key, Key.key_after(last_write_key))
          %{tx | reads: updated_reads, range_reads: updated_range_reads}
      end

    {updated_tx, writes_in_range}
  end

  defp add_range_conflict(tx, start_key, end_key) when start_key < end_key do
    updated_range_reads = add_or_merge(tx.range_reads, start_key, end_key)
    %{tx | range_reads: updated_range_reads}
  end

  # Empty range
  defp add_range_conflict(tx, _start_key, _end_key), do: tx

  # Merge storage data with transaction writes, but only within the specified boundary
  defp merge_storage_with_bounded_writes(tx, storage_results, max_boundary) do
    [{first_key, _} | _] = storage_results
    {last_key, _} = List.last(storage_results)

    # Boundary for merging is the minimum of Key.key_after(last_storage) and max_boundary
    merge_boundary = min(Key.key_after(last_key), max_boundary)

    # Get overlapping clear ranges
    data_range = {first_key, merge_boundary}

    tx_clear_ranges =
      Enum.filter(tx.mutations, fn
        {:clear_range, s, e} -> KeyRange.overlap?(data_range, {s, e})
        _ -> false
      end)

    # Merge storage with transaction writes only up to the boundary
    tx_iterator = :gb_trees.iterator_from(first_key, tx.writes)

    {acc, _tx_iterator} =
      merge_ordered_results_bounded(storage_results, tx_iterator, tx_clear_ranges, [], merge_boundary)

    merged_results =
      acc
      |> filter_cleared_keys(tx_clear_ranges)
      |> Enum.reverse()

    # Add conflict tracking
    updated_tx =
      case merged_results do
        [] ->
          tx

        [{actual_first_key, _} | _] ->
          {actual_last_key, _} = List.last(merged_results)

          updated_reads =
            Enum.reduce(merged_results, tx.reads, fn {key, value}, acc ->
              Map.put(acc, key, value)
            end)

          updated_range_reads = add_or_merge(tx.range_reads, actual_first_key, Key.key_after(actual_last_key))

          %{tx | reads: updated_reads, range_reads: updated_range_reads}
      end

    {updated_tx, merged_results}
  end

  # Bounded version of merge_ordered_results that stops at a boundary
  defp merge_ordered_results_bounded([], tx_iterator, clear_ranges, acc, boundary) do
    case :gb_trees.next(tx_iterator) do
      {tx_key, tx_value, iterator} when tx_key < boundary ->
        # Apply atomic operations to storage values (nil here means no storage value)
        final_value = apply_atomic_to_storage_value(tx_value, nil)
        merge_ordered_results_bounded([], iterator, clear_ranges, [{tx_key, final_value} | acc], boundary)

      _ ->
        {acc, tx_iterator}
    end
  end

  defp merge_ordered_results_bounded(storage_list, tx_iterator, clear_ranges, acc, boundary) do
    case :gb_trees.next(tx_iterator) do
      {tx_key, tx_value, iterator} when tx_key < boundary ->
        merge_with_tx_write_bounded({tx_key, tx_value}, iterator, storage_list, clear_ranges, acc, boundary)

      _ ->
        {Enum.reverse(storage_list, acc), tx_iterator}
    end
  end

  defp merge_with_tx_write_bounded(
         {tx_key, tx_value},
         iterator,
         [{storage_key, storage_value} = storage_kv | storage_rest] = storage_list,
         clear_ranges,
         acc,
         boundary
       ) do
    cond do
      tx_key < storage_key ->
        # TX key comes first, apply atomic to nil (no storage value)
        final_value = apply_atomic_to_storage_value(tx_value, nil)
        merge_ordered_results_bounded(storage_list, iterator, clear_ranges, [{tx_key, final_value} | acc], boundary)

      tx_key == storage_key ->
        # Same key, apply atomic to storage value
        final_value = apply_atomic_to_storage_value(tx_value, storage_value)
        merge_ordered_results_bounded(storage_rest, iterator, clear_ranges, [{tx_key, final_value} | acc], boundary)

      true ->
        # Storage key comes first, keep it and continue
        merge_with_tx_write_bounded(
          {tx_key, tx_value},
          iterator,
          storage_rest,
          clear_ranges,
          [storage_kv | acc],
          boundary
        )
    end
  end

  defp merge_with_tx_write_bounded({tx_key, tx_value}, iterator, [], clear_ranges, acc, boundary) do
    # No more storage values, apply atomic to nil
    final_value = apply_atomic_to_storage_value(tx_value, nil)
    merge_ordered_results_bounded([], iterator, clear_ranges, [{tx_key, final_value} | acc], boundary)
  end

  # =============================================================================
  # Private Helper Functions - Utility
  # =============================================================================

  def add_or_merge([], s, e), do: [{s, e}]
  def add_or_merge([{hs, he} | rest], s, e) when e < hs, do: [{s, e}, {hs, he} | rest]
  def add_or_merge([{hs, he} | rest], s, e) when he < s, do: [{hs, he} | add_or_merge(rest, s, e)]
  def add_or_merge([{hs, he} | rest], s, e), do: add_or_merge(rest, min(hs, s), max(he, e))

  defp filter_cleared_keys(key_value_pairs, clear_ranges) do
    Enum.reject(key_value_pairs, fn {key, _value} ->
      key_cleared_by_ranges?(key, clear_ranges)
    end)
  end

  defp key_cleared_by_ranges?(key, clear_ranges) do
    Enum.any?(clear_ranges, fn {:clear_range, start_range, end_range} ->
      key >= start_range and key < end_range
    end)
  end
end
