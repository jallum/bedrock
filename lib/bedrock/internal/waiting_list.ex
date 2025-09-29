defmodule Bedrock.Internal.WaitingList do
  @moduledoc """
  Unified waiting list for version-based out-of-order request handling.

  Supports both single-waiter (Resolver) and multi-waiter (LongPulls) patterns
  using a map of deadline-sorted lists.

  Structure: %{version => [{deadline, reply_fn, data}, ...]}
  Lists are sorted by deadline (earliest first).
  """

  alias Bedrock.Internal.Time

  @type version :: Bedrock.version()
  @type reply_fn :: (any() -> :ok)
  @type timeout_ms :: non_neg_integer()

  @type entry :: {
          deadline :: integer(),
          reply_fn :: reply_fn(),
          data :: any()
        }
  @type t :: %{version() => [entry()]}

  @doc """
  Add entry to waiting list with deadline.
  Lists are kept sorted by deadline (earliest first).
  Returns {new_map, timeout_for_next_deadline}.
  """
  @spec insert(t(), version(), any(), reply_fn(), timeout_ms()) ::
          {t(), timeout()}
  def insert(map, version, data, reply_fn, timeout_ms) do
    insert(map, version, data, reply_fn, timeout_ms, &Time.monotonic_now_in_ms/0)
  end

  @doc """
  Add entry to waiting list with deadline using provided time function.
  Lists are kept sorted by deadline (earliest first).
  Returns {new_map, timeout_for_next_deadline}.
  """
  @spec insert(t(), version(), any(), reply_fn(), timeout_ms(), (-> integer())) ::
          {t(), timeout()}
  def insert(map, version, data, reply_fn, timeout_ms, time_fn) when timeout_ms >= 0 do
    now = time_fn.()
    deadline = now + timeout_ms
    entry = {deadline, reply_fn, data}

    new_map = Map.update(map, version, [entry], &insert_sorted(&1, entry))
    timeout = next_timeout(new_map, time_fn)
    {new_map, timeout}
  end

  @doc """
  Remove first entry for a version from waiting list.
  For single-waiter patterns (Resolver).
  Returns {new_map, removed_entry | nil}.
  """
  @spec remove(t(), version()) :: {t(), entry() | nil}
  def remove(map, version) do
    case Map.get(map, version, []) do
      [] ->
        {map, nil}

      [entry | rest] ->
        new_map =
          case rest do
            [] -> Map.delete(map, version)
            remaining -> Map.put(map, version, remaining)
          end

        {new_map, entry}
    end
  end

  @doc """
  Remove all entries for a version from waiting list.
  For multi-waiter patterns (LongPulls).
  Returns {new_map, removed_entries}.
  """
  @spec remove_all(t(), version()) :: {t(), [entry()]}
  def remove_all(map, version) do
    {entries, new_map} = Map.pop(map, version, [])
    {new_map, entries}
  end

  @doc """
  Find first entry for a version without removing it.
  Returns entry or nil.
  """
  @spec find(t(), version()) :: entry() | nil
  def find(map, version) do
    case Map.get(map, version, []) do
      [] -> nil
      [entry | _] -> entry
    end
  end

  @doc """
  Expire entries past their deadline from waiting list.
  Returns {new_map, expired_entries}.
  """
  @spec expire(t()) :: {t(), [entry()]}
  def expire(map), do: expire(map, &Time.monotonic_now_in_ms/0)

  @doc """
  Expire entries past their deadline from waiting list using provided time function.
  Returns {new_map, expired_entries}.
  """
  @spec expire(t(), (-> integer())) :: {t(), [entry()]}
  def expire(map, time_fn) do
    now = time_fn.()

    {expired_entries, new_map} =
      Enum.reduce(map, {[], %{}}, fn {version, entries}, {expired_acc, map_acc} ->
        {expired, remaining} = split_expired(entries, now)

        case remaining do
          [] -> {expired ++ expired_acc, map_acc}
          remaining -> {expired ++ expired_acc, Map.put(map_acc, version, remaining)}
        end
      end)

    {new_map, expired_entries}
  end

  @doc """
  Calculate timeout for next deadline in waiting list.
  """
  @spec next_timeout(t()) :: timeout()
  def next_timeout(map) when map_size(map) == 0, do: :infinity
  def next_timeout(map), do: next_timeout(map, &Time.monotonic_now_in_ms/0)

  @doc """
  Calculate timeout for next deadline in waiting list using provided time function.
  """
  @spec next_timeout(t(), (-> integer())) :: timeout()
  def next_timeout(map, _time_fn) when map_size(map) == 0, do: :infinity

  def next_timeout(map, time_fn) do
    now = time_fn.()

    map
    |> Map.values()
    |> Enum.map(&get_earliest_deadline/1)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] ->
        :infinity

      deadlines ->
        next_deadline = Enum.min(deadlines)
        max(0, next_deadline - now)
    end
  end

  @doc """
  Reply to expired entries with error response.
  """
  @spec reply_to_expired([entry()], any()) :: :ok
  def reply_to_expired(expired_entries, error_response \\ {:error, :waiting_timeout}) do
    Enum.each(expired_entries, fn {_deadline, reply_fn, _data} ->
      reply_fn.(error_response)
    end)
  end

  # Private functions

  # Insert entry into deadline-sorted list (earliest first)
  defp insert_sorted([], entry), do: [entry]

  defp insert_sorted([{first_deadline, _, _} = first | rest], {deadline, _, _} = entry) do
    if deadline <= first_deadline do
      [entry, first | rest]
    else
      [first | insert_sorted(rest, entry)]
    end
  end

  # Split entries into expired and remaining, maintaining sort order
  defp split_expired(entries, now, expired \\ [], remaining \\ [])

  defp split_expired([{deadline, _, _} = entry | rest], now, expired, remaining) when deadline <= now,
    do: split_expired(rest, now, [entry | expired], remaining)

  defp split_expired(list, _now, expired, remaining), do: {Enum.reverse(expired), Enum.reverse(remaining, list)}

  # Get earliest deadline from a deadline-sorted list
  defp get_earliest_deadline([]), do: nil
  defp get_earliest_deadline([{deadline, _, _} | _]), do: deadline
end
