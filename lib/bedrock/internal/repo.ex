defmodule Bedrock.Internal.Repo do
  import Bedrock.Internal.GenServer.Calls, only: [cast: 2]
  import Bitwise

  alias Bedrock.Cluster.Gateway
  alias Bedrock.KeySelector

  @type transaction :: pid()
  @type key :: term()
  @type value :: term()

  defp txn(repo_module), do: Process.get({:transaction, repo_module})
  defp txn!(repo_module), do: txn(repo_module) || raise("No active transaction")

  @spec add_read_conflict_key(module(), key()) :: :ok
  def add_read_conflict_key(repo_module, key), do: cast(txn!(repo_module), {:add_read_conflict_key, key})

  @spec add_write_conflict_range(module(), key(), key()) :: :ok
  def add_write_conflict_range(repo_module, start_key, end_key),
    do: cast(txn!(repo_module), {:add_write_conflict_range, start_key, end_key})

  @spec get(module(), key()) :: nil | value()
  def get(repo_module, key), do: get(repo_module, key, [])

  @spec get(module(), key(), opts :: keyword()) :: nil | value()
  def get(repo_module, key, opts) do
    t = txn!(repo_module)

    case GenServer.call(t, {:get, key, opts}, :infinity) do
      {:ok, value} ->
        value

      {:error, :not_found} ->
        nil

      {:failure, reason} when reason in [:timeout, :unavailable, :version_too_new] ->
        throw({__MODULE__, t, :retryable_failure, reason})

      {failure_or_error, reason} when failure_or_error in [:error, :failure] and is_atom(reason) ->
        throw({__MODULE__, t, :transaction_error, reason, :get, key})
    end
  end

  @spec select(module(), KeySelector.t()) :: nil | {resolved_key :: key(), value()}
  def select(repo_module, %KeySelector{} = key_selector), do: select(repo_module, key_selector, [])

  @spec select(module(), KeySelector.t(), opts :: keyword()) :: nil | {resolved_key :: key(), value()}
  def select(repo_module, %KeySelector{} = key_selector, opts) do
    t = txn!(repo_module)

    case GenServer.call(t, {:get_key_selector, key_selector, opts}, :infinity) do
      {:ok, {_key, _value} = result} ->
        result

      {:error, :not_found} ->
        nil

      {:failure, reason} when reason in [:timeout, :unavailable, :version_too_new] ->
        throw({__MODULE__, t, :retryable_failure, reason})

      {failure_or_error, reason} when failure_or_error in [:error, :failure] and is_atom(reason) ->
        throw({__MODULE__, t, :transaction_error, reason, :select, key_selector})
    end
  end

  # Streaming

  @doc """
  Create a lazy stream for a range query.

  ## Options

  - `:batch_size` - Number of items to fetch per batch (default: 100)
  - `:timeout` - Timeout per batch request (default: 5000)
  - `:limit` - Maximum total items to return
  """
  @spec get_range(
          module(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            batch_size: pos_integer(),
            timeout: pos_integer(),
            limit: pos_integer(),
            mode: :individual | :batch,
            snapshot: boolean()
          ]
        ) :: Enumerable.t({any(), any()})
  def get_range(repo_module, start_key, end_key), do: get_range(repo_module, start_key, end_key, [])

  def get_range(repo_module, start_key, end_key, opts) do
    txn = txn!(repo_module)
    batch_size = Keyword.get(opts, :batch_size, 100)
    timeout = Keyword.get(opts, :timeout, 5_000)

    # Filter out stream-specific options, keep only TransactionBuilder options
    txn_opts = Keyword.drop(opts, [:batch_size, :timeout])

    # Initial state tracks the current position in the range
    initial_state = %{
      txn: txn,
      current_key: start_key,
      end_key: end_key,
      txn_opts: txn_opts,
      finished: false,
      items_returned: 0,
      limit: opts[:limit],
      current_batch: [],
      has_more: true
    }

    Stream.resource(
      fn -> initial_state end,
      fn state ->
        if state.finished or limit_reached?(state) do
          {:halt, state}
        else
          emit_next_row(state, batch_size, timeout)
        end
      end,
      fn _state -> :ok end
    )
  end

  defp limit_reached?(%{limit: nil}), do: false
  defp limit_reached?(%{limit: limit, items_returned: returned}), do: returned >= limit

  defp emit_next_row(state, batch_size, timeout) do
    case state.current_batch do
      [] -> fetch_and_emit_first_row(state, batch_size, timeout)
      [row | remaining_rows] -> emit_row_from_buffer(state, row, remaining_rows)
    end
  end

  defp emit_row_from_buffer(state, {key, _} = row, remaining_rows) do
    new_state = %{
      state
      | current_batch: remaining_rows,
        current_key: Bedrock.Key.key_after(key),
        items_returned: state.items_returned + 1,
        finished: remaining_rows == [] and not state.has_more
    }

    {[row], new_state}
  end

  defp fetch_and_emit_first_row(state, batch_size, timeout) do
    effective_batch_size =
      case state.limit do
        nil -> batch_size
        limit -> min(batch_size, limit - state.items_returned)
      end

    case GenServer.call(
           state.txn,
           {:get_range, state.current_key, state.end_key, effective_batch_size, state.txn_opts},
           timeout
         ) do
      {:ok, {[], _}} ->
        {:halt, %{state | finished: true}}

      {:ok, {[first_row | rest], has_more}} ->
        emit_row_from_buffer(%{state | current_batch: rest, has_more: has_more}, first_row, rest)

      {:failure, reason} when reason in [:timeout, :unavailable, :version_too_new, :no_servers_to_race] ->
        throw({__MODULE__, state.txn, :retryable_failure, reason})

      {:error, reason} ->
        raise "Range query failed: #{inspect(reason)}"
    end
  end

  # Clearing

  @spec clear_range(
          module(),
          start_key :: key(),
          end_key :: key(),
          opts :: [
            no_write_conflict: boolean()
          ]
        ) :: :ok
  def clear_range(repo_module, start_key, end_key, opts \\ []),
    do: cast(txn!(repo_module), {:clear_range, start_key, end_key, opts})

  @spec clear(module(), key()) :: :ok
  @spec clear(module(), key(), opts :: [no_write_conflict: boolean()]) :: :ok
  def clear(repo_module, key, opts \\ []), do: cast(txn!(repo_module), {:clear, key, opts})

  # Mutation

  @spec put(module(), key(), value(), opts :: [no_write_conflict: boolean()]) :: :ok
  def put(repo_module, key, value, opts \\ []) when is_binary(key) and is_binary(value),
    do: cast(txn!(repo_module), {:set_key, key, value, opts})

  @spec atomic(module(), atom(), key(), binary()) :: :ok
  def atomic(repo_module, op, key, value) when is_atom(op) and is_binary(key) and is_binary(value),
    do: cast(txn!(repo_module), {:atomic, op, key, value})

  # Transaction Control

  @spec rollback(reason :: term()) :: no_return()
  def rollback(reason), do: throw({__MODULE__, :rollback, reason})

  @spec transact(cluster :: module(), repo :: module(), (-> result) | (module() -> result), opts :: keyword()) :: result
        when result: any()
  def transact(cluster, repo, fun, opts \\ []) do
    case txn(repo) do
      nil ->
        start_new_transaction(cluster, repo, fun, {:transaction, repo}, opts)

      existing_txn ->
        start_nested_transaction(repo, existing_txn, fun)
    end
  end

  defp start_new_transaction(cluster, repo, fun, tx_key, opts) do
    retry_limit = Keyword.get(opts, :retry_limit)

    start_retryable_transaction(repo, fun, 0, retry_limit, fn ->
      {:ok, gateway} = cluster.fetch_gateway()

      case Gateway.begin_transaction(gateway) do
        {:ok, txn} ->
          Process.put(tx_key, txn)

          txn

        {:error, reason} ->
          throw({__MODULE__, nil, :retryable_failure, reason})
      end
    end)
  after
    Process.delete(tx_key)
  end

  defp start_nested_transaction(repo, txn, fun) do
    start_retryable_transaction(repo, fun, 0, nil, fn ->
      GenServer.call(txn, :nested_transaction, :infinity)
      txn
    end)
  end

  defp start_retryable_transaction(repo, fun, retry_count, retry_limit, restart_fn) do
    run_transaction(repo, restart_fn.(), fun)
  catch
    {__MODULE__, failed_txn, :retryable_failure, reason} ->
      try_to_rollback(failed_txn)
      enforce_retry_limit(retry_count, retry_limit, reason)
      wait_befor_retry(retry_count)
      start_retryable_transaction(repo, fun, retry_count + 1, retry_limit, restart_fn)

    {__MODULE__, failed_txn, :rollback, reason} ->
      try_to_rollback(failed_txn)
      {:error, reason}

    {__MODULE__, failed_txn, :transaction_error, reason, operation, key} ->
      try_to_rollback(failed_txn)
      raise Bedrock.TransactionError, reason: reason, operation: operation, key: key
  end

  defp run_transaction(repo, txn, fun) do
    fun
    |> Function.info(:arity)
    |> case do
      {:arity, 1} -> fun.(repo)
      {:arity, 0} -> fun.()
    end
    |> then(&try_to_commit(txn, &1))
  rescue
    exception ->
      try_to_rollback(txn)
      reraise exception, __STACKTRACE__
  end

  defp wait_befor_retry(retry_count) do
    base_delay = 1 <<< retry_count
    jitter = :rand.uniform(3)
    wait_time_in_ms = min(base_delay + jitter, 1000)
    Process.sleep(wait_time_in_ms)
  end

  defp enforce_retry_limit(_retry_count, nil, _reason), do: :ok
  defp enforce_retry_limit(retry_count, retry_limit, _reason) when retry_count < retry_limit, do: :ok

  defp enforce_retry_limit(_retry_count, retry_limit, reason) do
    raise Bedrock.TransactionError,
      reason: "Retry limit exceeded after #{retry_limit} attempts. Last error: #{inspect(reason)}",
      retry_limit: retry_limit
  end

  defp try_to_commit(txn, result) do
    case GenServer.call(txn, :commit) do
      :ok -> result
      {:ok, _commit_version} -> result
      {:error, reason} -> throw({__MODULE__, txn, :retryable_failure, reason})
    end
  end

  defp try_to_rollback(nil), do: :ok
  defp try_to_rollback(txn), do: GenServer.cast(txn, :rollback)
end
