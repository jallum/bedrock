defmodule Bedrock.DataPlane.CommitProxy.Finalization do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.EncodedTransaction
  alias Bedrock.DataPlane.Log.Transaction
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.Service.Worker

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [transactions_in_order: 1]

  @doc """
  Finalizes a batch of transactions by resolving conflicts, separating
  successful transactions from aborts, and pushing them to the log servers.

  This function processes a batch of transactions, first ensuring that any
  conflicts are resolved. After conflict resolution, it organizes the
  transactions into those that will be committed and those that will be aborted.

  Clients with aborted transactions are notified of the abort immediately.
  Successful transactions are pushed to the system's logs, and clients that
  submitted the transactions are notified when a majority of the log servers
  have acknowledged.

  ## Parameters

    - `batch`: A `Batch.t()` struct that contains the transactions to be finalized,
      along with the commit version details.
    - `transaction_system_layout`: Provides configuration and systemic details,
      including the available resolver and log servers.

  ## Returns
    - `:ok` when the batch has been processed, and all clients have been
      notified about the status of their transactions.
  """
  @spec finalize_batch(Batch.t(), TransactionSystemLayout.t()) ::
          {:ok, n_aborts :: non_neg_integer(), n_oks :: non_neg_integer()} | {:error, term()}
  def finalize_batch(batch, transaction_system_layout) do
    transactions_in_order = transactions_in_order(batch)

    commit_version = batch.commit_version

    with {:ok, aborted} <-
           resolve_transactions(
             transaction_system_layout.resolvers,
             batch.last_commit_version,
             commit_version,
             transform_transactions_for_resolution(transactions_in_order),
             timeout: 1_000
           ),
         {oks, aborts, compacted_transaction} <-
           prepare_transaction_to_log(
             transactions_in_order,
             aborted,
             commit_version
           ),
         :ok <- reply_to_all_clients_with_aborted_transactions(aborts),
         :ok <-
           push_transaction_to_logs(
             transaction_system_layout,
             batch.last_commit_version,
             compacted_transaction,
             fn version ->
               send_reply_with_commit_version(oks, version)
             end
           ) do
      {:ok, length(aborts), length(oks)}
    else
      {:error, _reason} = error ->
        batch
        |> Batch.all_callers()
        |> reply_to_all_clients_with_aborted_transactions()

        error
    end
  end

  @spec resolve_transactions(
          resolvers :: [{start_key :: Bedrock.key(), Resolver.ref()}],
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [Resolver.transaction()],
          opts :: [timeout: :infinity | non_neg_integer()]
        ) ::
          {:ok, aborted :: [index :: integer()]}
          | {:error, :timeout}
          | {:error, :unavailable}
  def resolve_transactions(
        resolvers,
        last_version,
        commit_version,
        transaction_summaries,
        opts \\ []
      ) do
    ranges =
      resolvers
      |> Enum.map(&elem(&1, 0))
      |> Enum.concat([:end])
      |> Enum.chunk_every(2, 1, :discard)

    transaction_summaries_by_start_key =
      ranges
      |> Enum.map(fn
        [start_key, end_key] ->
          filtered_summaries =
            filter_transaction_summaries(
              transaction_summaries,
              filter_fn(start_key, end_key)
            )

          {start_key, filtered_summaries}
      end)
      |> Enum.into(%{})

    resolvers
    |> Enum.map(fn {start_key, ref} ->
      Resolver.resolve_transactions(
        ref,
        last_version,
        commit_version,
        Map.get(transaction_summaries_by_start_key, start_key, opts)
      )
    end)
    |> Enum.reduce({:ok, []}, fn
      {:ok, aborted}, {:ok, acc} ->
        {:ok, Enum.uniq(acc ++ aborted)}

      {:error, reason}, _ ->
        {:error, reason}
    end)
  end

  defp filter_fn(start_key, :end), do: &(&1 >= start_key)
  defp filter_fn(start_key, end_key), do: &(&1 >= start_key and &1 < end_key)

  defp filter_transaction_summaries(transaction_summaries, filter_fn),
    do: Enum.map(transaction_summaries, &filter_transaction_summary(&1, filter_fn))

  defp filter_transaction_summary({nil, writes}, filter_fn),
    do: {nil, Enum.filter(writes, filter_fn)}

  defp filter_transaction_summary({{read_version, reads}, writes}, filter_fn),
    do: {{read_version, Enum.filter(reads, filter_fn)}, Enum.filter(writes, filter_fn)}

  @spec determine_majority(n :: non_neg_integer()) :: non_neg_integer()
  defp determine_majority(n), do: 1 + div(n, 2)

  @doc """
  Pushes a transaction to the logs and waits for acknowledgement from a
  majority of log servers.

  This function takes a transaction and tries to send it to all available
  log servers. It uses asynchronous tasks to push the transaction to each
  log server, and will consider the push successful once a majority of the
  servers confirm successful acceptance of the transaction.

  Once a majority of logs have acknowledged the push, the function will
  send an acknowledgement of success to the clients that initiated the
  transactions.

  ## Parameters

    - `transaction_system_layout`: Contains configuration information about the
      transaction system, including available log servers.
    - `last_commit_version`: The last known committed version; used to
      ensure consistency in log ordering.
    - `transaction`: The transaction to be committed to logs; this should
      include both data and a new commit version.
    - `oks`: A list of reply functions to which successful acknowledgements
      should be sent once enough logs have acknowledged.

  ## Returns
    - `:ok` if enough acknowledgements have been received from the log servers.
    - `:error` if a majority of logs have not successfully acknowledged the
       push within the timeout period.
  """
  @spec push_transaction_to_logs(
          TransactionSystemLayout.t(),
          last_commit_version :: Bedrock.version(),
          Transaction.t(),
          majority_reached :: (Bedrock.version() -> :ok)
        ) :: :ok
  def push_transaction_to_logs(
        transaction_system_layout,
        last_commit_version,
        transaction,
        majority_reached
      ) do
    commit_version = Transaction.version(transaction)
    encoded_transacton = EncodedTransaction.encode(transaction)

    log_descriptors = transaction_system_layout.logs
    n = map_size(log_descriptors)
    m = determine_majority(n)

    log_descriptors
    |> resolve_log_descriptors(transaction_system_layout.services)
    |> Task.async_stream(
      fn {log_id, service_descriptor} ->
        service_descriptor
        |> try_to_push_transaction_to_log(encoded_transacton, last_commit_version)

        :ok
        |> then(&{log_id, &1})
      end,
      timeout: 5_000
    )
    |> Enum.reduce_while(0, fn
      {:ok, {_log_id, {:error, _reason}}}, count ->
        {:cont, count}

      {:ok, {_log_id, :ok}}, count ->
        count = 1 + count

        if count == m do
          :ok = majority_reached.(commit_version)
        end

        {:cont, count}
    end)
    |> case do
      ^n -> :ok
      # If we haven't received enough responses, we need to abort
      _ -> :error
    end
  end

  @spec resolve_log_descriptors(
          %{Log.id() => LogDescriptor.t()},
          %{Worker.id() => ServiceDescriptor.t()}
        ) ::
          %{Worker.id() => ServiceDescriptor.t()}
  def resolve_log_descriptors(log_descriptors, services) do
    log_descriptors
    |> Map.keys()
    |> Enum.map(&{&1, Map.get(services, &1)})
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Map.new()
  end

  @spec try_to_push_transaction_to_log(
          ServiceDescriptor.t(),
          EncodedTransaction.t(),
          Bedrock.version()
        ) ::
          :ok | {:error, :unavailable}
  def try_to_push_transaction_to_log(
        %{kind: :log, status: {:up, log_server}},
        transaction,
        last_commit_version
      ) do
    Log.push(log_server, transaction, last_commit_version)
  end

  def try_to_push_transaction_to_log(_, _, _), do: {:error, :unavailable}

  @spec reply_to_all_clients_with_aborted_transactions([Batch.reply_fn()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions([]), do: :ok

  def reply_to_all_clients_with_aborted_transactions(aborts),
    do: Enum.each(aborts, & &1.({:error, :aborted}))

  @spec send_reply_with_commit_version([Batch.reply_fn()], Bedrock.version()) ::
          :ok
  def send_reply_with_commit_version(oks, commit_version),
    do: Enum.each(oks, & &1.({:ok, commit_version}))

  @doc """
  Prepare a transaction for logging by separating successful transactions
  from aborted ones and consolidating writes. Since we've completed conflict
  resolution, we can drop the read data and only keep the writes.

  For transactions without any aborts, it efficiently aggregates all writes and
  acknowledges all clients with successful executions.

  In the presence of aborted transactions, it identifies and separates them,
  ensuring only the successful transactions' writes are aggregated, and replies
  to the relevant clients about the aborts.

  Returns a tuple with:
    - A list of reply functions for successful transactions.
    - A list of reply functions for aborted transactions.
    - A Transaction.t() containing the commit version along with the aggregated
      writes from the successful transactions.

  ## Parameters

    - `transactions`: A list of transactions, each containing the reply
      function, read/write data, and other necessary details.
    - `aborts`: A list of integer indices indicating which transactions were
      aborted.
    - `commit_version`: The current commit version.

  ## Returns
    - A tuple: `{oks, aborts, transaction_to_log}`
  """
  @spec prepare_transaction_to_log(
          transactions :: [Bedrock.transaction()],
          aborts :: [integer()],
          commit_version :: Bedrock.version()
        ) ::
          {oks :: [Batch.reply_fn()], aborts :: [Batch.reply_fn()], Transaction.t()}
  # If there are no aborted transactions, we can make take some shortcuts.
  def prepare_transaction_to_log(transactions, [], commit_version) do
    transactions
    |> Enum.reduce({[], %{}}, fn {from, {_, writes}}, {oks, all_writes} ->
      {[from | oks], Map.merge(all_writes, writes)}
    end)
    |> then(fn {oks, combined_writes} ->
      {oks, [], Transaction.new(commit_version, combined_writes)}
    end)
  end

  # If there are aborted transactions, we need to pluck them out so that they
  # can be informed of the failure.
  def prepare_transaction_to_log(transactions, aborts, commit_version) do
    aborted_set = MapSet.new(aborts)

    transactions
    |> Enum.with_index()
    |> Enum.reduce({[], [], %{}}, fn
      {{from, {_, _, writes}}, idx}, {oks, aborts, all_writes} ->
        if MapSet.member?(aborted_set, idx) do
          {oks, [from | aborts], all_writes}
        else
          {[from | oks], aborts, Map.merge(all_writes, writes)}
        end
    end)
    |> then(fn {oks, aborts, combined_writes} ->
      {oks, aborts, Transaction.new(commit_version, combined_writes)}
    end)
  end

  @doc """
  Transforms the list of transactions for resolution.

  Converts the transaction data to the format expected by the conflict
  resolution logic. For each transaction, it extracts the read version,
  the reads, and the keys of the writes, discarding the values of the writes
  as they are not needed for resolution.
  """
  @spec transform_transactions_for_resolution([Bedrock.transaction()]) :: [Resolver.transaction()]
  def transform_transactions_for_resolution(transactions) do
    transactions
    |> Enum.map(fn
      {_from, {nil, writes}} ->
        {nil, writes |> Map.keys()}

      {_from, {{read_version, reads}, writes}} ->
        {{read_version, reads |> Enum.uniq()}, writes |> Map.keys()}
    end)
  end
end
