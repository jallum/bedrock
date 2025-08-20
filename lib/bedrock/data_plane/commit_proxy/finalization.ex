defmodule Bedrock.DataPlane.CommitProxy.Finalization do
  @moduledoc """
  Transaction finalization pipeline that handles conflict resolution and log persistence.

  ## Version Chain Integrity

  CRITICAL: This module maintains the Lamport clock version chain established by the sequencer.
  The sequencer provides both `last_commit_version` and `commit_version` as a proper chain link:

  - `last_commit_version`: The actual last committed version from the sequencer
  - `commit_version`: The new version assigned to this batch

  Always use the exact version values provided by the sequencer through the batch to maintain
  proper MVCC conflict detection and transaction ordering. Version gaps can exist due to failed
  transactions, recovery scenarios, or system restarts.
  """

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [transactions_in_order: 1]

  import Bitwise, only: [<<<: 2]

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Transaction

  @type resolver_fn() :: (resolvers :: [{start_key :: Bedrock.key(), Resolver.ref()}],
                          last_version :: Bedrock.version(),
                          commit_version :: Bedrock.version(),
                          transaction_summaries :: [Resolver.transaction_summary()],
                          resolver_opts :: [timeout: Bedrock.timeout_in_ms()] ->
                            {:ok, aborted :: [transaction_index :: non_neg_integer()]}
                            | {:error, resolution_error()})

  @type log_push_batch_fn() :: (TransactionSystemLayout.t(),
                                last_commit_version :: Bedrock.version(),
                                transactions_by_tag :: %{
                                  Bedrock.range_tag() => Transaction.encoded()
                                },
                                commit_version :: Bedrock.version(),
                                opts :: [
                                  timeout: Bedrock.timeout_in_ms(),
                                  async_stream_fn: async_stream_fn()
                                ] ->
                                  :ok | {:error, log_push_error()})

  @type log_push_single_fn() :: (ServiceDescriptor.t(), binary(), Bedrock.version() ->
                                   :ok | {:error, :unavailable})

  @type async_stream_fn() :: (enumerable :: Enumerable.t(), fun :: (term() -> term()), opts :: keyword() ->
                                Enumerable.t())

  @type abort_reply_fn() :: ([Batch.reply_fn()] -> :ok)

  @type success_reply_fn() :: ([Batch.reply_fn()], Bedrock.version() -> :ok)

  @type timeout_fn() :: (non_neg_integer() -> non_neg_integer())

  @type resolution_error() ::
          :timeout
          | :unavailable
          | {:resolver_unavailable, term()}

  @type storage_coverage_error() ::
          {:storage_team_coverage_error, binary()}

  @type log_push_error() ::
          {:log_failures, [{Log.id(), term()}]}
          | {:insufficient_acknowledgments, non_neg_integer(), non_neg_integer(), [{Log.id(), term()}]}
          | :log_push_failed

  @type finalization_error() ::
          resolution_error()
          | storage_coverage_error()
          | log_push_error()

  defmodule FinalizationPlan do
    @moduledoc """
    Pipeline state for transaction finalization that accumulates information
    and tracks reply status to prevent double-replies.
    """

    @enforce_keys [
      :transactions,
      :commit_version,
      :last_commit_version,
      :storage_teams,
      :logs_by_id
    ]
    defstruct [
      :transactions,
      :indexed_transactions,
      :commit_version,
      :last_commit_version,
      :storage_teams,
      :logs_by_id,
      resolver_data: [],
      aborted_indices: [],
      aborted_indices_set: %MapSet{},
      aborted_replies: [],
      successful_replies: [],
      transactions_by_log: %{},
      replied_indices: MapSet.new(),
      stage: :initialized,
      error: nil
    ]

    @type t :: %__MODULE__{
            transactions: [{Batch.reply_fn(), binary()}],
            commit_version: Bedrock.version(),
            last_commit_version: Bedrock.version(),
            storage_teams: [StorageTeamDescriptor.t()],
            logs_by_id: %{Log.id() => [Bedrock.range_tag()]},
            resolver_data: [Resolver.transaction_summary()],
            aborted_indices: [integer()],
            aborted_replies: [Batch.reply_fn()],
            successful_replies: [Batch.reply_fn()],
            transactions_by_log: %{Log.id() => Transaction.encoded()},
            replied_indices: MapSet.t(),
            stage: atom(),
            error: term() | nil
          }
  end

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
  @spec finalize_batch(
          Batch.t(),
          TransactionSystemLayout.t(),
          opts :: [
            epoch: Bedrock.epoch(),
            resolver_fn: resolver_fn(),
            batch_log_push_fn: log_push_batch_fn(),
            abort_reply_fn: abort_reply_fn(),
            success_reply_fn: success_reply_fn(),
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            timeout: non_neg_integer()
          ]
        ) ::
          {:ok, n_aborts :: non_neg_integer(), n_oks :: non_neg_integer()}
          | {:error, finalization_error()}
  def finalize_batch(batch, transaction_system_layout, opts \\ []) do
    batch
    |> create_finalization_plan(transaction_system_layout)
    |> prepare_for_resolution()
    |> resolve_conflicts(transaction_system_layout, opts)
    |> split_and_notify_aborts(opts)
    |> prepare_for_logging()
    |> push_to_logs(transaction_system_layout, opts)
    |> notify_sequencer(transaction_system_layout.sequencer)
    |> notify_successes(opts)
    |> extract_result_or_handle_error(opts)
  end

  @spec create_finalization_plan(Batch.t(), TransactionSystemLayout.t()) :: FinalizationPlan.t()
  defp create_finalization_plan(batch, transaction_system_layout) do
    %FinalizationPlan{
      transactions: transactions_in_order(batch),
      commit_version: batch.commit_version,
      last_commit_version: batch.last_commit_version,
      storage_teams: transaction_system_layout.storage_teams,
      logs_by_id: transaction_system_layout.logs,
      stage: :created
    }
  end

  @spec prepare_for_resolution(FinalizationPlan.t()) :: FinalizationPlan.t()
  defp prepare_for_resolution(%FinalizationPlan{stage: :created} = plan) do
    resolver_data = transform_transactions_for_resolution(plan.transactions)

    %{plan | resolver_data: resolver_data, stage: :ready_for_resolution}
  end

  @doc """
  Transforms the list of transactions for resolution.

  Converts the transaction data to the format expected by the conflict
  resolution logic. For each transaction, it extracts the read version,
  read conflicts, and write conflicts. Handles both map format (Bedrock.transaction())
  and binary encoded format for backward compatibility.
  """
  @spec transform_transactions_for_resolution([
          {Batch.reply_fn(), Bedrock.transaction() | binary()}
        ]) :: [
          Resolver.transaction_summary()
        ]
  def transform_transactions_for_resolution(transactions) do
    Enum.map(transactions, &transform_single_transaction_for_resolution/1)
  end

  @spec transform_single_transaction_for_resolution({Batch.reply_fn(), Bedrock.transaction() | binary()}) ::
          Resolver.transaction_summary()
  defp transform_single_transaction_for_resolution({_reply_fn, transaction}) when is_map(transaction) do
    read_version = Map.get(transaction, :read_version)
    read_conflicts = Map.get(transaction, :read_conflicts, [])
    write_conflicts = Map.get(transaction, :write_conflicts, [])

    transform_version_and_conflicts(read_version, read_conflicts, write_conflicts)
  end

  defp transform_single_transaction_for_resolution({_reply_fn, binary_transaction})
       when is_binary(binary_transaction) do
    {:ok, read_version} = Transaction.extract_read_version(binary_transaction)

    {:ok, {_read_version, read_conflicts}} =
      Transaction.extract_read_conflicts(binary_transaction)

    {:ok, write_conflicts} = Transaction.extract_write_conflicts(binary_transaction)

    transform_version_and_conflicts(read_version, read_conflicts, write_conflicts)
  end

  @spec transform_version_and_conflicts(nil | Bedrock.version(), [Bedrock.key()], [Bedrock.key()]) ::
          Resolver.transaction_summary()
  defp transform_version_and_conflicts(nil, _read_conflicts, write_conflicts) do
    {nil, write_conflicts}
  end

  defp transform_version_and_conflicts(version, read_conflicts, write_conflicts) do
    {{version, Enum.uniq(read_conflicts)}, write_conflicts}
  end

  @spec resolve_conflicts(FinalizationPlan.t(), TransactionSystemLayout.t(), keyword()) ::
          FinalizationPlan.t()
  defp resolve_conflicts(%FinalizationPlan{stage: :ready_for_resolution} = plan, layout, opts) do
    resolver_fn = Keyword.get(opts, :resolver_fn, &resolve_transactions/6)
    epoch = Keyword.get(opts, :epoch) || raise "Missing epoch in finalization opts"

    case resolver_fn.(
           epoch,
           layout.resolvers,
           plan.last_commit_version,
           plan.commit_version,
           plan.resolver_data,
           Keyword.put(opts, :timeout, 1_000)
         ) do
      {:ok, aborted_indices} ->
        %{plan | aborted_indices: aborted_indices, stage: :conflicts_resolved}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @spec resolve_transactions(
          epoch :: Bedrock.epoch(),
          resolvers :: [{start_key :: Bedrock.key(), Resolver.ref()}],
          last_version :: Bedrock.version(),
          commit_version :: Bedrock.version(),
          [Resolver.transaction_summary()],
          opts :: [
            timeout: :infinity | non_neg_integer(),
            timeout_fn: timeout_fn(),
            attempts_remaining: non_neg_integer(),
            attempts_used: non_neg_integer()
          ]
        ) ::
          {:ok, aborted :: [index :: integer()]}
          | {:error, resolution_error()}
  def resolve_transactions(epoch, resolvers, last_version, commit_version, transaction_summaries, opts) do
    timeout_fn = Keyword.get(opts, :timeout_fn, &default_timeout_fn/1)
    attempts_remaining = Keyword.get(opts, :attempts_remaining, 2)
    attempts_used = Keyword.get(opts, :attempts_used, 0)
    timeout = Keyword.get(opts, :timeout, timeout_fn.(attempts_used))

    ranges =
      resolvers
      |> Enum.map(&elem(&1, 0))
      |> Enum.concat([:end])
      |> Enum.chunk_every(2, 1, :discard)

    transaction_summaries_by_start_key =
      Map.new(ranges, fn
        [start_key, end_key] ->
          filtered_summaries =
            filter_transaction_summaries(
              transaction_summaries,
              filter_fn(start_key, end_key)
            )

          {start_key, filtered_summaries}
      end)

    result =
      resolvers
      |> Enum.map(fn {start_key, ref} ->
        Resolver.resolve_transactions(
          ref,
          epoch,
          last_version,
          commit_version,
          Map.get(transaction_summaries_by_start_key, start_key, []),
          timeout: timeout
        )
      end)
      |> Enum.reduce({:ok, []}, fn
        {:ok, aborted}, {:ok, acc} ->
          {:ok, Enum.uniq(acc ++ aborted)}

        {:error, reason}, _ ->
          {:error, reason}
      end)

    case result do
      {:ok, _} = success ->
        success

      {:error, reason} when attempts_remaining > 0 ->
        :telemetry.execute(
          [:bedrock, :commit_proxy, :resolver, :retry],
          %{attempts_remaining: attempts_remaining - 1, attempts_used: attempts_used + 1},
          %{reason: reason}
        )

        updated_opts =
          opts
          |> Keyword.put(:attempts_remaining, attempts_remaining - 1)
          |> Keyword.put(:attempts_used, attempts_used + 1)

        resolve_transactions(
          epoch,
          resolvers,
          last_version,
          commit_version,
          transaction_summaries,
          updated_opts
        )

      {:error, reason} ->
        :telemetry.execute(
          [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded],
          %{total_attempts: attempts_used + 1},
          %{reason: reason}
        )

        {:error, {:resolver_unavailable, reason}}
    end
  end

  @spec default_timeout_fn(non_neg_integer()) :: non_neg_integer()
  def default_timeout_fn(attempts_used), do: 500 * (1 <<< attempts_used)

  @spec filter_fn(Bedrock.key(), :end | Bedrock.key()) :: (Bedrock.key() -> boolean())
  defp filter_fn(start_key, :end), do: &(&1 >= start_key)
  defp filter_fn(start_key, end_key), do: &(&1 >= start_key and &1 < end_key)

  @spec filter_transaction_summaries([Resolver.transaction_summary()], (Bedrock.key() ->
                                                                          boolean())) :: [
          Resolver.transaction_summary()
        ]
  defp filter_transaction_summaries(transaction_summaries, filter_fn),
    do: Enum.map(transaction_summaries, &filter_transaction_summary(&1, filter_fn))

  @spec filter_transaction_summary(Resolver.transaction_summary(), (Bedrock.key() -> boolean())) ::
          Resolver.transaction_summary()
  defp filter_transaction_summary({nil, writes}, filter_fn), do: {nil, Enum.filter(writes, filter_fn)}

  defp filter_transaction_summary({{read_version, reads}, writes}, filter_fn),
    do: {{read_version, Enum.filter(reads, filter_fn)}, Enum.filter(writes, filter_fn)}

  @spec split_and_notify_aborts(FinalizationPlan.t(), keyword()) :: FinalizationPlan.t()
  defp split_and_notify_aborts(%FinalizationPlan{stage: :failed} = plan, _opts), do: plan

  defp split_and_notify_aborts(%FinalizationPlan{stage: :conflicts_resolved} = plan, opts) do
    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    indexed_transactions = Enum.with_index(plan.transactions)

    {aborted_replies, successful_replies, aborted_indices_set} =
      split_transactions_by_abort_status(plan, indexed_transactions)

    new_aborted_indices = MapSet.difference(aborted_indices_set, plan.replied_indices)

    new_aborted_replies =
      indexed_transactions
      |> Enum.filter(fn {_transaction, idx} -> MapSet.member?(new_aborted_indices, idx) end)
      |> Enum.map(fn {{reply_fn, _transaction}, _idx} -> reply_fn end)

    abort_reply_fn.(new_aborted_replies)

    updated_replied_indices = MapSet.union(plan.replied_indices, aborted_indices_set)

    %{
      plan
      | indexed_transactions: indexed_transactions,
        aborted_indices_set: aborted_indices_set,
        aborted_replies: aborted_replies,
        successful_replies: successful_replies,
        replied_indices: updated_replied_indices,
        stage: :aborts_notified
    }
  end

  @spec reply_to_all_clients_with_aborted_transactions([Batch.reply_fn()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions([]), do: :ok

  def reply_to_all_clients_with_aborted_transactions(aborts), do: Enum.each(aborts, & &1.({:error, :aborted}))

  @spec split_transactions_by_abort_status(FinalizationPlan.t(), list()) ::
          {[Batch.reply_fn()], [Batch.reply_fn()], MapSet.t()}
  defp split_transactions_by_abort_status(plan, indexed_transactions) do
    aborted_set = MapSet.new(plan.aborted_indices)

    {aborted_replies, successful_replies} =
      Enum.reduce(indexed_transactions, {[], []}, fn {{reply_fn, _transaction}, idx}, {aborts_acc, success_acc} ->
        if MapSet.member?(aborted_set, idx) do
          {[reply_fn | aborts_acc], success_acc}
        else
          {aborts_acc, [reply_fn | success_acc]}
        end
      end)

    {aborted_replies, successful_replies, aborted_set}
  end

  @spec prepare_for_logging(FinalizationPlan.t()) :: FinalizationPlan.t()
  defp prepare_for_logging(%FinalizationPlan{stage: :failed} = plan), do: plan

  defp prepare_for_logging(%FinalizationPlan{stage: :aborts_notified} = plan) do
    case build_transactions_for_logs(plan, plan.logs_by_id) do
      {:ok, transactions_by_log} ->
        %{plan | transactions_by_log: transactions_by_log, stage: :ready_for_logging}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @spec build_transactions_for_logs(FinalizationPlan.t(), %{Log.id() => [Bedrock.range_tag()]}) ::
          {:ok, %{Log.id() => Transaction.encoded()}} | {:error, term()}
  defp build_transactions_for_logs(%{} = plan, logs_by_id) do
    initial_mutations_by_log = Map.new(logs_by_id, fn {key, _} -> {key, []} end)

    plan.indexed_transactions
    |> Enum.reduce_while({:ok, initial_mutations_by_log}, fn transaction_with_idx, {:ok, acc} ->
      process_transaction_for_logs(transaction_with_idx, plan.aborted_indices_set, plan.storage_teams, logs_by_id, acc)
    end)
    |> case do
      {:ok, mutations_by_log} ->
        result =
          Map.new(mutations_by_log, fn {log_id, mutations_list} ->
            encoded =
              Transaction.encode(%{
                mutations: Enum.reverse(mutations_list), # Why reverse here?
                commit_version: plan.commit_version
              })

            {log_id, encoded}
          end)

        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec process_transaction_for_logs(
          {{function(), binary()}, non_neg_integer()},
          MapSet.t(non_neg_integer()),
          [StorageTeamDescriptor.t()],
          %{Log.id() => [Bedrock.range_tag()]},
          %{Log.id() => [term()]}
        ) ::
          {:cont, {:ok, %{Log.id() => [term()]}}}
          | {:halt, {:error, term()}}
  defp process_transaction_for_logs({{_reply_fn, binary_transaction}, idx}, aborted_set, storage_teams, logs_by_id, acc) do
    if MapSet.member?(aborted_set, idx) do
      {:cont, {:ok, acc}}
    else
      process_transaction_mutations(binary_transaction, storage_teams, logs_by_id, acc)
    end
  end

  @spec process_transaction_mutations(
          binary(),
          [StorageTeamDescriptor.t()],
          %{Log.id() => [Bedrock.range_tag()]},
          %{Log.id() => [term()]}
        ) ::
          {:cont, {:ok, %{Log.id() => [term()]}}} | {:halt, {:error, term()}}
  defp process_transaction_mutations(binary_transaction, storage_teams, logs_by_id, acc) do
    case Transaction.stream_mutations(binary_transaction) do
      {:ok, mutations_stream} ->
        case process_mutations_for_transaction(mutations_stream, storage_teams, logs_by_id, acc) do
          {:ok, updated_acc} ->
            {:cont, {:ok, updated_acc}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end

      {:error, reason} ->
        {:halt, {:error, {:mutation_extraction_failed, reason}}}
    end
  end

  @spec process_mutations_for_transaction(
          Enumerable.t(),
          [StorageTeamDescriptor.t()],
          %{Log.id() => [Bedrock.range_tag()]},
          %{Log.id() => [term()]}
        ) ::
          {:ok, %{Log.id() => [term()]}} | {:error, term()}
  defp process_mutations_for_transaction(mutations_stream, storage_teams, logs_by_id, acc) do
    Enum.reduce_while(mutations_stream, {:ok, acc}, fn mutation, {:ok, mutations_acc} ->
      distribute_mutation_to_logs(mutation, storage_teams, logs_by_id, mutations_acc)
    end)
  end

  @spec distribute_mutation_to_logs(
          term(),
          [StorageTeamDescriptor.t()],
          %{Log.id() => [Bedrock.range_tag()]},
          %{Log.id() => [term()]}
        ) ::
          {:cont, {:ok, %{Log.id() => [term()]}}}
          | {:halt, {:error, term()}}
  defp distribute_mutation_to_logs(mutation, storage_teams, logs_by_id, mutations_acc) do
    key_or_range = mutation_to_key_or_range(mutation)

    case key_or_range_to_tags(key_or_range, storage_teams) do
      {:ok, []} ->
        {:halt, {:error, {:storage_team_coverage_error, key_or_range}}}

      {:ok, affected_tags} ->
        affected_logs = find_logs_for_tags(affected_tags, logs_by_id)

        updated_acc =
          Enum.reduce(affected_logs, mutations_acc, fn log_id, acc_inner ->
            Map.update!(acc_inner, log_id, &[mutation | &1])
          end)

        {:cont, {:ok, updated_acc}}
    end
  end

  @doc """
  Extracts the key or range affected by a mutation using pattern matching.

  ## Parameters
    - `mutation`: A tuple representing the mutation operation

  ## Returns
    - For `{:set, key, value}` -> `key`
    - For `{:clear, key}` -> `key`
    - For `{:clear_range, start_key, end_key}` -> `{start_key, end_key}`

  ## Examples
      iex> mutation_to_key_or_range({:set, "hello", "world"})
      "hello"

      iex> mutation_to_key_or_range({:clear_range, "a", "z"})
      {"a", "z"}
  """
  @spec mutation_to_key_or_range(
          {:set, Bedrock.key(), Bedrock.value()}
          | {:clear, Bedrock.key()}
          | {:clear_range, Bedrock.key(), Bedrock.key()}
        ) ::
          Bedrock.key() | {Bedrock.key(), Bedrock.key()}
  def mutation_to_key_or_range({:set, key, _value}), do: key
  def mutation_to_key_or_range({:clear, key}), do: key
  def mutation_to_key_or_range({:clear_range, start_key, end_key}), do: {start_key, end_key}

  @doc """
  Maps a key or range to all storage team tags that are affected by it.

  For single keys, uses existing `key_to_tags/2` logic.
  For ranges, finds all storage teams that intersect with the range.

  ## Parameters
    - `key_or_range`: Either a single key or a `{start_key, end_key}` tuple
    - `storage_teams`: List of storage team descriptors

  ## Returns
    - `{:ok, [tag]}` with list of affected tags

  ## Examples
      iex> key_or_range_to_tags("hello", storage_teams)
      {:ok, [:team1]}

      iex> key_or_range_to_tags({"a", "z"}, storage_teams)
      {:ok, [:team1, :team2]}
  """
  @spec key_or_range_to_tags(Bedrock.key() | {Bedrock.key(), Bedrock.key()}, [
          StorageTeamDescriptor.t()
        ]) ::
          {:ok, [Bedrock.range_tag()]}
  def key_or_range_to_tags(key, storage_teams) when is_binary(key), do: key_to_tags(key, storage_teams)

  def key_or_range_to_tags({start_key, end_key}, storage_teams) do
    tags =
      |> Enum.filter(fn %{key_range: {team_start, team_end}} ->
        ranges_intersect?(start_key, end_key, team_start, team_end)
      end)
      |> Enum.map(fn %{tag: tag} -> tag end)

    {:ok, tags}
  end

  @doc """
  Finds all logs whose tag sets intersect with the given tags.

  ## Parameters
    - `tags`: List of storage team tags
    - `logs_by_id`: Map of log_id -> list of tags covered by that log

  ## Returns
    - List of log IDs whose tag sets intersect with the given tags

  ## Examples
      iex> find_logs_for_tags([:tag1, :tag2], %{log1: [:tag1, :tag3], log2: [:tag2, :tag4]})
      [:log1, :log2]
  """
  @spec find_logs_for_tags([Bedrock.range_tag()], %{Log.id() => [Bedrock.range_tag()]}) ::
          [Log.id()]
  def find_logs_for_tags(tags, logs_by_id) do
    tag_set = MapSet.new(tags)

    for {log_id, log_tags} <- logs_by_id, Enum.any?(log_tags, &MapSet.member?(tag_set)) do
      log_id
    end
  end

  @spec ranges_intersect?(
          Bedrock.key(),
          Bedrock.key() | :end,
          Bedrock.key(),
          Bedrock.key() | :end
        ) ::
          boolean()
  defp ranges_intersect?(_start1, :end, _start2, :end), do: true
  defp ranges_intersect?(start1, :end, _start2, end2), do: start1 < end2
  defp ranges_intersect?(_start1, end1, start2, :end), do: end1 > start2
  defp ranges_intersect?(start1, end1, start2, end2), do: start1 < end2 and end1 > start2

  defguard is_key_in_range(key, start_key, end_key) when
    key >= start_key and (end_key == :end or key < end_key)

  @doc """
  Maps a key to all storage team tags that contain it.

  Searches through storage teams to find all teams whose key ranges contain
  the given key. A key can belong to multiple overlapping teams.
  Uses lexicographic ordering where a key belongs to a team
  if it falls within [start_key, end_key) or [start_key, :end).

  ## Parameters
    - `key`: The key to map to storage teams
    - `storage_teams`: List of storage team descriptors

  ## Returns
    - `{:ok, [tag]}` with list of matching tags (may be empty)

  ## Examples
      iex> teams = [%{tag: :team1, key_range: {"a", "m"}}, %{tag: :team2, key_range: {"h", "z"}}]
      iex> key_to_tags("hello", teams)
      {:ok, [:team1, :team2]}
  """
  @spec key_to_tags(Bedrock.key(), [StorageTeamDescriptor.t()]) ::
          {:ok, [Bedrock.range_tag()]}
  def key_to_tags(key, storage_teams) do
    tags =
      for %{tag: tag, key_range: {team_start, team_end}} when is_key_in_range(key, team_start, team_end) <- storage_teams do
        tag
      end

    {:ok, tags}
  end

  @spec push_to_logs(FinalizationPlan.t(), TransactionSystemLayout.t(), keyword()) ::
          FinalizationPlan.t()
  defp push_to_logs(%FinalizationPlan{stage: :failed} = plan, _layout, _opts), do: plan

  defp push_to_logs(%FinalizationPlan{stage: :ready_for_logging} = plan, layout, opts) do
    batch_log_push_fn = Keyword.get(opts, :batch_log_push_fn, &push_transaction_to_logs_direct/5)

    case batch_log_push_fn.(
           layout,
           plan.last_commit_version,
           plan.transactions_by_log,
           plan.commit_version,
           opts
         ) do
      :ok ->
        %{plan | stage: :logged}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @spec resolve_log_descriptors(%{Log.id() => term()}, %{Log.id() => ServiceDescriptor.t()}) :: %{
          Log.id() => ServiceDescriptor.t()
        }
  def resolve_log_descriptors(log_descriptors, services) do
    Enum.reduce(log_descriptors, %{}, fn {log_id, _}, acc ->
      case services do
        %{^log_id => service_descriptor} when not is_nil(service_descriptor) ->
          Map.put(acc, log_id, service_descriptor)

        _ ->
          acc
      end
    end)
  end

  @spec try_to_push_transaction_to_log(
          ServiceDescriptor.t(),
          binary(),
          Bedrock.version()
        ) ::
          :ok | {:error, :unavailable}
  def try_to_push_transaction_to_log(%{kind: :log, status: {:up, log_server}}, transaction, last_commit_version) do
    Log.push(log_server, transaction, last_commit_version)
  end

  def try_to_push_transaction_to_log(_, _, _), do: {:error, :unavailable}

  @doc """
  Pushes transactions directly to logs and waits for acknowledgement from ALL log servers.

  This function takes transactions that have already been built per log and pushes them
  to the appropriate log servers. Each log receives its pre-built transaction.
  All logs must acknowledge to maintain durability guarantees.

  ## Parameters

    - `transaction_system_layout`: Contains configuration information about the
      transaction system, including available log servers.
    - `last_commit_version`: The last known committed version; used to
      ensure consistency in log ordering.
    - `transactions_by_log`: Map of log_id to transaction for that log.
      May be empty transactions if all transactions were aborted.
    - `commit_version`: The version assigned by the sequencer for this batch.
    - `opts`: Optional configuration for testing and customization.

  ## Options
    - `:async_stream_fn` - Function for parallel processing (default: Task.async_stream/3)
    - `:log_push_fn` - Function for pushing to individual logs (default: try_to_push_transaction_to_log/3)
    - `:timeout` - Timeout for log push operations (default: 5_000ms)

  ## Returns
    - `:ok` if acknowledgements have been received from ALL log servers.
    - `{:error, log_push_error()}` if any log has not successfully acknowledged the
       push within the timeout period or other errors occur.
  """
  @spec push_transaction_to_logs_direct(
          TransactionSystemLayout.t(),
          last_commit_version :: Bedrock.version(),
          %{Log.id() => Transaction.encoded()},
          commit_version :: Bedrock.version(),
          opts :: [
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            timeout: non_neg_integer()
          ]
        ) :: :ok | {:error, log_push_error()}
  def push_transaction_to_logs_direct(
        transaction_system_layout,
        last_commit_version,
        transactions_by_log,
        _commit_version,
        opts \\ []
      ) do
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    log_push_fn = Keyword.get(opts, :log_push_fn, &try_to_push_transaction_to_log/3)
    timeout = Keyword.get(opts, :timeout, 5_000)

    logs_by_id = transaction_system_layout.logs
    required_acknowledgments = map_size(logs_by_id)

    resolved_logs = resolve_log_descriptors(logs_by_id, transaction_system_layout.services)

    stream_result =
      async_stream_fn.(
        resolved_logs,
        fn {log_id, service_descriptor} ->
          encoded_transaction = Map.get(transactions_by_log, log_id)
          result = log_push_fn.(service_descriptor, encoded_transaction, last_commit_version)
          {log_id, result}
        end,
        timeout: timeout
      )

    stream_result
    |> Enum.reduce_while({0, []}, fn
      {:ok, {log_id, {:error, reason}}}, {_count, errors} ->
        {:halt, {:error, [{log_id, reason} | errors]}}

      {:ok, {_log_id, :ok}}, {count, errors} ->
        count = 1 + count

        if count == required_acknowledgments do
          {:halt, {:ok, count}}
        else
          {:cont, {count, errors}}
        end

      {:exit, {log_id, reason}}, {_count, errors} ->
        {:halt, {:error, [{log_id, reason} | errors]}}
    end)
    |> case do
      {:ok, ^required_acknowledgments} ->
        :ok

      {:error, errors} ->
        {:error, {:log_failures, errors}}

      {count, errors} when count < required_acknowledgments ->
        {:error, {:insufficient_acknowledgments, count, required_acknowledgments, errors}}

      _other ->
        {:error, :log_push_failed}
    end
  end

  @spec notify_sequencer(FinalizationPlan.t(), Sequencer.ref()) ::
          FinalizationPlan.t()
  defp notify_sequencer(%FinalizationPlan{stage: :failed} = plan, _sequencer), do: plan

  defp notify_sequencer(%FinalizationPlan{stage: :logged} = plan, sequencer) do
    :ok = Sequencer.report_successful_commit(sequencer, plan.commit_version)
    %{plan | stage: :sequencer_notified}
  end

  @spec notify_successes(FinalizationPlan.t(), keyword()) :: FinalizationPlan.t()
  defp notify_successes(%FinalizationPlan{stage: :failed} = plan, _opts), do: plan

  defp notify_successes(%FinalizationPlan{stage: :sequencer_notified} = plan, opts) do
    success_reply_fn = Keyword.get(opts, :success_reply_fn, &send_reply_with_commit_version/2)

    successful_indices = get_successful_indices(plan)
    unreplied_success_indices = MapSet.difference(successful_indices, plan.replied_indices)

    unreplied_successes =
      filter_replies_by_indices(plan.successful_replies, unreplied_success_indices)

    success_reply_fn.(unreplied_successes, plan.commit_version)

    updated_replied_indices = MapSet.union(plan.replied_indices, successful_indices)

    %{plan | replied_indices: updated_replied_indices, stage: :completed}
  end

  @spec send_reply_with_commit_version([Batch.reply_fn()], Bedrock.version()) ::
          :ok
  def send_reply_with_commit_version(oks, commit_version), do: Enum.each(oks, & &1.({:ok, commit_version}))

  @spec get_successful_indices(FinalizationPlan.t()) :: MapSet.t()
  defp get_successful_indices(plan) do
    aborted_set = MapSet.new(plan.aborted_indices)
    transaction_count = length(plan.transactions)

    if transaction_count > 0 do
      all_indices = MapSet.new(0..(transaction_count - 1))
      MapSet.difference(all_indices, aborted_set)
    else
      MapSet.new()
    end
  end

  @spec filter_replies_by_indices([Batch.reply_fn()], MapSet.t()) :: [Batch.reply_fn()]
  defp filter_replies_by_indices(replies, indices_set) do
    replies
    |> Enum.with_index()
    |> Enum.filter(fn {_reply_fn, idx} -> MapSet.member?(indices_set, idx) end)
    |> Enum.map(fn {reply_fn, _idx} -> reply_fn end)
  end

  @spec extract_result_or_handle_error(FinalizationPlan.t(), keyword()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | {:error, finalization_error()}
  defp extract_result_or_handle_error(%FinalizationPlan{stage: :completed} = plan, _opts) do
    n_aborts = length(plan.aborted_replies)
    n_successes = length(plan.successful_replies)
    {:ok, n_aborts, n_successes}
  end

  defp extract_result_or_handle_error(%FinalizationPlan{stage: :failed} = plan, opts) do
    handle_error(plan, opts)
  end

  @spec handle_error(FinalizationPlan.t(), keyword()) :: {:error, finalization_error()}
  defp handle_error(%FinalizationPlan{error: error} = plan, opts) when not is_nil(error) do
    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    all_indices = MapSet.new(0..(length(plan.transactions) - 1))
    unreplied_indices = MapSet.difference(all_indices, plan.replied_indices)

    unreplied_replies =
      plan.transactions
      |> Enum.with_index()
      |> Enum.filter(fn {_transaction, idx} -> MapSet.member?(unreplied_indices, idx) end)
      |> Enum.map(fn {{reply_fn, _transaction}, _idx} -> reply_fn end)

    abort_reply_fn.(unreplied_replies)

    {:error, plan.error}
  end
end
