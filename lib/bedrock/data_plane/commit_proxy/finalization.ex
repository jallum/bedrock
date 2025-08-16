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

  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Sequencer

  import Bedrock.DataPlane.CommitProxy.Batch,
    only: [transactions_in_order: 1]

  import Bitwise, only: [<<<: 2]

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
                                  Bedrock.range_tag() => BedrockTransaction.encoded()
                                },
                                commit_version :: Bedrock.version(),
                                opts :: [
                                  timeout: Bedrock.timeout_in_ms(),
                                  async_stream_fn: async_stream_fn()
                                ] ->
                                  :ok | {:error, log_push_error()})

  @type log_push_single_fn() :: (ServiceDescriptor.t(), binary(), Bedrock.version() ->
                                   :ok | {:error, :unavailable})

  @type async_stream_fn() :: (enumerable :: Enumerable.t(),
                              fun :: (term() -> term()),
                              opts :: keyword() ->
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
          | {:insufficient_acknowledgments, non_neg_integer(), non_neg_integer(),
             [{Log.id(), term()}]}
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

    @enforce_keys [:transactions, :commit_version, :last_commit_version, :storage_teams]
    defstruct [
      :transactions,
      :commit_version,
      :last_commit_version,
      :storage_teams,

      # Accumulated pipeline state
      resolver_data: [],
      aborted_indices: [],
      aborted_replies: [],
      successful_replies: [],
      transactions_by_tag: %{},

      # Reply tracking for safety
      replied_indices: MapSet.new(),

      # Pipeline stage status
      stage: :initialized,
      error: nil
    ]

    @type t :: %__MODULE__{
            transactions: [{Batch.reply_fn(), binary()}],
            commit_version: Bedrock.version(),
            last_commit_version: Bedrock.version(),
            storage_teams: [StorageTeamDescriptor.t()],
            resolver_data: [Resolver.transaction_summary()],
            aborted_indices: [integer()],
            aborted_replies: [Batch.reply_fn()],
            successful_replies: [Batch.reply_fn()],
            transactions_by_tag: %{Bedrock.range_tag() => BedrockTransaction.encoded()},
            replied_indices: MapSet.t(),
            stage: atom(),
            error: term() | nil
          }
  end

  # ============================================================================
  # MAIN PIPELINE FUNCTION
  # ============================================================================

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

  # ============================================================================
  # STEP 1: CREATE FINALIZATION PLAN
  # ============================================================================

  @spec create_finalization_plan(Batch.t(), TransactionSystemLayout.t()) :: FinalizationPlan.t()
  defp create_finalization_plan(batch, transaction_system_layout) do
    # Batch now consistently stores binary format transactions
    %FinalizationPlan{
      transactions: transactions_in_order(batch),
      commit_version: batch.commit_version,
      last_commit_version: batch.last_commit_version,
      storage_teams: transaction_system_layout.storage_teams,
      stage: :created
    }
  end

  # ============================================================================
  # STEP 2: PREPARE FOR RESOLUTION
  # ============================================================================

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
    transactions
    |> Enum.map(&transform_single_transaction_for_resolution/1)
  end

  # Helper function to transform a single transaction - this breaks up the complex
  # case analysis to help dialyzer understand the type flow better
  @spec transform_single_transaction_for_resolution(
          {Batch.reply_fn(), Bedrock.transaction() | binary()}
        ) ::
          Resolver.transaction_summary()
  defp transform_single_transaction_for_resolution({_reply_fn, transaction})
       when is_map(transaction) do
    # Extract data directly from the transaction map (BedrockTransaction format)
    read_version = Map.get(transaction, :read_version)
    read_conflicts = Map.get(transaction, :read_conflicts, [])
    write_conflicts = Map.get(transaction, :write_conflicts, [])

    transform_version_and_conflicts(read_version, read_conflicts, write_conflicts)
  end

  defp transform_single_transaction_for_resolution({_reply_fn, binary_transaction})
       when is_binary(binary_transaction) do
    # Extract data from binary encoded transaction (for backward compatibility)
    {:ok, read_version} = BedrockTransaction.extract_read_version(binary_transaction)

    {:ok, {_read_version, read_conflicts}} =
      BedrockTransaction.extract_read_conflicts(binary_transaction)

    {:ok, write_conflicts} = BedrockTransaction.extract_write_conflicts(binary_transaction)

    transform_version_and_conflicts(read_version, read_conflicts, write_conflicts)
  end

  # This helper function makes the pattern match more explicit for dialyzer
  @spec transform_version_and_conflicts(nil | Bedrock.version(), [Bedrock.key()], [Bedrock.key()]) ::
          Resolver.transaction_summary()
  defp transform_version_and_conflicts(nil, _read_conflicts, write_conflicts) do
    # No read version - resolver can optimize by skipping read conflict checks
    {nil, write_conflicts}
  end

  defp transform_version_and_conflicts(version, read_conflicts, write_conflicts) do
    # Send read version and conflicts for resolution
    {{version, read_conflicts |> Enum.uniq()}, write_conflicts}
  end

  # ============================================================================
  # STEP 3: RESOLVE CONFLICTS
  # ============================================================================

  @spec resolve_conflicts(FinalizationPlan.t(), TransactionSystemLayout.t(), keyword()) ::
          FinalizationPlan.t()
  defp resolve_conflicts(%FinalizationPlan{stage: :ready_for_resolution} = plan, layout, opts) do
    resolver_fn = Keyword.get(opts, :resolver_fn, &resolve_transactions/5)

    case resolver_fn.(
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
  def resolve_transactions(
        resolvers,
        last_version,
        commit_version,
        transaction_summaries,
        opts
      ) do
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

    result =
      resolvers
      |> Enum.map(fn {start_key, ref} ->
        Resolver.resolve_transactions(
          ref,
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
        # Emit telemetry for retry attempt (after this failure, before next retry)
        :telemetry.execute(
          [:bedrock, :commit_proxy, :resolver, :retry],
          %{attempts_remaining: attempts_remaining - 1, attempts_used: attempts_used + 1},
          %{reason: reason}
        )

        # Retry with updated attempt counters
        updated_opts =
          opts
          |> Keyword.put(:attempts_remaining, attempts_remaining - 1)
          |> Keyword.put(:attempts_used, attempts_used + 1)

        resolve_transactions(
          resolvers,
          last_version,
          commit_version,
          transaction_summaries,
          updated_opts
        )

      {:error, reason} ->
        # Emit telemetry for final failure
        :telemetry.execute(
          [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded],
          %{total_attempts: attempts_used + 1},
          %{reason: reason}
        )

        # Max retries exceeded, return error to allow commit proxy to trigger recovery
        {:error, {:resolver_unavailable, reason}}
    end
  end

  @spec default_timeout_fn(non_neg_integer()) :: non_neg_integer()
  def default_timeout_fn(attempts_used), do: 500 * (1 <<< attempts_used)

  # Helper functions for resolve_conflicts step

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
  defp filter_transaction_summary({nil, writes}, filter_fn),
    do: {nil, Enum.filter(writes, filter_fn)}

  defp filter_transaction_summary({{read_version, reads}, writes}, filter_fn),
    do: {{read_version, Enum.filter(reads, filter_fn)}, Enum.filter(writes, filter_fn)}

  # ============================================================================
  # STEP 4: SPLIT AND NOTIFY ABORTS
  # ============================================================================

  @spec split_and_notify_aborts(FinalizationPlan.t(), keyword()) :: FinalizationPlan.t()
  defp split_and_notify_aborts(%FinalizationPlan{stage: :failed} = plan, _opts), do: plan

  defp split_and_notify_aborts(%FinalizationPlan{stage: :conflicts_resolved} = plan, opts) do
    abort_reply_fn =
      Keyword.get(opts, :abort_reply_fn, &reply_to_all_clients_with_aborted_transactions/1)

    {aborted_replies, successful_replies, aborted_indices_set} =
      split_transactions_by_abort_status(plan)

    new_aborted_indices = MapSet.difference(aborted_indices_set, plan.replied_indices)

    new_aborted_replies =
      plan.transactions
      |> Enum.with_index()
      |> Enum.filter(fn {_transaction, idx} -> MapSet.member?(new_aborted_indices, idx) end)
      |> Enum.map(fn {{reply_fn, _transaction}, _idx} -> reply_fn end)

    abort_reply_fn.(new_aborted_replies)

    updated_replied_indices = MapSet.union(plan.replied_indices, aborted_indices_set)

    %{
      plan
      | aborted_replies: aborted_replies,
        successful_replies: successful_replies,
        replied_indices: updated_replied_indices,
        stage: :aborts_notified
    }
  end

  @spec reply_to_all_clients_with_aborted_transactions([Batch.reply_fn()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions([]), do: :ok

  def reply_to_all_clients_with_aborted_transactions(aborts),
    do: Enum.each(aborts, & &1.({:error, :aborted}))

  # Helper functions for split_and_notify_aborts step

  @spec split_transactions_by_abort_status(FinalizationPlan.t()) ::
          {[Batch.reply_fn()], [Batch.reply_fn()], MapSet.t()}
  defp split_transactions_by_abort_status(plan) do
    aborted_set = MapSet.new(plan.aborted_indices)

    {aborted_replies, successful_replies} =
      plan.transactions
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {{reply_fn, _transaction}, idx}, {aborts_acc, success_acc} ->
        if MapSet.member?(aborted_set, idx) do
          {[reply_fn | aborts_acc], success_acc}
        else
          {aborts_acc, [reply_fn | success_acc]}
        end
      end)

    {Enum.reverse(aborted_replies), Enum.reverse(successful_replies), aborted_set}
  end

  # ============================================================================
  # STEP 5: PREPARE FOR LOGGING
  # ============================================================================

  @spec prepare_for_logging(FinalizationPlan.t()) :: FinalizationPlan.t()
  defp prepare_for_logging(%FinalizationPlan{stage: :failed} = plan), do: plan

  defp prepare_for_logging(%FinalizationPlan{stage: :aborts_notified} = plan) do
    case group_successful_transactions_by_tag(plan) do
      {:ok, transactions_by_tag} ->
        %{plan | transactions_by_tag: transactions_by_tag, stage: :ready_for_logging}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  # Helper functions for prepare_for_logging step

  @spec group_successful_transactions_by_tag(FinalizationPlan.t()) ::
          {:ok, %{Bedrock.range_tag() => BedrockTransaction.encoded()}} | {:error, term()}
  defp group_successful_transactions_by_tag(plan) do
    aborted_set = MapSet.new(plan.aborted_indices)

    with {:ok, combined_mutations_by_tag} <-
           plan.transactions
           |> Enum.with_index()
           |> Enum.reduce_while({:ok, %{}}, fn transaction_with_idx, {:ok, acc} ->
             process_transaction_for_grouping(
               transaction_with_idx,
               aborted_set,
               plan.storage_teams,
               acc
             )
           end) do
      result =
        combined_mutations_by_tag
        |> Enum.map(fn {tag, writes} ->
          # Convert writes to mutations
          mutations =
            Enum.map(writes, fn
              {key, nil} -> {:clear_range, key, key <> <<0>>}
              {key, value} -> {:set, key, value}
            end)

          # Create transaction with commit version
          encoded = BedrockTransaction.encode(%{mutations: mutations})

          {:ok, with_version} =
            BedrockTransaction.add_commit_version(encoded, plan.commit_version)

          {tag, with_version}
        end)
        |> Map.new()

      {:ok, result}
    end
  end

  @spec process_transaction_for_grouping(
          {{function(), binary()}, non_neg_integer()},
          MapSet.t(non_neg_integer()),
          [StorageTeamDescriptor.t()],
          %{Bedrock.range_tag() => %{Bedrock.key() => Bedrock.value() | nil}}
        ) ::
          {:cont, {:ok, %{Bedrock.range_tag() => %{Bedrock.key() => Bedrock.value() | nil}}}}
          | {:halt, {:error, term()}}
  defp process_transaction_for_grouping(
         {{_reply_fn, binary_transaction}, idx},
         aborted_set,
         storage_teams,
         acc
       ) do
    if MapSet.member?(aborted_set, idx) do
      {:cont, {:ok, acc}}
    else
      # Extract actual mutations from binary transaction
      case extract_mutations_as_key_values(binary_transaction) do
        {:ok, mutations} ->
          handle_non_aborted_transaction(mutations, storage_teams, acc)

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end
  end

  # Extracts mutations from a BedrockTransaction binary and converts them to key-value format.
  #
  # Returns %{key => value} map, handling:
  # - SET operations: key => value
  # - CLEAR operations: key => nil
  # - CLEAR_RANGE operations: {start_key, end_key} => nil
  @spec extract_mutations_as_key_values(binary()) ::
          {:ok, %{Bedrock.key() => Bedrock.value() | nil}} | {:error, term()}
  defp extract_mutations_as_key_values(binary_transaction) do
    case BedrockTransaction.stream_mutations(binary_transaction) do
      {:ok, mutations_stream} ->
        mutations_map =
          mutations_stream
          |> Enum.reduce(%{}, fn
            {:set, key, value}, acc ->
              Map.put(acc, key, value)

            {:clear, key}, acc ->
              Map.put(acc, key, nil)

            {:clear_range, start_key, end_key}, acc ->
              Map.put(acc, {start_key, end_key}, nil)
          end)

        {:ok, mutations_map}

      {:error, reason} ->
        {:error, {:mutation_extraction_failed, reason}}
    end
  end

  @spec handle_non_aborted_transaction(
          %{Bedrock.key() => Bedrock.value() | nil},
          [StorageTeamDescriptor.t()],
          %{Bedrock.range_tag() => %{Bedrock.key() => Bedrock.value() | nil}}
        ) ::
          {:cont, {:ok, %{Bedrock.range_tag() => %{Bedrock.key() => Bedrock.value() | nil}}}}
          | {:halt, {:error, term()}}
  defp handle_non_aborted_transaction(mutations, storage_teams, acc) do
    case group_writes_by_tag(mutations, storage_teams) do
      {:ok, tag_grouped_writes} ->
        {:cont, {:ok, merge_writes_by_tag(acc, tag_grouped_writes)}}

      {:error, _reason} = error ->
        {:halt, error}
    end
  end

  @doc """
  Groups a map of writes by their target storage team tags.

  For each key-value pair in the writes map, determines which storage team
  tags the key belongs to and groups the writes accordingly. A single write
  can be distributed to multiple tags if storage teams overlap.

  ## Parameters
    - `writes`: Map of key -> value pairs to be written
    - `storage_teams`: List of storage team descriptors for tag mapping

  ## Returns
    - Map of tag -> %{key => value} for writes belonging to each tag

  ## Failure Behavior
    - Returns `{:error, {:storage_team_coverage_error, key}}` if any key doesn't
      match any storage team. This indicates a critical configuration error
      where storage teams don't cover the full keyspace, requiring recovery.
  """
  @spec group_writes_by_tag(%{Bedrock.key() => term()}, [StorageTeamDescriptor.t()]) ::
          {:ok, %{Bedrock.range_tag() => %{Bedrock.key() => term()}}}
          | {:error, storage_coverage_error()}
  def group_writes_by_tag(writes, storage_teams) do
    result =
      writes
      |> Enum.reduce_while({:ok, %{}}, fn {key, value}, {:ok, acc} ->
        case key_to_tags(key, storage_teams) do
          {:ok, []} ->
            {:halt, {:error, {:storage_team_coverage_error, key}}}

          {:ok, tags} ->
            # Distribute this write to all matching tags
            updated_acc = distribute_write_to_tags(tags, acc, key, value)
            {:cont, {:ok, updated_acc}}
        end
      end)

    result
  end

  defp distribute_write_to_tags(tags, acc, key, value) do
    Enum.reduce(tags, acc, fn tag, acc_inner ->
      Map.update(acc_inner, tag, %{key => value}, &Map.put(&1, key, value))
    end)
  end

  @doc """
  Merges two maps of writes grouped by tag.

  Takes two maps where keys are tags and values are write maps,
  and merges the write maps for each tag.

  ## Parameters
    - `acc`: Accumulator map of tag -> writes
    - `new_writes`: New writes map to merge

  ## Returns
    - Merged map of tag -> combined writes
  """
  @spec merge_writes_by_tag(
          %{Bedrock.range_tag() => %{Bedrock.key() => term()}},
          %{Bedrock.range_tag() => %{Bedrock.key() => term()}}
        ) :: %{Bedrock.range_tag() => %{Bedrock.key() => term()}}
  def merge_writes_by_tag(acc, new_writes) do
    Map.merge(acc, new_writes, fn _tag, existing_writes, new_writes ->
      Map.merge(existing_writes, new_writes)
    end)
  end

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
      storage_teams
      |> Enum.filter(fn %{key_range: {start_key, end_key}} ->
        key_in_range?(key, start_key, end_key)
      end)
      |> Enum.map(fn %{tag: tag} -> tag end)

    {:ok, tags}
  end

  @doc """
  Maps a key to its corresponding storage team tag (legacy function).

  Searches through storage teams to find which team's key range contains
  the given key. Uses lexicographic ordering where a key belongs to a team
  if it falls within [start_key, end_key) or [start_key, :end).

  This function returns the first matching tag for backward compatibility.
  Consider using `key_to_tags/2` for multi-tag support.

  ## Parameters
    - `key`: The key to map to a storage team
    - `storage_teams`: List of storage team descriptors

  ## Returns
    - `{:ok, tag}` if a matching storage team is found
    - `{:error, :no_matching_team}` if no team covers the key

  ## Examples
      iex> teams = [%{tag: :team1, key_range: {"a", "m"}}, %{tag: :team2, key_range: {"m", :end}}]
      iex> key_to_tag("hello", teams)
      {:ok, :team1}
  """
  @spec key_to_tag(Bedrock.key(), [StorageTeamDescriptor.t()]) ::
          {:ok, Bedrock.range_tag()} | {:error, :no_matching_team}
  def key_to_tag(key, storage_teams) do
    case key_to_tags(key, storage_teams) do
      {:ok, [tag | _]} -> {:ok, tag}
      {:ok, []} -> {:error, :no_matching_team}
    end
  end

  @spec key_in_range?(Bedrock.key(), Bedrock.key(), Bedrock.key() | :end) :: boolean()
  defp key_in_range?(key, start_key, :end), do: key >= start_key
  defp key_in_range?(key, start_key, end_key), do: key >= start_key and key < end_key

  # Converts key-value writes format to BedrockTransaction mutations format.
  #
  # Transforms %{key => value} map to [{:set, key, value}] list of mutations.
  # Handles special cases:
  # - `nil` values become `{:clear, key}` mutations

  # ============================================================================
  # STEP 6: PUSH TO LOGS
  # ============================================================================

  @spec push_to_logs(FinalizationPlan.t(), TransactionSystemLayout.t(), keyword()) ::
          FinalizationPlan.t()
  defp push_to_logs(%FinalizationPlan{stage: :failed} = plan, _layout, _opts), do: plan

  defp push_to_logs(%FinalizationPlan{stage: :ready_for_logging} = plan, layout, opts) do
    batch_log_push_fn = Keyword.get(opts, :batch_log_push_fn, &push_transaction_to_logs/5)

    case batch_log_push_fn.(
           layout,
           plan.last_commit_version,
           plan.transactions_by_tag,
           plan.commit_version,
           opts
         ) do
      :ok ->
        %{plan | stage: :logged}

      {:error, reason} ->
        %{plan | error: reason, stage: :failed}
    end
  end

  @doc """
  Builds the transaction that each log should receive based on tag intersection.

  Each log receives a transaction containing writes for all tags it covers.
  This supports overlapping storage teams where a single write may be included
  in multiple logs if there is tag intersection between the write's tags and
  the log's covered tags. Logs that don't cover any tags in the transaction 
  get an empty transaction to maintain version consistency.

  ## Parameters
    - `logs_by_id`: Map of log_id -> list of tags covered by that log
    - `transactions_by_tag`: Map of tag -> transaction shard for that tag
    - `commit_version`: The commit version for empty transactions

  ## Returns
    - Map of log_id -> transaction that log should receive

  ## Examples
      iex> logs_by_id = %{log1: [:tag1, :tag2], log2: [:tag2, :tag3]}
      iex> transactions_by_tag = %{tag1: transaction1, tag2: transaction2, tag3: transaction3}
      iex> build_log_transactions(logs_by_id, transactions_by_tag, 42)
      %{log1: combined_transaction_with_tag1_and_tag2, log2: combined_transaction_with_tag2_and_tag3}
  """
  @spec build_log_transactions(
          %{Log.id() => [Bedrock.range_tag()]},
          %{Bedrock.range_tag() => BedrockTransaction.encoded()},
          Bedrock.version()
        ) :: %{Log.id() => BedrockTransaction.encoded()}
  def build_log_transactions(logs_by_id, transactions_by_tag, commit_version) do
    logs_by_id
    |> Enum.map(fn {log_id, tags_covered} ->
      # Collect all mutations for all tags this log covers
      combined_mutations =
        tags_covered
        |> Enum.filter(&Map.has_key?(transactions_by_tag, &1))
        |> Enum.flat_map(fn tag ->
          transaction = Map.get(transactions_by_tag, tag)
          {:ok, mutations_stream} = BedrockTransaction.stream_mutations(transaction)
          Enum.to_list(mutations_stream)
        end)

      # Create transaction with combined mutations
      encoded = BedrockTransaction.encode(%{mutations: combined_mutations})
      {:ok, transaction} = BedrockTransaction.add_commit_version(encoded, commit_version)
      {log_id, transaction}
    end)
    |> Map.new()
  end

  @spec resolve_log_descriptors(%{Log.id() => term()}, %{term() => ServiceDescriptor.t()}) :: %{
          Log.id() => ServiceDescriptor.t()
        }
  def resolve_log_descriptors(log_descriptors, services) do
    log_descriptors
    |> Map.keys()
    |> Enum.map(&{&1, Map.get(services, &1)})
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Map.new()
  end

  @spec try_to_push_transaction_to_log(
          ServiceDescriptor.t(),
          binary(),
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

  @doc """
  Pushes transaction shards to logs based on tag coverage and waits for
  acknowledgement from ALL log servers.

  This function takes transaction shards grouped by storage team tags and
  routes them efficiently to logs. Each log receives only the transaction
  shards for tags it covers, plus empty transactions for version consistency.
  All logs must acknowledge to maintain durability guarantees.

  ## Parameters

    - `transaction_system_layout`: Contains configuration information about the
      transaction system, including available log servers and their tag coverage.
    - `last_commit_version`: The last known committed version; used to
      ensure consistency in log ordering.
    - `transactions_by_tag`: Map of storage team tag to transaction shard.
      May be empty if all transactions were aborted.
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
  @spec push_transaction_to_logs(
          TransactionSystemLayout.t(),
          last_commit_version :: Bedrock.version(),
          %{Bedrock.range_tag() => BedrockTransaction.encoded()},
          commit_version :: Bedrock.version(),
          opts :: [
            async_stream_fn: async_stream_fn(),
            log_push_fn: log_push_single_fn(),
            timeout: non_neg_integer()
          ]
        ) :: :ok | {:error, log_push_error()}
  def push_transaction_to_logs(
        transaction_system_layout,
        last_commit_version,
        transactions_by_tag,
        commit_version,
        opts \\ []
      ) do
    # Extract configurable functions for testability
    async_stream_fn = Keyword.get(opts, :async_stream_fn, &Task.async_stream/3)
    log_push_fn = Keyword.get(opts, :log_push_fn, &try_to_push_transaction_to_log/3)
    timeout = Keyword.get(opts, :timeout, 5_000)

    logs_by_id = transaction_system_layout.logs
    required_acknowledgments = map_size(logs_by_id)

    # Build the transaction each log should receive
    log_transactions = build_log_transactions(logs_by_id, transactions_by_tag, commit_version)
    resolved_logs = resolve_log_descriptors(logs_by_id, transaction_system_layout.services)

    # Use configurable async stream function
    stream_result =
      async_stream_fn.(
        resolved_logs,
        fn {log_id, service_descriptor} ->
          encoded_transaction = Map.get(log_transactions, log_id)

          # Transaction is already fully encoded with commit version by build_log_transactions
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

  # ============================================================================
  # STEP 7: NOTIFY SEQUENCER
  # ============================================================================

  @spec notify_sequencer(FinalizationPlan.t(), Bedrock.DataPlane.Sequencer.ref()) ::
          FinalizationPlan.t()
  defp notify_sequencer(%FinalizationPlan{stage: :failed} = plan, _sequencer), do: plan

  defp notify_sequencer(%FinalizationPlan{stage: :logged} = plan, sequencer) do
    :ok = Sequencer.report_successful_commit(sequencer, plan.commit_version)
    %{plan | stage: :sequencer_notified}
  end

  # ============================================================================
  # STEP 8: NOTIFY SUCCESSES
  # ============================================================================

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
  def send_reply_with_commit_version(oks, commit_version),
    do: Enum.each(oks, & &1.({:ok, commit_version}))

  # Helper functions for notify_successes step

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

  # ============================================================================
  # STEP 8: EXTRACT RESULT OR HANDLE ERROR
  # ============================================================================

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

  # Error recovery: safely abort all unreplied transactions
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
