defmodule Bedrock.DataPlane.Resolver.Recovery do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Log.Transaction

  @spec recover_from(
          State.t(),
          Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version_vector()
        ) ::
          {:ok, State.t()} | {:error, {:failed_to_pull_log, Log.id(), reason :: term()}}
  def recover_from(t, _, _, _) when t.mode != :locked,
    do: {:error, :lock_required}

  def recover_from(t, source_logs, first_version, last_version) do
    Enum.reduce_while(source_logs, t, fn {log_id, log_ref}, t ->
      case pull_transactions(t.tree, log_ref, first_version, last_version) do
        {:ok, tree} ->
          {:cont, %{t | tree: tree}}

        {:error, reason} ->
          {:halt, {:error, {:failed_to_pull_log, log_id, reason}}}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      %{} = t -> {:ok, %{t | last_version: last_version, oldest_version: first_version}}
    end
  end

  @spec pull_transactions(
          tree :: Tree.t() | nil,
          log_to_pull :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, Tree.t()}
          | Log.pull_errors()
          | {:error, {:log_unavailable, log_to_pull :: Log.ref()}}
  def pull_transactions(tree, nil, 0, 0),
    do: {:ok, tree}

  def pull_transactions(tree, _, first_version, last_version)
      when first_version == last_version,
      do: {:ok, tree}

  def pull_transactions(tree, log_to_pull, first_version, last_version) do
    case Log.pull(log_to_pull, first_version, last_version: last_version) do
      {:ok, []} ->
        {:ok, tree}

      {:ok, transactions} ->
        {tree, last_version_in_batch} = apply_batch_of_transactions(tree, transactions)
        pull_transactions(tree, log_to_pull, last_version_in_batch, last_version)

      {:error, :unavailable} ->
        {:error, {:log_unavailable, log_to_pull}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def apply_batch_of_transactions(tree, transactions) do
    transactions
    |> Enum.reduce(
      {tree, nil},
      fn transaction, {tree, _last_version} ->
        apply_transaction(tree, transaction)
      end
    )
  end

  @spec apply_transaction(Tree.t(), Transaction.t()) :: {Tree.t(), Bedrock.version()}
  def apply_transaction(tree, {write_version, writes}) do
    {writes |> Enum.reduce(tree, &Tree.insert(&2, &1, write_version)), write_version}
  end
end
