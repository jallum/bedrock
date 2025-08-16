defmodule FinalizationTestSupport do
  @moduledoc """
  Shared test utilities and fixtures for finalization tests.
  """

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Version

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false

    def name, do: "test_cluster"

    def otp_name(component) when is_atom(component) do
      :"test_cluster_#{component}"
    end
  end

  @doc """
  Creates a mock log server that responds to GenServer calls.
  Automatically registers cleanup via on_exit to ensure the process is killed.
  """
  def create_mock_log_server do
    pid =
      spawn(fn ->
        receive do
          {:"$gen_call", from, {:push, _transaction, _last_version}} ->
            GenServer.reply(from, :ok)
        after
          5000 -> :timeout
        end
      end)

    ensure_process_killed(pid)
    pid
  end

  @doc """
  Creates a basic transaction system layout for testing.
  """
  def basic_transaction_system_layout(log_server) do
    %{
      sequencer: :test_sequencer,
      resolvers: [{<<0>>, :test_resolver}],
      logs: %{"log_1" => [0, 1]},
      services: %{
        "log_1" => %{kind: :log, status: {:up, log_server}}
      },
      storage_teams: [
        %{tag: 0, key_range: {<<>>, :end}, storage_ids: ["storage_1"]}
      ]
    }
  end

  @doc """
  Creates a multi-log transaction system layout for testing.
  """
  def multi_log_transaction_system_layout do
    %{
      logs: %{
        "log_1" => [0],
        "log_2" => [1],
        "log_3" => [2]
      },
      services: %{
        "log_1" => %{kind: :log, status: {:up, self()}},
        "log_2" => %{kind: :log, status: {:up, self()}},
        "log_3" => %{kind: :log, status: {:up, self()}}
      }
    }
  end

  @doc """
  Creates sample storage teams configuration for testing.
  """
  def sample_storage_teams do
    [
      %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
      %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
      %{tag: 2, key_range: {<<"z">>, :end}, storage_ids: ["storage_3"]}
    ]
  end

  @doc """
  Creates a test batch with given parameters.
  """
  def create_test_batch(commit_version, last_commit_version, transactions \\ []) do
    # Ensure versions are in proper Bedrock.version() binary format
    commit_version =
      if is_integer(commit_version),
        do: Version.from_integer(commit_version),
        else: commit_version

    last_commit_version =
      if is_integer(last_commit_version),
        do: Version.from_integer(last_commit_version),
        else: last_commit_version

    # Create binary transaction using BedrockTransaction encoding
    default_transaction_map = %{
      mutations: [{:set, <<"key1">>, <<"value1">>}],
      write_conflicts: [{<<"key1">>, <<"key1\0">>}],
      read_conflicts: [],
      read_version: nil
    }

    default_binary = BedrockTransaction.encode(default_transaction_map)

    default_transactions = [
      {fn result -> send(self(), {:reply, result}) end, default_binary}
    ]

    buffer = if Enum.empty?(transactions), do: default_transactions, else: transactions

    %Bedrock.DataPlane.CommitProxy.Batch{
      commit_version: commit_version,
      last_commit_version: last_commit_version,
      n_transactions: length(buffer),
      buffer: buffer
    }
  end

  @doc """
  Creates an all_logs_reached callback for testing.
  """
  def create_all_logs_reached_callback(test_pid \\ nil) do
    target_pid = test_pid || self()

    fn version ->
      send(target_pid, {:all_logs_reached, version})
      :ok
    end
  end

  @doc """
  Ensures a process is killed on test exit.
  """
  def ensure_process_killed(pid) when is_pid(pid) do
    ExUnit.Callbacks.on_exit(fn ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
    end)
  end

  @doc """
  Creates a mock async stream function that simulates log responses.
  """
  def mock_async_stream_with_responses(responses) do
    fn logs, _fun, _opts ->
      logs
      |> Enum.map(fn {log_id, _service_descriptor} ->
        process_log_response(log_id, responses)
      end)
    end
  end

  defp process_log_response(log_id, responses) do
    case Map.get(responses, log_id) do
      :ok -> {:ok, {log_id, :ok}}
      {:error, reason} -> {:ok, {log_id, {:error, reason}}}
      # Default to success
      nil -> {:ok, {log_id, :ok}}
    end
  end
end
