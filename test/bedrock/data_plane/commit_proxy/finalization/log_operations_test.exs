defmodule Bedrock.DataPlane.CommitProxy.FinalizationLogOperationsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Version
  alias FinalizationTestSupport, as: Support

  describe "build_log_transactions/3" do
    test "builds transaction for each log based on tag coverage" do
      log_descriptors = %{
        # covers tags 0 and 1
        "log_1" => [0, 1],
        # covers tags 1 and 2
        "log_2" => [1, 2],
        # covers tag 3 only
        "log_3" => [3]
      }

      transactions_by_tag = %{
        0 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_0" => "value_0"}),
        1 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_1" => "value_1"}),
        2 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_2" => "value_2"})
      }

      result =
        Finalization.build_log_transactions(
          log_descriptors,
          transactions_by_tag,
          Version.from_integer(100)
        )

      # log_1 should get writes for tags 0 and 1
      log_1_writes = BedrockTransactionTestSupport.extract_log_writes(result["log_1"])
      assert log_1_writes == %{"key_0" => "value_0", "key_1" => "value_1"}

      # log_2 should get writes for tags 1 and 2
      log_2_writes = BedrockTransactionTestSupport.extract_log_writes(result["log_2"])
      assert log_2_writes == %{"key_1" => "value_1", "key_2" => "value_2"}

      # log_3 should get empty transaction (no matching tags)
      log_3_writes = BedrockTransactionTestSupport.extract_log_writes(result["log_3"])
      assert log_3_writes == %{}

      # All transactions should have same version
      assert BedrockTransactionTestSupport.extract_log_version(result["log_1"]) ==
               Version.from_integer(100)

      assert BedrockTransactionTestSupport.extract_log_version(result["log_2"]) ==
               Version.from_integer(100)

      assert BedrockTransactionTestSupport.extract_log_version(result["log_3"]) ==
               Version.from_integer(100)
    end

    test "handles case where no tags match any logs" do
      log_descriptors = %{
        # tags that don't exist in transactions
        "log_1" => [10, 11]
      }

      transactions_by_tag = %{
        0 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_0" => "value_0"})
      }

      result =
        Finalization.build_log_transactions(
          log_descriptors,
          transactions_by_tag,
          Version.from_integer(100)
        )

      # log_1 should get empty transaction
      log_1_writes = BedrockTransactionTestSupport.extract_log_writes(result["log_1"])
      assert log_1_writes == %{}

      assert BedrockTransactionTestSupport.extract_log_version(result["log_1"]) ==
               Version.from_integer(100)
    end

    test "handles empty transactions_by_tag" do
      log_descriptors = %{
        "log_1" => [0, 1],
        "log_2" => [2, 3]
      }

      result =
        Finalization.build_log_transactions(log_descriptors, %{}, Version.from_integer(100))

      # All logs should get empty transactions
      assert BedrockTransactionTestSupport.extract_log_writes(result["log_1"]) == %{}
      assert BedrockTransactionTestSupport.extract_log_writes(result["log_2"]) == %{}

      assert BedrockTransactionTestSupport.extract_log_version(result["log_1"]) ==
               Version.from_integer(100)

      assert BedrockTransactionTestSupport.extract_log_version(result["log_2"]) ==
               Version.from_integer(100)
    end

    test "handles overlapping tag coverage" do
      log_descriptors = %{
        "log_1" => [0, 1],
        "log_2" => [1, 2],
        # Overlaps with both log_1 and log_2
        "log_3" => [0, 2]
      }

      transactions_by_tag = %{
        0 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_0" => "value_0"}),
        1 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_1" => "value_1"}),
        2 => BedrockTransactionTestSupport.new_log_transaction(100, %{"key_2" => "value_2"})
      }

      result =
        Finalization.build_log_transactions(
          log_descriptors,
          transactions_by_tag,
          Version.from_integer(100)
        )

      # Verify each log gets correct writes
      assert BedrockTransactionTestSupport.extract_log_writes(result["log_1"]) == %{
               "key_0" => "value_0",
               "key_1" => "value_1"
             }

      assert BedrockTransactionTestSupport.extract_log_writes(result["log_2"]) == %{
               "key_1" => "value_1",
               "key_2" => "value_2"
             }

      assert BedrockTransactionTestSupport.extract_log_writes(result["log_3"]) == %{
               "key_0" => "value_0",
               "key_2" => "value_2"
             }
    end

    test "demonstrates multi-tag write distribution with overlapping storage teams" do
      # This test demonstrates the full multi-tag functionality:
      # A single write gets distributed to multiple logs based on tag intersection
      log_descriptors = %{
        "log_alpha" => [:team_a, :team_b],
        "log_beta" => [:team_b, :team_c],
        "log_gamma" => [:team_a, :team_c]
      }

      # Simulating the result after group_writes_by_tag with overlapping teams:
      # A key "shared_key" was distributed to multiple tags due to overlapping storage teams
      transactions_by_tag = %{
        # All three teams contain the same key due to overlapping ranges
        :team_a =>
          BedrockTransactionTestSupport.new_log_transaction(100, %{
            <<"shared_key">> => <<"shared_value">>,
            <<"key_a">> => <<"value_a">>
          }),
        :team_b =>
          BedrockTransactionTestSupport.new_log_transaction(100, %{
            <<"shared_key">> => <<"shared_value">>,
            <<"key_b">> => <<"value_b">>
          }),
        :team_c =>
          BedrockTransactionTestSupport.new_log_transaction(100, %{
            <<"shared_key">> => <<"shared_value">>,
            <<"key_c">> => <<"value_c">>
          })
      }

      result =
        Finalization.build_log_transactions(
          log_descriptors,
          transactions_by_tag,
          Version.from_integer(100)
        )

      # log_alpha covers team_a and team_b, so gets writes from both
      assert BedrockTransactionTestSupport.extract_log_writes(result["log_alpha"]) == %{
               # appears from both teams (same key/value)
               <<"shared_key">> => <<"shared_value">>,
               <<"key_a">> => <<"value_a">>,
               <<"key_b">> => <<"value_b">>
             }

      # log_beta covers team_b and team_c
      assert BedrockTransactionTestSupport.extract_log_writes(result["log_beta"]) == %{
               # appears from both teams
               <<"shared_key">> => <<"shared_value">>,
               <<"key_b">> => <<"value_b">>,
               <<"key_c">> => <<"value_c">>
             }

      # log_gamma covers team_a and team_c
      assert BedrockTransactionTestSupport.extract_log_writes(result["log_gamma"]) == %{
               # appears from both teams
               <<"shared_key">> => <<"shared_value">>,
               <<"key_a">> => <<"value_a">>,
               <<"key_c">> => <<"value_c">>
             }

      # This demonstrates that a single write (shared_key) properly reaches all logs
      # that have tag intersection with the storage teams containing that key
    end
  end

  describe "try_to_push_transaction_to_log/3" do
    test "succeeds when log server responds with :ok" do
      log_server = Support.create_mock_log_server()

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == :ok
    end

    test "returns error when log server is down" do
      service_descriptor = %{kind: :log, status: {:down, :some_reason}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == {:error, :unavailable}
    end

    test "returns error when log server responds with error" do
      log_server =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:push, _transaction, _last_version}} ->
              GenServer.reply(from, {:error, :disk_full})
          end
        end)

      Support.ensure_process_killed(log_server)

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      assert result == {:error, :disk_full}
    end

    test "handles log server process exit" do
      # Create a log server that will exit immediately
      log_server = spawn(fn -> exit(:normal) end)
      # Give it time to exit
      Process.sleep(100)

      service_descriptor = %{kind: :log, status: {:up, log_server}}
      encoded_transaction = "mock_encoded_transaction"
      last_commit_version = 99

      # Should handle process exit gracefully
      result =
        Finalization.try_to_push_transaction_to_log(
          service_descriptor,
          encoded_transaction,
          last_commit_version
        )

      # Should get an error when the process is dead
      assert {:error, _reason} = result
    end
  end
end
