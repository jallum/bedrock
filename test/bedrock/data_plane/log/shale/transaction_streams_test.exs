defmodule Bedrock.DataPlane.Log.Shale.TransactionStreamsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransactionTestSupport
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Version

  describe "from_segments/2 with unloaded transactions" do
    test "demonstrates the fix - no longer crashes with enumerable error" do
      # Create a segment with nil transactions (simulating unloaded state)
      segment = %Segment{
        path: "nonexistent_path_for_test",
        min_version: Version.from_integer(1),
        transactions: nil
      }

      # Before the fix, this would crash with:
      # "protocol Enumerable not implemented for type Atom. Got value: nil"
      #
      # After the fix, it should handle nil gracefully by calling ensure_transactions_are_loaded
      # The file doesn't exist, so we expect a File.Error, not an Enumerable error

      assert_raise File.Error, fn ->
        TransactionStreams.from_segments([segment], Version.from_integer(1))
      end

      # The key point is that we get a File.Error (expected) rather than:
      # Protocol.UndefinedError with "protocol Enumerable not implemented for type Atom"
    end

    test "processes segments with loaded transactions normally" do
      # Create a segment with pre-loaded transactions (reversed order as stored)
      transaction_1 =
        BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
          "key1" => "value1"
        })

      transaction_2 =
        BedrockTransactionTestSupport.new_log_transaction(Version.from_integer(2), %{
          "key2" => "value2"
        })

      segment = %Segment{
        path: "test_path",
        min_version: Version.from_integer(1),
        transactions: [transaction_2, transaction_1]
      }

      # This should work normally
      result = TransactionStreams.from_segments([segment], Version.from_integer(1))

      assert {:ok, stream} = result

      # Convert stream to list to verify content
      transactions = Enum.to_list(stream)
      assert length(transactions) == 1
      # The stream returns the remaining transactions after the target version
      # Since target_version=1 matches the first transaction, we get the rest
      assert BedrockTransactionTestSupport.extract_log_version(hd(transactions)) ==
               Version.from_integer(2)
    end
  end
end
