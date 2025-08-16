defmodule Bedrock.DataPlane.Log.Shale.RecoveryTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log.Shale.Recovery
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    {:ok, recycler} =
      start_supervised(
        {SegmentRecycler,
         path: tmp_dir, min_available: 1, max_available: 1, segment_size: 1024 * 1024}
      )

    state = %State{
      mode: :locked,
      path: tmp_dir,
      segment_recycler: recycler,
      active_segment: nil,
      segments: [],
      writer: nil,
      oldest_version: Version.from_integer(0),
      last_version: Version.from_integer(0)
    }

    {:ok, state: state, tmp_dir: tmp_dir}
  end

  describe "recover_from/4" do
    test "returns error when not in locked mode", %{state: state} do
      unlocked_state = %{state | mode: :running}

      assert {:error, :lock_required} =
               Recovery.recover_from(
                 unlocked_state,
                 :source,
                 Version.from_integer(1),
                 Version.from_integer(2)
               )
    end

    test "successfully recovers with no transactions", %{state: state} do
      source_log = setup_mock_log([])

      assert {:ok, recovered} =
               Recovery.recover_from(
                 state,
                 source_log,
                 Version.from_integer(1),
                 Version.from_integer(1)
               )

      assert recovered.mode == :running
      assert recovered.oldest_version == Version.from_integer(1)
      assert recovered.last_version == Version.from_integer(1)
    end

    test "successfully recovers with valid transactions", %{state: state} do
      transactions = [
        create_encoded_tx(Version.from_integer(1), %{"data" => "test1"}),
        create_encoded_tx(Version.from_integer(2), %{"data" => "test2"})
      ]

      source_log = setup_mock_log(transactions)

      assert {:ok, recovered} =
               Recovery.recover_from(
                 state,
                 source_log,
                 Version.from_integer(1),
                 Version.from_integer(2)
               )

      assert recovered.mode == :running
      assert recovered.oldest_version == Version.from_integer(1)
      assert recovered.last_version == Version.from_integer(2)
    end

    test "handles unavailable source log", %{state: state} do
      source_log = setup_failing_mock_log(:unavailable)

      assert {:error, {:source_log_unavailable, ^source_log}} =
               Recovery.recover_from(
                 state,
                 source_log,
                 Version.from_integer(1),
                 Version.from_integer(2)
               )
    end
  end

  describe "pull_transactions/4" do
    test "handles empty transaction list", %{state: state} do
      source_log = setup_mock_log([])

      assert {:ok, ^state} =
               Recovery.pull_transactions(
                 state,
                 source_log,
                 Version.from_integer(1),
                 Version.from_integer(1)
               )
    end

    test "handles invalid transaction data", %{state: state} do
      source_log = setup_mock_log(["invalid"])

      assert {:error, :invalid_format} =
               Recovery.pull_transactions(
                 state,
                 source_log,
                 Version.from_integer(1),
                 Version.from_integer(2)
               )
    end
  end

  # Helper functions
  defp create_encoded_tx(version, data) do
    # Convert data map to mutations format
    mutations = Enum.map(data, fn {key, value} -> {:set, key, value} end)

    # Create BedrockTransaction
    transaction = %{
      mutations: mutations,
      read_conflicts: [],
      write_conflicts: [],
      read_version: nil
    }

    # Encode and add version as transaction_id
    encoded = BedrockTransaction.encode(transaction)
    {:ok, encoded_with_id} = BedrockTransaction.add_transaction_id(encoded, version)
    encoded_with_id
  end

  defp setup_mock_log(transactions) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
          send(from, {ref, {:ok, transactions}})
      after
        500 -> :timeout
      end
    end)
  end

  defp setup_failing_mock_log(error) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
          send(from, {ref, {:error, error}})
      after
        500 -> :timeout
      end
    end)
  end
end
