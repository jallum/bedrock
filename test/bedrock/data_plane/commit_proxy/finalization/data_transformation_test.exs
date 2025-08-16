defmodule Bedrock.DataPlane.CommitProxy.FinalizationDataTransformationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias FinalizationTestSupport, as: Support

  describe "key_to_tags/2" do
    setup do
      # Create overlapping storage teams to test multi-tag behavior
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0x80>>, :end}, storage_ids: ["storage_3", "storage_4"]},
        %{tag: 2, key_range: {<<0x40>>, <<0xC0>>}, storage_ids: ["storage_5"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "maps key to single tag when no overlap", %{storage_teams: storage_teams} do
      assert {:ok, [0]} = Finalization.key_to_tags(<<0x01>>, storage_teams)
      assert {:ok, [0]} = Finalization.key_to_tags(<<0x3F>>, storage_teams)
    end

    test "maps key to multiple tags when overlapping", %{storage_teams: storage_teams} do
      # Key 0x90 should match tags 0, 1, and 2
      # tag 0: <<>> to <<0xFF>>  (includes 0x90)
      # tag 1: <<0x80>> to :end  (includes 0x90)
      # tag 2: <<0x40>> to <<0xC0>>  (includes 0x90)
      assert {:ok, tags} = Finalization.key_to_tags(<<0x90>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]

      # Key 0x50 should match tags 0 and 2
      # tag 0: <<>> to <<0xFF>>  (includes 0x50)
      # tag 1: <<0x80>> to :end  (does NOT include 0x50, since 0x50 < 0x80)
      # tag 2: <<0x40>> to <<0xC0>>  (includes 0x50)
      assert {:ok, tags} = Finalization.key_to_tags(<<0x50>>, storage_teams)
      assert Enum.sort(tags) == [0, 2]

      # Key 0x85 should match tags 0, 1, and 2
      # tag 0: <<>> to <<0xFF>>  (includes 0x85)
      # tag 1: <<0x80>> to :end  (includes 0x85)
      # tag 2: <<0x40>> to <<0xC0>>  (includes 0x85)
      assert {:ok, tags} = Finalization.key_to_tags(<<0x85>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]
    end

    test "returns empty list for key with no matching teams" do
      storage_teams = [
        %{tag: 0, key_range: {<<0x10>>, <<0x20>>}, storage_ids: ["storage_1"]}
      ]

      assert {:ok, []} = Finalization.key_to_tags(<<0x05>>, storage_teams)
      assert {:ok, []} = Finalization.key_to_tags(<<0x25>>, storage_teams)
    end

    test "handles boundary conditions correctly", %{storage_teams: storage_teams} do
      # Key exactly at range boundaries
      # Key 0x80 should match tags 0, 1, and 2
      # tag 0: <<>> to <<0xFF>>  (includes 0x80)
      # tag 1: <<0x80>> to :end  (includes 0x80, boundary inclusive)
      # tag 2: <<0x40>> to <<0xC0>>  (includes 0x80)
      assert {:ok, tags} = Finalization.key_to_tags(<<0x80>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]

      # Key 0x40 should match tags 0 and 2
      # tag 0: <<>> to <<0xFF>>  (includes 0x40)
      # tag 1: <<0x80>> to :end  (does NOT include 0x40)
      # tag 2: <<0x40>> to <<0xC0>>  (includes 0x40, boundary inclusive)
      assert {:ok, tags} = Finalization.key_to_tags(<<0x40>>, storage_teams)
      assert Enum.sort(tags) == [0, 2]
    end
  end

  describe "key_to_tag/2 (legacy)" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0xFF>>, :end}, storage_ids: ["storage_3", "storage_4"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "maps key to correct tag for first range", %{storage_teams: storage_teams} do
      assert {:ok, 0} = Finalization.key_to_tag(<<0x01>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0x80>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE>>, storage_teams)
    end

    test "maps key to correct tag for second range", %{storage_teams: storage_teams} do
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF, 0x01>>, storage_teams)
    end

    test "returns error for empty storage teams" do
      assert {:error, :no_matching_team} = Finalization.key_to_tag(<<"any_key">>, [])
    end

    test "handles boundary conditions correctly", %{storage_teams: storage_teams} do
      # Key exactly at range boundary should belong to second range
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)

      # Key just before boundary should belong to first range
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE, 0xFF>>, storage_teams)
    end
  end

  describe "group_writes_by_tag/2" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, :end}, storage_ids: ["storage_2"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "groups writes by their target storage team tags", %{storage_teams: storage_teams} do
      writes = %{
        <<"apple">> => <<"fruit">>,
        <<"banana">> => <<"yellow">>,
        <<"orange">> => <<"citrus">>,
        <<"zebra">> => <<"animal">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected =
        {:ok,
         %{
           0 => %{
             <<"apple">> => <<"fruit">>,
             <<"banana">> => <<"yellow">>
           },
           1 => %{
             <<"orange">> => <<"citrus">>,
             <<"zebra">> => <<"animal">>
           }
         }}

      assert result == expected
    end

    test "handles empty writes map", %{storage_teams: storage_teams} do
      result = Finalization.group_writes_by_tag(%{}, storage_teams)
      assert result == {:ok, %{}}
    end

    test "handles writes that all belong to same tag", %{storage_teams: storage_teams} do
      writes = %{
        <<"apple">> => <<"fruit">>,
        <<"banana">> => <<"yellow">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected =
        {:ok,
         %{
           0 => %{
             <<"apple">> => <<"fruit">>,
             <<"banana">> => <<"yellow">>
           }
         }}

      assert result == expected
    end
  end

  describe "merge_writes_by_tag/2" do
    test "merges write maps for same tags" do
      acc = %{
        0 => %{<<"key1">> => <<"value1">>},
        1 => %{<<"key2">> => <<"value2">>}
      }

      new_writes = %{
        0 => %{<<"key3">> => <<"value3">>},
        2 => %{<<"key4">> => <<"value4">>}
      }

      result = Finalization.merge_writes_by_tag(acc, new_writes)

      expected = %{
        0 => %{<<"key1">> => <<"value1">>, <<"key3">> => <<"value3">>},
        1 => %{<<"key2">> => <<"value2">>},
        2 => %{<<"key4">> => <<"value4">>}
      }

      assert result == expected
    end

    test "handles empty maps" do
      assert Finalization.merge_writes_by_tag(%{}, %{}) == %{}

      acc = %{0 => %{<<"key">> => <<"value">>}}
      assert Finalization.merge_writes_by_tag(acc, %{}) == acc
      assert Finalization.merge_writes_by_tag(%{}, acc) == acc
    end

    test "overwrites values for same keys" do
      acc = %{0 => %{<<"key">> => <<"old_value">>}}
      new_writes = %{0 => %{<<"key">> => <<"new_value">>}}

      result = Finalization.merge_writes_by_tag(acc, new_writes)
      expected = %{0 => %{<<"key">> => <<"new_value">>}}

      assert result == expected
    end
  end

  describe "transform_transactions_for_resolution/1" do
    test "transforms transaction list to resolver format" do
      transaction_map1 = %{
        mutations: [{:set, <<"write_key1">>, <<"write_value1">>}],
        write_conflicts: [{<<"write_key1">>, <<"write_key1\0">>}],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(100), [{<<"read_key">>, <<"read_key\0">>}]}
      }

      transaction_map2 = %{
        mutations: [{:set, <<"write_key2">>, <<"write_value2">>}],
        write_conflicts: [{<<"write_key2">>, <<"write_key2\0">>}],
        read_conflicts: {nil, []}
      }

      binary_transaction1 = BedrockTransaction.encode(transaction_map1)
      binary_transaction2 = BedrockTransaction.encode(transaction_map2)

      transactions = [
        {fn _ -> :ok end, binary_transaction1},
        {fn _ -> :ok end, binary_transaction2}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      expected_version = Bedrock.DataPlane.Version.from_integer(100)

      expected = [
        {{expected_version, [{<<"read_key">>, <<"read_key\0">>}]},
         [{<<"write_key1">>, <<"write_key1\0">>}]},
        {nil, [{<<"write_key2">>, <<"write_key2\0">>}]}
      ]

      assert result == expected
    end

    test "handles empty transaction list" do
      result = Finalization.transform_transactions_for_resolution([])
      assert result == []
    end

    test "handles transactions with no reads" do
      transaction_map = %{
        mutations: [{:set, <<"key">>, <<"value">>}],
        write_conflicts: [{<<"key">>, <<"key\0">>}],
        read_conflicts: {nil, []}
      }

      binary_transaction = BedrockTransaction.encode(transaction_map)

      transactions = [
        {fn _ -> :ok end, binary_transaction}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)
      expected = [{nil, [{<<"key">>, <<"key\0">>}]}]

      assert result == expected
    end

    test "handles transactions with no writes" do
      transaction_map = %{
        mutations: [],
        write_conflicts: [],
        read_conflicts:
          {Bedrock.DataPlane.Version.from_integer(100), [{<<"read_key">>, <<"read_key\0">>}]}
      }

      binary_transaction = BedrockTransaction.encode(transaction_map)

      transactions = [
        {fn _ -> :ok end, binary_transaction}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)
      expected_version = Bedrock.DataPlane.Version.from_integer(100)
      expected = [{{expected_version, [{<<"read_key">>, <<"read_key\0">>}]}, []}]

      assert result == expected
    end

    test "extracts write conflicts in consistent order" do
      transaction_map = %{
        mutations: [
          {:set, <<"z_key">>, <<"value1">>},
          {:set, <<"a_key">>, <<"value2">>},
          {:set, <<"m_key">>, <<"value3">>}
        ],
        write_conflicts: [
          {<<"z_key">>, <<"z_key\0">>},
          {<<"a_key">>, <<"a_key\0">>},
          {<<"m_key">>, <<"m_key\0">>}
        ],
        read_conflicts: {nil, []}
      }

      binary_transaction = BedrockTransaction.encode(transaction_map)

      transactions = [
        {fn _ -> :ok end, binary_transaction}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      [{nil, write_conflicts}] = result

      # Write conflicts should maintain order from transaction
      expected_conflicts = [
        {<<"z_key">>, <<"z_key\0">>},
        {<<"a_key">>, <<"a_key\0">>},
        {<<"m_key">>, <<"m_key\0">>}
      ]

      assert write_conflicts == expected_conflicts
    end
  end

  describe "edge cases and error handling" do
    test "key_to_tag handles keys at exact boundaries" do
      storage_teams = Support.sample_storage_teams()

      # Key exactly at boundary should belong to the second range
      assert {:ok, 1} = Finalization.key_to_tag(<<"m">>, storage_teams)
      assert {:ok, 2} = Finalization.key_to_tag(<<"z">>, storage_teams)

      # Keys just before boundaries
      assert {:ok, 0} = Finalization.key_to_tag(<<"l">>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<"y">>, storage_teams)
    end

    test "group_writes_by_tag handles unknown keys by returning error" do
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]}
      ]

      # This key doesn't match any range
      writes = %{<<"z_unknown">> => <<"value">>}

      # Should return error with storage team coverage error
      assert Finalization.group_writes_by_tag(writes, storage_teams) ==
               {:error, {:storage_team_coverage_error, "z_unknown"}}
    end

    test "group_writes_by_tag distributes writes to multiple tags for overlapping teams" do
      # Create overlapping storage teams
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"h">>, <<"z">>}, storage_ids: ["storage_2"]}
      ]

      # Keys "hello" and "india" should match both teams
      writes = %{
        <<"hello">> => <<"world">>,
        <<"india">> => <<"country">>,
        # Should only match tag 0
        <<"apple">> => <<"fruit">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected =
        {:ok,
         %{
           0 => %{
             <<"hello">> => <<"world">>,
             <<"india">> => <<"country">>,
             <<"apple">> => <<"fruit">>
           },
           1 => %{
             <<"hello">> => <<"world">>,
             <<"india">> => <<"country">>
           }
         }}

      assert result == expected
    end
  end
end
