defmodule Bedrock.DataPlane.Storage.Olivine.AtomicOperationsTest do
  @moduledoc """
  Tests for atomic operations in the Olivine storage engine (IndexManager).

  These tests verify that atomic operations (add, min, max) work
  correctly at the Olivine IndexManager layer, including:
  - Integration with persistent database storage
  - Proper handling of missing keys (default to empty binary)
  - Correct binary arithmetic with variable-length operands
  - Database persistence of atomic operation results
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database, as: OlivineDatabase
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  describe "Olivine IndexManager atomic operations" do
    setup do
      # Create temporary directory for test database
      temp_dir = System.tmp_dir!() <> "/test_olivine_#{System.unique_integer()}"
      File.mkdir_p!(temp_dir)

      db_file_path = Path.join(temp_dir, "olivine_test.dets")
      {:ok, database} = OlivineDatabase.open(:"test_db_#{System.unique_integer()}", db_file_path)
      index_manager = IndexManager.new()

      on_exit(fn ->
        :ok = OlivineDatabase.close(database)
        File.rm_rf!(temp_dir)
      end)

      {:ok, database: database, index_manager: index_manager}
    end

    test "add works with missing key", %{database: database, index_manager: index_manager} do
      transaction = create_atomic_transaction([{:atomic, :add, "counter", <<8>>}])
      apply_and_verify(index_manager, transaction, database, [{"counter", <<8>>}])
    end

    test "atomic operations work with existing values through database", %{
      database: database,
      index_manager: index_manager
    } do
      initial_transaction = create_set_transaction("balance", <<100>>, 1)

      {manager_with_initial, database_with_initial} =
        IndexManager.apply_transaction(index_manager, initial_transaction, database)

      add_transaction = create_atomic_transaction([{:atomic, :add, "balance", <<231>>}], 2)
      # Verify the result (100 + 231 with carry = 75 as next byte)
      apply_and_verify(manager_with_initial, add_transaction, database_with_initial, [{"balance", <<75, 1>>}])
    end

    test "min and max work correctly", %{database: database, index_manager: index_manager} do
      atomic_transaction =
        create_atomic_transaction([
          {:atomic, :min, "temperature", <<22>>},
          {:atomic, :max, "pressure", <<150>>}
        ])

      apply_and_verify(index_manager, atomic_transaction, database, [
        {"temperature", <<22>>},
        {"pressure", <<150>>}
      ])
    end
  end

  # Helper functions

  defp apply_and_verify(index_manager, transaction, database, expected_values) do
    {updated_manager, updated_database} = IndexManager.apply_transaction(index_manager, transaction, database)
    version = Transaction.commit_version!(transaction)

    # Get the current index from the updated manager
    [{^version, {current_index, _modified_pages}} | _] = updated_manager.versions

    for {key, expected_value} <- expected_values do
      # Get locator for the key from the index
      {:ok, _page, locator} = Index.locator_for_key(current_index, key)
      # Load value using the locator
      assert {:ok, ^expected_value} = OlivineDatabase.load_value(updated_database, locator)
    end
  end

  defp create_atomic_transaction(mutations, version_int \\ 1) do
    create_transaction(mutations, version_int)
  end

  defp create_set_transaction(key, value, version_int) do
    create_transaction([{:set, key, value}], version_int)
  end

  defp create_transaction(mutations, version_int) do
    transaction_map = %{
      mutations: mutations,
      read_conflicts: {nil, []},
      write_conflicts: []
    }

    encoded = Transaction.encode(transaction_map)
    version = Version.from_integer(version_int)

    {:ok, with_version} = Transaction.add_commit_version(encoded, version)
    with_version
  end
end
