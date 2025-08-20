defmodule Bedrock.DataPlane.Storage.Olivine.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database

  @tmp_dir "/tmp/olivine_database_test"

  setup do
    # Ensure clean state for each test
    File.rm_rf(@tmp_dir)
    File.mkdir_p!(@tmp_dir)

    on_exit(fn ->
      File.rm_rf(@tmp_dir)
    end)

    {:ok, tmp_dir: @tmp_dir}
  end

  describe "database lifecycle" do
    test "open/2 creates and opens a DETS database", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.dets")

      {:ok, db} = Database.open(table_name, file_path)

      assert db.dets_storage
      assert db.window_size_in_microseconds == 5_000_000

      Database.close(db)
    end

    test "open/3 accepts custom window size", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.dets")

      {:ok, db} = Database.open(table_name, file_path, 2_000)

      assert db.window_size_in_microseconds == 2_000_000

      Database.close(db)
    end

    test "close/1 properly syncs and closes database", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.dets")

      {:ok, db} = Database.open(table_name, file_path)
      assert :ok = Database.close(db)
    end

    test "database persists data across open/close cycles", %{tmp_dir: tmp_dir} do
      file_path = Path.join(tmp_dir, "persist_test.dets")

      # First session: store data
      table1 = String.to_atom("persist_test1_#{System.unique_integer([:positive])}")
      {:ok, db1} = Database.open(table1, file_path)
      :ok = Database.store_page(db1, 42, <<"test_page_data">>)
      :ok = Database.store_value(db1, <<"key1">>, <<"value1">>)
      Database.close(db1)

      # Second session: verify data persists
      table2 = String.to_atom("persist_test2_#{System.unique_integer([:positive])}")
      {:ok, db2} = Database.open(table2, file_path)

      {:ok, page_data} = Database.load_page(db2, 42)
      assert page_data == <<"test_page_data">>

      {:ok, value} = Database.load_value(db2, <<"key1">>)
      assert value == <<"value1">>

      Database.close(db2)
    end
  end

  describe "page operations" do
    setup %{tmp_dir: tmp_dir} do
      # Use unique table name for each test
      table_name = String.to_atom("pages_test_#{System.unique_integer([:positive])}_#{:erlang.system_time()}")
      file_path = Path.join(tmp_dir, "pages_#{table_name}.dets")
      {:ok, db} = Database.open(table_name, file_path)

      on_exit(fn -> Database.close(db) end)

      {:ok, db: db}
    end

    test "store_page/3 and load_page/2 work correctly", %{db: db} do
      page_binary = <<"this is a test page">>

      :ok = Database.store_page(db, 1, page_binary)
      {:ok, loaded_page} = Database.load_page(db, 1)

      assert loaded_page == page_binary
    end

    test "load_page/2 returns error for non-existent page", %{db: db} do
      assert {:error, :not_found} = Database.load_page(db, 999)
    end

    test "store_page/3 overwrites existing pages", %{db: db} do
      :ok = Database.store_page(db, 1, <<"original">>)
      :ok = Database.store_page(db, 1, <<"updated">>)

      {:ok, loaded} = Database.load_page(db, 1)
      assert loaded == <<"updated">>
    end

    test "get_all_page_ids/1 returns all stored page IDs", %{db: db} do
      # Store some pages
      :ok = Database.store_page(db, 1, <<"page1">>)
      :ok = Database.store_page(db, 5, <<"page5">>)
      :ok = Database.store_page(db, 3, <<"page3">>)

      # Store some values (should not appear in page IDs)
      :ok = Database.store_value(db, <<"key1">>, <<"value1">>)

      page_ids = Database.get_all_page_ids(db)
      assert Enum.sort(page_ids) == [1, 3, 5]
    end
  end

  describe "value operations" do
    setup %{tmp_dir: tmp_dir} do
      # Use unique table name for each test
      table_name = String.to_atom("values_test_#{System.unique_integer([:positive])}_#{:erlang.system_time()}")
      file_path = Path.join(tmp_dir, "values_#{table_name}.dets")
      {:ok, db} = Database.open(table_name, file_path)

      on_exit(fn -> Database.close(db) end)

      {:ok, db: db}
    end

    test "store_value/3 and load_value/2 work correctly", %{db: db} do
      key = <<"test_key">>
      value = <<"test_value">>

      :ok = Database.store_value(db, key, value)
      {:ok, loaded_value} = Database.load_value(db, key)

      assert loaded_value == value
    end

    test "load_value/2 returns error for non-existent key", %{db: db} do
      assert {:error, :not_found} = Database.load_value(db, <<"missing">>)
    end

    test "store_value/3 handles last-write-wins behavior", %{db: db} do
      # Store initial value
      :ok = Database.store_value(db, <<"key1">>, <<"initial_value">>)

      # Overwrite with new value (last-write-wins)
      :ok = Database.store_value(db, <<"key1">>, <<"updated_value">>)

      # Different key
      :ok = Database.store_value(db, <<"key2">>, <<"different_key">>)

      # Verify last value wins
      {:ok, val1} = Database.load_value(db, <<"key1">>)
      assert val1 == <<"updated_value">>

      {:ok, val2} = Database.load_value(db, <<"key2">>)
      assert val2 == <<"different_key">>
    end

    test "batch_store_values/2 stores multiple values efficiently", %{db: db} do
      values = [
        {<<"key1">>, <<"value1">>},
        {<<"key2">>, <<"value2">>},
        {<<"key3">>, <<"value3">>}
      ]

      :ok = Database.batch_store_values(db, values)

      # Verify all values were stored
      {:ok, val1} = Database.load_value(db, <<"key1">>)
      assert val1 == <<"value1">>

      {:ok, val2} = Database.load_value(db, <<"key2">>)
      assert val2 == <<"value2">>

      {:ok, val3} = Database.load_value(db, <<"key3">>)
      assert val3 == <<"value3">>
    end
  end

  describe "database info and statistics" do
    setup %{tmp_dir: tmp_dir} do
      # Use unique table name for each test
      table_name = String.to_atom("info_test_#{System.unique_integer([:positive])}_#{:erlang.system_time()}")
      file_path = Path.join(tmp_dir, "info_#{table_name}.dets")
      {:ok, db} = Database.open(table_name, file_path)

      on_exit(fn -> Database.close(db) end)

      {:ok, db: db}
    end

    test "info/2 returns database statistics", %{db: db} do
      # Empty database
      assert Database.info(db, :n_keys) == 0
      assert Database.info(db, :size_in_bytes) >= 0
      assert Database.info(db, :utilization) >= 0.0
      assert Database.info(db, :key_ranges) == []

      # Add some data
      :ok = Database.store_page(db, 1, <<"page_data">>)
      :ok = Database.store_value(db, <<"key1">>, <<"value1">>)

      # Statistics should reflect added data
      assert Database.info(db, :n_keys) > 0
      assert Database.info(db, :size_in_bytes) > 0
    end

    test "info/2 handles unknown statistics", %{db: db} do
      assert Database.info(db, :unknown_stat) == :undefined
    end

    test "sync/1 forces data to disk", %{db: db} do
      :ok = Database.store_page(db, 1, <<"test">>)
      assert :ok = Database.sync(db)
    end
  end

  describe "DETS schema verification" do
    setup %{tmp_dir: tmp_dir} do
      # Use unique table name for each test
      table_name = String.to_atom("schema_test_#{System.unique_integer([:positive])}_#{:erlang.system_time()}")
      file_path = Path.join(tmp_dir, "schema_#{table_name}.dets")
      {:ok, db} = Database.open(table_name, file_path)

      on_exit(fn -> Database.close(db) end)

      {:ok, db: db}
    end

    test "natural type separation works correctly", %{db: db} do
      # Store page (integer key)
      :ok = Database.store_page(db, 42, <<"page_data">>)

      # Store value (tuple key)
      :ok = Database.store_value(db, <<"key">>, <<"value_data">>)

      # Verify they don't interfere with each other
      {:ok, page_data} = Database.load_page(db, 42)
      assert page_data == <<"page_data">>

      {:ok, value_data} = Database.load_value(db, <<"key">>)
      assert value_data == <<"value_data">>

      # Verify page IDs only include integer keys, not tuple keys
      page_ids = Database.get_all_page_ids(db)
      assert page_ids == [42]
    end

    test "handles edge cases in schema separation", %{db: db} do
      # Store value with integer-like key (should still use tuple format)
      :ok = Database.store_value(db, <<42>>, <<"binary_42">>)

      # Store page with ID 42 (different from the binary key above)
      :ok = Database.store_page(db, 42, <<"page_42">>)

      # Both should coexist without conflict
      {:ok, value} = Database.load_value(db, <<42>>)
      assert value == <<"binary_42">>

      {:ok, page} = Database.load_page(db, 42)
      assert page == <<"page_42">>
    end
  end
end
