defmodule Bedrock.DataPlane.Storage.Olivine.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Version

  defp with_db(context, file_name, table_name) do
    tmp_dir = context[:tmp_dir] || raise "tmp_dir not available in context"
    file_path = Path.join(tmp_dir, file_name)
    {:ok, db} = Database.open(table_name, file_path)
    on_exit(fn -> Database.close(db) end)
    {:ok, db: db}
  end

  describe "database lifecycle" do
    @tag :tmp_dir

    setup context do
      tmp_dir =
        context[:tmp_dir] || Path.join(System.tmp_dir!(), "db_lifecycle_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      {:ok, tmp_dir: tmp_dir}
    end

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

      table1 = String.to_atom("persist_test1_#{System.unique_integer([:positive])}")
      {:ok, db1} = Database.open(table1, file_path)
      :ok = Database.store_page(db1, 42, <<"test_page_data">>)
      :ok = Database.store_value(db1, <<"key1">>, <<"value1">>)
      Database.close(db1)

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
    setup context, do: with_db(context, "pages.dets", :pages_test)

    @tag :tmp_dir
    test "store_page/3 and load_page/2 work correctly", %{db: db} do
      page_binary = <<"this is a test page">>

      :ok = Database.store_page(db, 1, page_binary)
      {:ok, loaded_page} = Database.load_page(db, 1)

      assert loaded_page == page_binary
    end

    @tag :tmp_dir
    test "load_page/2 returns error for non-existent page", %{db: db} do
      assert {:error, :not_found} = Database.load_page(db, 999)
    end

    @tag :tmp_dir
    test "store_page/3 overwrites existing pages", %{db: db} do
      :ok = Database.store_page(db, 1, <<"original">>)
      :ok = Database.store_page(db, 1, <<"updated">>)

      {:ok, loaded} = Database.load_page(db, 1)
      assert loaded == <<"updated">>
    end

    @tag :tmp_dir
    test "get_all_page_ids/1 returns all stored page IDs", %{db: db} do
      :ok = Database.store_page(db, 1, <<"page1">>)
      :ok = Database.store_page(db, 5, <<"page5">>)
      :ok = Database.store_page(db, 3, <<"page3">>)

      :ok = Database.store_value(db, <<"key1">>, <<"value1">>)

      page_ids = Database.get_all_page_ids(db)
      assert Enum.sort(page_ids) == [1, 3, 5]
    end
  end

  describe "value operations" do
    setup context, do: with_db(context, "values.dets", :values_test)

    @tag :tmp_dir
    test "store_value/3 and load_value/2 work correctly", %{db: db} do
      key = <<"test_key">>
      value = <<"test_value">>

      :ok = Database.store_value(db, key, value)
      {:ok, loaded_value} = Database.load_value(db, key)

      assert loaded_value == value
    end

    @tag :tmp_dir
    test "load_value/2 returns error for non-existent key", %{db: db} do
      assert {:error, :not_found} = Database.load_value(db, <<"missing">>)
    end

    @tag :tmp_dir
    test "store_value/3 handles last-write-wins behavior", %{db: db} do
      :ok = Database.store_value(db, <<"key1">>, <<"initial_value">>)

      :ok = Database.store_value(db, <<"key1">>, <<"updated_value">>)

      :ok = Database.store_value(db, <<"key2">>, <<"different_key">>)

      {:ok, val1} = Database.load_value(db, <<"key1">>)
      assert val1 == <<"updated_value">>

      {:ok, val2} = Database.load_value(db, <<"key2">>)
      assert val2 == <<"different_key">>
    end

    @tag :tmp_dir
    test "batch_store_values/2 stores multiple values efficiently", %{db: db} do
      values = [
        {<<"key1">>, <<"value1">>},
        {<<"key2">>, <<"value2">>},
        {<<"key3">>, <<"value3">>}
      ]

      :ok = Database.batch_store_values(db, values)

      {:ok, val1} = Database.load_value(db, <<"key1">>)
      assert val1 == <<"value1">>

      {:ok, val2} = Database.load_value(db, <<"key2">>)
      assert val2 == <<"value2">>

      {:ok, val3} = Database.load_value(db, <<"key3">>)
      assert val3 == <<"value3">>
    end
  end

  describe "database info and statistics" do
    setup context, do: with_db(context, "info.dets", :info_test)

    @tag :tmp_dir
    test "info/2 returns database statistics", %{db: db} do
      assert Database.info(db, :n_keys) == 0
      assert Database.info(db, :size_in_bytes) >= 0
      assert Database.info(db, :utilization) >= 0.0
      assert Database.info(db, :key_ranges) == []

      :ok = Database.store_page(db, 1, <<"page_data">>)
      :ok = Database.store_value(db, <<"key1">>, <<"value1">>)

      assert Database.info(db, :n_keys) > 0
      assert Database.info(db, :size_in_bytes) > 0
    end

    @tag :tmp_dir
    test "info/2 handles unknown statistics", %{db: db} do
      assert Database.info(db, :unknown_stat) == :undefined
    end

    @tag :tmp_dir
    test "sync/1 forces data to disk", %{db: db} do
      :ok = Database.store_page(db, 1, <<"test">>)
      assert :ok = Database.sync(db)
    end
  end

  describe "DETS schema verification" do
    setup context, do: with_db(context, "schema.dets", :schema_test)

    @tag :tmp_dir
    test "natural type separation works correctly", %{db: db} do
      :ok = Database.store_page(db, 42, <<"page_data">>)

      :ok = Database.store_value(db, <<"key">>, <<"value_data">>)

      {:ok, page_data} = Database.load_page(db, 42)
      assert page_data == <<"page_data">>

      {:ok, value_data} = Database.load_value(db, <<"key">>)
      assert value_data == <<"value_data">>

      page_ids = Database.get_all_page_ids(db)
      assert page_ids == [42]
    end

    @tag :tmp_dir
    test "handles edge cases in schema separation", %{db: db} do
      :ok = Database.store_value(db, <<42>>, <<"binary_42">>)

      :ok = Database.store_page(db, 42, <<"page_42">>)

      {:ok, value} = Database.load_value(db, <<42>>)
      assert value == <<"binary_42">>

      {:ok, page} = Database.load_page(db, 42)
      assert page == <<"page_42">>
    end
  end

  describe "unified value operations" do
    setup context, do: with_db(context, "unified_values.dets", :unified_test)

    @tag :tmp_dir
    test "fetch_value/3 returns values from lookaside buffer for recent versions", %{db: db} do
      key = <<"test_key">>
      value = <<"test_value">>
      version = Version.from_integer(1000)

      # Store value in lookaside buffer
      :ok = Database.store_value(db, key, version, value)

      # Should be able to fetch it
      assert {:ok, ^value} = Database.fetch_value(db, key, version)
    end

    @tag :tmp_dir
    test "fetch_value/3 returns values from DETS for durable versions", %{db: db} do
      key = <<"durable_key">>
      value = <<"durable_value">>

      # Store directly in DETS (simulating durable storage)
      :ok = Database.store_value(db, key, value)

      # Should be able to fetch from DETS storage
      assert {:ok, ^value} = Database.fetch_value(db, key, Version.zero())
    end

    @tag :tmp_dir
    test "fetch_value/3 routes correctly based on durable version", %{db: db} do
      key = <<"routing_key">>
      hot_value = <<"hot_value">>
      cold_value = <<"cold_value">>

      # Store cold value in DETS
      :ok = Database.store_value(db, key, cold_value)

      # Store hot value in lookaside buffer (version higher than durable)
      hot_version = Version.from_integer(2000)
      :ok = Database.store_value(db, key, hot_version, hot_value)

      # Fetch at durable version should get cold value
      assert {:ok, ^cold_value} = Database.fetch_value(db, key, Version.zero())

      # Fetch at hot version should get hot value
      assert {:ok, ^hot_value} = Database.fetch_value(db, key, hot_version)
    end

    @tag :tmp_dir
    test "store_value/4 handles version-specific storage", %{db: db} do
      key = <<"versioned_key">>
      version1 = Version.from_integer(1000)
      version2 = Version.from_integer(2000)

      :ok = Database.store_value(db, key, version1, <<"value1">>)
      :ok = Database.store_value(db, key, version2, <<"value2">>)

      assert {:ok, <<"value1">>} = Database.fetch_value(db, key, version1)
      assert {:ok, <<"value2">>} = Database.fetch_value(db, key, version2)
    end

    @tag :tmp_dir
    test "store_value/4 prevents duplicate entries", %{db: db} do
      key = <<"dup_key">>
      version = Version.from_integer(1000)

      :ok = Database.store_value(db, key, version, <<"first">>)
      {:error, :already_exists} = Database.store_value(db, key, version, <<"second">>)

      # Should still have the first value
      assert {:ok, <<"first">>} = Database.fetch_value(db, key, version)
    end
  end

  describe "durable version management" do
    setup context, do: with_db(context, "durable_version.dets", :durable_test)

    @tag :tmp_dir
    test "store_durable_version/2 persists and updates durable version", %{db: db} do
      new_version = Version.from_integer(7000)
      {:ok, updated_db} = Database.store_durable_version(db, new_version)

      # Should be updated in memory
      {:ok, loaded_version} = Database.load_durable_version(updated_db)
      assert loaded_version == new_version
    end
  end

  describe "value_loader function" do
    setup context, do: with_db(context, "value_loader.dets", :value_loader_test)

    @tag :tmp_dir
    test "value_loader/1 creates function that can load values", %{db: db} do
      key = <<"loader_key">>
      hot_version = Version.from_integer(1000)
      cold_version = Version.zero()

      # Store hot value
      :ok = Database.store_value(db, key, hot_version, <<"hot_value">>)
      # Store cold value
      :ok = Database.store_value(db, key, <<"cold_value">>)

      # Create value loader
      loader = Database.value_loader(db)

      # Should load hot value for hot version
      assert {:ok, <<"hot_value">>} = loader.(key, hot_version)

      # Should load cold value for cold version
      assert {:ok, <<"cold_value">>} = loader.(key, cold_version)

      # Should return not_found for missing key
      assert {:error, :not_found} = loader.(<<"missing">>, hot_version)
    end
  end
end
