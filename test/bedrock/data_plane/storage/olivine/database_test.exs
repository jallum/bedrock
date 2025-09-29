defmodule Bedrock.DataPlane.Storage.Olivine.DatabaseTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Database

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

      assert {:ok, {data_db, index_db} = db} = Database.open(table_name, file_path)

      assert data_db.window_size_in_microseconds == 5_000_000
      assert index_db.dets_storage == table_name

      Database.close(db)
    end

    test "open/3 accepts custom window size via options", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.dets")

      assert {:ok, {data_db, _index_db} = db} = Database.open(table_name, file_path, window_in_ms: 2_000)

      assert data_db.window_size_in_microseconds == 2_000_000

      Database.close(db)
    end

    test "close/1 properly syncs and closes database", %{tmp_dir: tmp_dir} do
      table_name = String.to_atom("test_db_#{System.unique_integer([:positive])}")
      file_path = Path.join(tmp_dir, "test_#{table_name}.dets")

      {:ok, db} = Database.open(table_name, file_path)
      assert :ok = Database.close(db)
    end
  end

  describe "value operations" do
    setup context, do: with_db(context, "values.dets", :values_test)

    @tag :tmp_dir
    test "store_value/4 and load_value/2 work correctly", %{db: db} do
      key = <<"test_key">>
      value = <<"test_value">>
      version = <<0, 0, 0, 0, 0, 0, 0, 1>>

      {:ok, locator, _updated_db} = Database.store_value(db, key, version, value)
      assert {:ok, ^value} = Database.load_value(db, locator)
    end

    @tag :tmp_dir
    test "load_value/2 returns error for non-existent locator", %{db: db} do
      # Create a locator that doesn't exist in the buffer or file
      non_existent_locator = <<999, 0, 0, 0, 0, 0, 0, 10>>
      assert :eof = Database.load_value(db, non_existent_locator)
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
    end

    @tag :tmp_dir
    test "info/2 handles unknown statistics", %{db: db} do
      assert Database.info(db, :unknown_stat) == :undefined
    end
  end

  describe "DETS schema verification" do
    setup context, do: with_db(context, "schema.dets", :schema_test)
  end

  describe "durable version management" do
    setup context, do: with_db(context, "durable_version.dets", :durable_test)
  end

  describe "value_loader function" do
    setup context, do: with_db(context, "value_loader.dets", :value_loader_test)
  end
end
