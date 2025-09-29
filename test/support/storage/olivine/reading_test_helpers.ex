defmodule Bedrock.DataPlane.Storage.Olivine.ReadingTestHelpers do
  @moduledoc """
  Test helper functions for Reading testing.

  This module provides simplified interfaces for testing read operations
  without the full server integration lifecycle.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Reading
  alias Bedrock.Internal.WaitingList
  alias Bedrock.KeySelector

  @doc """
  Simple synchronous get function for testing.
  """
  def get(state, key_or_selector, version, opts \\ [])

  def get(%{index_manager: index_manager, database: database}, key, version, opts) when is_binary(key) do
    case IndexManager.page_for_key(index_manager, key, version) do
      {:ok, page} ->
        load_fn = Database.value_loader(database)

        do_now_or_async_with_reply(opts[:reply_fn], fn ->
          fetch_from_page(page, key, load_fn)
        end)

      error ->
        error
    end
  end

  def get(%{index_manager: index_manager, database: database}, %KeySelector{} = key_selector, version, opts) do
    case IndexManager.page_for_key(index_manager, key_selector, version) do
      {:ok, resolved_key, page} ->
        load_fn = Database.value_loader(database)
        fetch_task = fn -> fetch_resolved_key_selector(page, resolved_key, load_fn) end
        do_now_or_async_with_reply(opts[:reply_fn], fetch_task)

      error ->
        error
    end
  end

  @doc """
  Simple synchronous get_range function for testing.
  """
  def get_range(state, start_key_or_selector, end_key_or_selector, version, opts \\ [])

  def get_range(%{index_manager: index_manager, database: database}, start_key, end_key, version, opts)
      when is_binary(start_key) and is_binary(end_key) do
    case IndexManager.pages_for_range(index_manager, start_key, end_key, version) do
      {:ok, pages} ->
        load_many_fn = Database.many_value_loader(database)
        limit = opts[:limit]

        do_now_or_async_with_reply(opts[:reply_fn], fn ->
          range_fetch_from_pages(pages, start_key, end_key, limit, load_many_fn)
        end)

      error ->
        error
    end
  end

  def get_range(
        %{index_manager: index_manager, database: database},
        %KeySelector{} = start_selector,
        %KeySelector{} = end_selector,
        version,
        opts
      ) do
    case IndexManager.pages_for_range(index_manager, start_selector, end_selector, version) do
      {:ok, {resolved_start_key, resolved_end_key}, pages} ->
        load_many_fn = Database.many_value_loader(database)
        limit = opts[:limit]

        do_now_or_async_with_reply(opts[:reply_fn], fn ->
          range_fetch_from_pages(pages, resolved_start_key, resolved_end_key, limit, load_many_fn)
        end)

      error ->
        error
    end
  end

  @doc """
  Test helper to add requests to waitlist from state.
  """
  def add_to_waitlist_from_state(
        %{read_request_manager: manager} = state,
        fetch_request,
        version,
        reply_fn,
        timeout_in_ms
      ) do
    {updated_waiting_fetches, _timeout} =
      WaitingList.insert(
        manager.waiting_fetches,
        version,
        fetch_request,
        reply_fn,
        timeout_in_ms
      )

    updated_manager = %{manager | waiting_fetches: updated_waiting_fetches}
    %{state | read_request_manager: updated_manager}
  end

  @doc """
  Test helper to notify waiting fetches from state.
  """
  def notify_waiting_fetches_from_state(%{read_request_manager: manager} = state, applied_version) do
    context = Reading.ReadingContext.new(state.index_manager, state.database)

    # Extract spawned PIDs by comparing active tasks before and after
    original_tasks = Reading.get_active_tasks(manager)
    updated_manager = Reading.notify_waiting_fetches(manager, context, applied_version)
    new_tasks = Reading.get_active_tasks(updated_manager)
    spawned_pids = MapSet.to_list(MapSet.difference(new_tasks, original_tasks))

    updated_state = %{state | read_request_manager: updated_manager}
    {updated_state, spawned_pids}
  end

  # Private helpers (copied from Reading)

  defp fetch_resolved_key_selector(page, resolved_key, load_fn) do
    case fetch_from_page(page, resolved_key, load_fn) do
      {:ok, value} -> {:ok, {resolved_key, value}}
      error -> error
    end
  end

  defp fetch_from_page(page, key, load_fn) do
    case Page.locator_for_key(page, key) do
      {:ok, locator} -> load_fn.(locator)
      error -> error
    end
  end

  defp range_fetch_from_pages(pages, start_key, end_key, limit, load_many_fn) do
    {keys_and_locators, has_more} =
      pages
      |> Page.stream_key_locators_in_range(start_key, end_key)
      |> Stream.with_index()
      |> Enum.reduce_while({[], false}, fn
        {key_and_locator, index}, {acc, _} when index < limit ->
          {:cont, {[key_and_locator | acc], false}}

        _, {acc, _} ->
          {:halt, {acc, true}}
      end)

    {:ok, values_by_locator} =
      keys_and_locators
      |> Enum.map(fn {_key, locator} -> locator end)
      |> load_many_fn.()

    results =
      keys_and_locators
      |> Enum.reverse()
      |> Enum.map(fn {key, locator} -> {key, Map.get(values_by_locator, locator)} end)

    {:ok, {results, has_more}}
  end

  defp do_now_or_async_with_reply(nil, fun), do: fun.()
  defp do_now_or_async_with_reply(reply_fn, fun), do: Task.start(fn -> reply_fn.(fun.()) end)
end
