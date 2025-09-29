defmodule Bedrock.DataPlane.Storage.Olivine.Reading do
  @moduledoc """
  Manages read request tasks and waitlists for Olivine storage.

  Handles active task tracking, process monitoring, and waitlist management
  for read operations that cannot be immediately satisfied.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Telemetry, as: OlivineTelemetry
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.Internal.WaitingList
  alias Bedrock.KeySelector

  # Context struct containing only what Reading needs from state
  defmodule ReadingContext do
    @moduledoc false
    defstruct [:index_manager, :database]

    @type t :: %__MODULE__{
            index_manager: IndexManager.t(),
            database: Database.t()
          }

    @spec new(IndexManager.t(), Database.t()) :: t()
    def new(index_manager, database) do
      %__MODULE__{index_manager: index_manager, database: database}
    end
  end

  # Request context for read operations
  defmodule ReadRequest do
    @moduledoc false
    defstruct [
      :start_time,
      :operation,
      :key,
      :wait_start_time,
      :task_start_time,
      :was_waitlisted,
      :was_async,
      :result
    ]

    def new(operation, key) do
      %__MODULE__{
        start_time: System.monotonic_time(:microsecond),
        operation: operation,
        key: key,
        was_waitlisted: false,
        was_async: false,
        result: :unknown
      }
    end

    def mark_waitlisted(%__MODULE__{} = timing),
      do: %{timing | was_waitlisted: true, wait_start_time: System.monotonic_time(:microsecond)}

    def mark_async(%__MODULE__{} = timing),
      do: %{timing | was_async: true, task_start_time: System.monotonic_time(:microsecond)}

    def mark_result(%__MODULE__{} = timing, result), do: %{timing | result: classify_result(result)}

    def emit_telemetry(%__MODULE__{} = timing) do
      now = System.monotonic_time(:microsecond)
      total_duration = now - timing.start_time
      wait_duration = if timing.wait_start_time, do: (timing.task_start_time || now) - timing.wait_start_time, else: 0
      task_duration = if timing.task_start_time, do: now - timing.task_start_time, else: 0

      OlivineTelemetry.trace_read_operation_complete(timing.operation, timing.key,
        total_duration_μs: total_duration,
        wait_duration_μs: wait_duration,
        task_duration_μs: task_duration,
        was_waitlisted: timing.was_waitlisted,
        was_async: timing.was_async,
        result: timing.result
      )
    end

    defp classify_result({:ok, _}), do: :success
    defp classify_result(:ok), do: :success
    defp classify_result({:error, _}), do: :error
    defp classify_result(_), do: :unknown
  end

  @type t :: %__MODULE__{
          active_tasks: MapSet.t(pid()),
          waiting_fetches: WaitingList.t()
        }

  defstruct active_tasks: MapSet.new(),
            waiting_fetches: %{}

  @doc """
  Creates a new empty read request manager.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Handle a get request with full lifecycle management (async, waitlist, task tracking).
  Returns {updated_manager, result}.
  """
  @spec handle_get(t(), ReadingContext.t(), Bedrock.key() | KeySelector.t(), Bedrock.version(), keyword()) ::
          {t(), term()}
  def handle_get(manager, context, key_or_selector, version, opts),
    do: execute_request(manager, context, :get, {key_or_selector, version}, opts)

  @doc """
  Handle a get_range request with full lifecycle management (async, waitlist, task tracking).
  Returns {updated_manager, result}.
  """
  @spec handle_get_range(
          t(),
          ReadingContext.t(),
          Bedrock.key() | KeySelector.t(),
          Bedrock.key() | KeySelector.t(),
          Bedrock.version(),
          keyword()
        ) ::
          {t(), term()}
  def handle_get_range(manager, context, start_key, end_key, version, opts),
    do: execute_request(manager, context, :get_range, {start_key, end_key, version}, opts)

  @doc """
  Removes an active task from tracking with telemetry.
  """
  @spec remove_active_task(t(), pid()) :: t()
  def remove_active_task(%__MODULE__{} = manager, task_pid) do
    # Emit consolidated telemetry if timing info is available
    case Process.get({:read_timing, task_pid}) do
      %ReadRequest{} = timing ->
        timing
        # Assume success if task completed
        |> ReadRequest.mark_result(:success)
        |> ReadRequest.emit_telemetry()

        Process.delete({:read_timing, task_pid})

      nil ->
        # Fallback for tasks without timing (shouldn't happen with new code)
        :ok
    end

    %{manager | active_tasks: MapSet.delete(manager.active_tasks, task_pid)}
  end

  @doc """
  Returns the set of active task PIDs.
  """
  @spec get_active_tasks(t()) :: MapSet.t(pid())
  def get_active_tasks(%__MODULE__{} = manager), do: manager.active_tasks

  @doc """
  Removes all waiting fetches for a version and processes them with fetch logic.
  Returns updated_manager.
  """
  @spec notify_waiting_fetches(t(), ReadingContext.t(), Bedrock.version()) :: t()
  def notify_waiting_fetches(%__MODULE__{} = manager, context, applied_version) do
    {updated_waiting_fetches, waiting_entries} =
      WaitingList.remove_all(
        manager.waiting_fetches,
        applied_version
      )

    # Process waiting entries and collect spawned PIDs
    spawned_pids =
      Enum.reduce(waiting_entries, [], fn {_deadline, reply_fn, fetch_request}, pids_acc ->
        case execute_fetch_request(context, reply_fn, fetch_request) do
          {:ok, pid} -> [pid | pids_acc]
          :ok -> pids_acc
        end
      end)

    # Add spawned PIDs to active tasks
    manager_with_updated_waiting = %{manager | waiting_fetches: updated_waiting_fetches}
    Enum.reduce(spawned_pids, manager_with_updated_waiting, &add_active_task(&2, &1))
  end

  @doc """
  Shuts down the read request manager by waiting for active tasks and notifying waiting fetches.
  """
  @spec shutdown(t()) :: :ok
  def shutdown(%__MODULE__{} = manager) do
    if MapSet.size(manager.active_tasks) > 0 do
      wait_for_tasks(manager.active_tasks, 5_000)
    end

    notify_waitlist_shutdown(manager.waiting_fetches)
  end

  # Private implementation

  defp execute_request(manager, context, operation, request_args, opts) do
    operation_key = extract_operation_key(operation, request_args)
    timing = ReadRequest.new(operation, operation_key)

    case setup_fetch_task(context, operation, request_args, opts) do
      {:ok, fetch_task} ->
        handle_successful_resolution(manager, fetch_task, timing, opts)

      {:error, :version_too_new} = error ->
        handle_version_too_new(manager, error, request_args, timing, opts)

      error ->
        handle_immediate_error(error, timing)
        {manager, error}
    end
  end

  defp extract_operation_key(:get, {%KeySelector{} = key_selector, _version}), do: key_selector
  defp extract_operation_key(:get, {key, _version}) when is_binary(key), do: key

  defp extract_operation_key(:get_range, {start_key, end_key, _version}), do: {start_key, end_key}

  defp setup_fetch_task(
         %ReadingContext{index_manager: index_manager, database: database},
         :get,
         {key_or_selector, version},
         _opts
       ) do
    case resolve_key_to_page(index_manager, key_or_selector, version) do
      {:ok, {resolved_key, page, fetch_type}} ->
        load_fn = Database.value_loader(database)
        {:ok, fetch_task(fetch_type, page, resolved_key, load_fn)}

      error ->
        error
    end
  end

  defp setup_fetch_task(
         %ReadingContext{index_manager: index_manager, database: database},
         :get_range,
         {start_key, end_key, version},
         opts
       ) do
    case resolve_range_to_pages(index_manager, start_key, end_key, version) do
      {:ok, pages, resolved_start, resolved_end, _} ->
        load_many_fn = Database.many_value_loader(database)
        limit = opts[:limit]
        {:ok, range_fetch_task(pages, resolved_start, resolved_end, limit, load_many_fn)}

      error ->
        error
    end
  end

  defp fetch_task(:keyselector, page, resolved_key, load_fn),
    do: fn -> fetch_resolved_key_selector(page, resolved_key, load_fn) end

  defp fetch_task(:binary, page, resolved_key, load_fn), do: fn -> fetch_from_page(page, resolved_key, load_fn) end

  defp range_fetch_task(pages, resolved_start, resolved_end, limit, load_many_fn),
    do: fn -> range_fetch_from_pages(pages, resolved_start, resolved_end, limit, load_many_fn) end

  defp resolve_key_to_page(index_manager, %KeySelector{} = key_selector, version) do
    case IndexManager.page_for_key(index_manager, key_selector, version) do
      {:ok, resolved_key, page} -> {:ok, {resolved_key, page, :keyselector}}
      error -> error
    end
  end

  defp resolve_key_to_page(index_manager, key, version) when is_binary(key) do
    case IndexManager.page_for_key(index_manager, key, version) do
      {:ok, page} -> {:ok, {key, page, :binary}}
      error -> error
    end
  end

  defp resolve_range_to_pages(index_manager, start_key, end_key, version)
       when is_binary(start_key) and is_binary(end_key) do
    case IndexManager.pages_for_range(index_manager, start_key, end_key, version) do
      {:ok, pages} -> {:ok, pages, start_key, end_key, nil}
      error -> error
    end
  end

  defp resolve_range_to_pages(index_manager, %KeySelector{} = start_sel, %KeySelector{} = end_sel, version) do
    case IndexManager.pages_for_range(index_manager, start_sel, end_sel, version) do
      {:ok, {resolved_start, resolved_end}, pages} -> {:ok, pages, resolved_start, resolved_end, nil}
      error -> error
    end
  end

  defp handle_successful_resolution(manager, fetch_task, timing, opts) do
    case do_now_or_async_with_reply(opts[:reply_fn], fetch_task) do
      {:ok, pid} ->
        timing = ReadRequest.mark_async(timing)
        # Store timing in process dictionary for task completion
        Process.put({:read_timing, pid}, timing)
        {add_active_task(manager, pid), :ok}

      result ->
        timing
        |> ReadRequest.mark_result(result)
        |> ReadRequest.emit_telemetry()

        {manager, result}
    end
  end

  defp handle_version_too_new(manager, error, request_args, timing, opts) do
    case opts[:wait_ms] do
      wait_ms when is_integer(wait_ms) and wait_ms > 0 ->
        timing = ReadRequest.mark_waitlisted(timing)
        fetch_request = build_waitlist_request(timing.operation, request_args)

        # Store timing for when the waitlisted request is eventually processed
        waitlist_key = {extract_version(request_args), opts[:reply_fn]}
        Process.put({:waitlist_timing, waitlist_key}, timing)

        updated_manager =
          add_to_waitlist(manager, fetch_request, extract_version(request_args), opts[:reply_fn], wait_ms)

        {updated_manager, :ok}

      _ ->
        timing
        |> ReadRequest.mark_result(error)
        |> ReadRequest.emit_telemetry()

        {manager, error}
    end
  end

  defp handle_immediate_error(error, timing) do
    timing
    |> ReadRequest.mark_result(error)
    |> ReadRequest.emit_telemetry()
  end

  defp build_waitlist_request(:get, {%KeySelector{} = ks, version}), do: {ks, version}
  defp build_waitlist_request(:get, {key, version}), do: {key, version}

  defp build_waitlist_request(:get_range, {start_key, end_key, version}), do: {start_key, end_key, version}

  defp extract_version({_, version}), do: version
  defp extract_version({_, _, version}), do: version

  # Task and waitlist management

  defp add_active_task(%__MODULE__{} = manager, task_pid) do
    Process.monitor(task_pid)
    %{manager | active_tasks: MapSet.put(manager.active_tasks, task_pid)}
  end

  defp add_to_waitlist(%__MODULE__{} = manager, fetch_request, version, reply_fn, timeout_ms) do
    {updated_waiting_fetches, _timeout} =
      WaitingList.insert(
        manager.waiting_fetches,
        version,
        fetch_request,
        reply_fn,
        timeout_ms
      )

    %{manager | waiting_fetches: updated_waiting_fetches}
  end

  # Fetch execution

  defp execute_fetch_request(context, reply_fn, {key, version}) when is_binary(key) do
    case execute_get(context, key, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  defp execute_fetch_request(context, reply_fn, {start_key, end_key, version})
       when is_binary(start_key) and is_binary(end_key) do
    case execute_get_range(context, start_key, end_key, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  defp execute_fetch_request(context, reply_fn, {%KeySelector{} = key_selector, version}) do
    case execute_get(context, key_selector, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  defp execute_fetch_request(
         context,
         reply_fn,
         {%KeySelector{} = start_selector, %KeySelector{} = end_selector, version}
       ) do
    case execute_get_range(context, start_selector, end_selector, version, reply_fn: reply_fn) do
      {:ok, pid} ->
        {:ok, pid}

      error ->
        reply_fn.(error)
        :ok
    end
  end

  # Core fetch operations (private)

  defp execute_get(%ReadingContext{index_manager: index_manager, database: database}, key, version, opts)
       when is_binary(key) do
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

  defp execute_get(
         %ReadingContext{index_manager: index_manager, database: database},
         %KeySelector{} = key_selector,
         version,
         opts
       ) do
    case IndexManager.page_for_key(index_manager, key_selector, version) do
      {:ok, resolved_key, page} ->
        load_fn = Database.value_loader(database)
        fetch_task = fn -> fetch_resolved_key_selector(page, resolved_key, load_fn) end
        do_now_or_async_with_reply(opts[:reply_fn], fetch_task)

      error ->
        error
    end
  end

  defp execute_get_range(
         %ReadingContext{index_manager: index_manager, database: database},
         start_key,
         end_key,
         version,
         opts
       )
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

  defp execute_get_range(
         %ReadingContext{index_manager: index_manager, database: database},
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

  # Low-level helpers

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

  defp notify_waitlist_shutdown(waiting_fetches) do
    waiting_fetches
    |> Enum.flat_map(fn {_version, entries} -> entries end)
    |> Enum.each(fn {_deadline, reply_fn, _fetch_request} ->
      reply_fn.({:error, :shutting_down})
    end)
  end

  defp wait_for_tasks(tasks, timeout) do
    Telemetry.trace_shutdown_waiting(MapSet.size(tasks))
    do_wait_for_tasks_loop(tasks, timeout)
  end

  defp do_wait_for_tasks_loop(tasks, timeout) do
    cond do
      MapSet.size(tasks) == 0 ->
        :ok

      timeout <= 0 ->
        Telemetry.trace_shutdown_timeout(MapSet.size(tasks))
        :timeout

      true ->
        start_time = System.monotonic_time(:millisecond)

        receive do
          {:DOWN, _ref, :process, pid, _reason} ->
            tasks = MapSet.delete(tasks, pid)
            elapsed = System.monotonic_time(:millisecond) - start_time
            do_wait_for_tasks_loop(tasks, timeout - elapsed)
        after
          timeout ->
            Telemetry.trace_shutdown_timeout(MapSet.size(tasks))
            :timeout
        end
    end
  end
end
