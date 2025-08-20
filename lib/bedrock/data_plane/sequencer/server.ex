defmodule Bedrock.DataPlane.Sequencer.Server do
  @moduledoc """
  GenServer implementation for the Sequencer version authority.

  Manages Lamport clock state for version assignment, tracking the last committed
  version and next commit version. Implements the core Lamport clock semantics
  where each commit version assignment updates the internal clock state to maintain
  proper causality relationships for distributed MVCC conflict detection.
  """

  use GenServer

  import Bedrock.DataPlane.Sequencer.Telemetry,
    only: [
      emit_next_read_version: 1,
      emit_next_commit_version: 3,
      emit_successful_commit: 2
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Sequencer.State
  alias Bedrock.DataPlane.Version

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    known_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {director, epoch, known_committed_version},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @impl true
  @spec init({pid(), Bedrock.epoch(), Bedrock.version()}) :: {:ok, State.t()}
  def init({director, epoch, known_committed_version}) do
    epoch_start_monotonic_us = System.monotonic_time(:microsecond)
    epoch_baseline_version_int = Version.to_integer(known_committed_version)

    then(
      %State{
        director: director,
        epoch: epoch,
        next_commit_version_int: epoch_baseline_version_int + 1,
        last_commit_version_int: epoch_baseline_version_int,
        known_committed_version_int: epoch_baseline_version_int,
        epoch_baseline_version_int: epoch_baseline_version_int,
        epoch_start_monotonic_us: epoch_start_monotonic_us
      },
      &{:ok, &1}
    )
  end

  @impl true
  @spec handle_call(:next_read_version | :next_commit_version, GenServer.from(), State.t()) ::
          {:reply, {:ok, Bedrock.version()} | {:ok, Bedrock.version(), Bedrock.version()}, State.t()}
  def handle_call(:next_read_version, _from, t) do
    # Convert to Version.t() only at return
    read_version = Version.from_integer(t.known_committed_version_int)
    emit_next_read_version(read_version)
    reply(t, {:ok, read_version})
  end

  @impl true
  def handle_call(:next_commit_version, _from, t) do
    current_monotonic_us = System.monotonic_time(:microsecond)

    # Calculate microseconds elapsed since epoch start
    elapsed_us = current_monotonic_us - t.epoch_start_monotonic_us

    # Proposed version = baseline + elapsed microseconds
    proposed_version_int = t.epoch_baseline_version_int + elapsed_us

    # Ensure we always advance by at least 1 from the last assigned version
    commit_version_int = max(proposed_version_int, t.next_commit_version_int)
    next_commit_version_int = commit_version_int + 1

    updated_state = %{
      t
      | next_commit_version_int: next_commit_version_int,
        last_commit_version_int: commit_version_int
    }

    # Convert to Version.t() for return
    last_commit_version = Version.from_integer(t.last_commit_version_int)
    commit_version = Version.from_integer(commit_version_int)

    emit_next_commit_version(last_commit_version, commit_version, elapsed_us)
    reply(updated_state, {:ok, last_commit_version, commit_version})
  end

  @impl true
  @spec handle_cast({:report_successful_commit, Bedrock.version()}, State.t()) ::
          {:noreply, State.t()}
  def handle_cast({:report_successful_commit, commit_version}, t) do
    # Convert incoming Version.t() to integer for comparison
    commit_version_int = Version.to_integer(commit_version)
    updated_known_committed_int = max(t.known_committed_version_int, commit_version_int)

    known_committed_version = Version.from_integer(updated_known_committed_int)
    emit_successful_commit(commit_version, known_committed_version)

    noreply(%{t | known_committed_version_int: updated_known_committed_int}, [])
  end
end
