defmodule Bedrock.Internal.GenServer.Calls do
  @spec broadcast([GenServer.name()], otp_name :: GenServer.name(), message :: any()) :: :ok
  def broadcast(nodes, otp_name, message) do
    GenServer.abcast(nodes, otp_name, message)
    :ok
  end

  @spec cast(GenServer.server(), message :: any()) :: :ok
  def cast(server, message), do: GenServer.cast(server, message)

  @spec call(GenServer.server(), message :: any(), timeout()) :: term()
  def call(server, message, timeout) do
    try do
      GenServer.call(server, message, to_timeout(timeout))
    rescue
      _ -> {:error, :unknown}
    catch
      :exit, {:noproc, _} -> {:error, :unavailable}
      :exit, {{:nodedown, _}, _} -> {:error, :unavailable}
      :exit, {:timeout, _} -> {:error, :timeout}
    end
  end
end
