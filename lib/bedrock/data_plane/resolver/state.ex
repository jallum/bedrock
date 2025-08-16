defmodule Bedrock.DataPlane.Resolver.State do
  @moduledoc """
  State structure for Resolver GenServer processes.

  Maintains the interval tree for conflict detection, version tracking, and
  waiting queue for out-of-order transactions. Includes lock token for
  authentication.
  """

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Tree

  @type mode :: :running

  @type t :: %__MODULE__{
          tree: Tree.t(),
          oldest_version: Bedrock.version(),
          last_version: Bedrock.version(),
          waiting: %{
            Bedrock.version() =>
              {Bedrock.version(), [Resolver.transaction_summary()],
               (aborted :: [non_neg_integer()] -> :ok)}
          },
          mode: mode(),
          lock_token: Bedrock.lock_token()
        }
  defstruct tree: nil,
            oldest_version: nil,
            last_version: nil,
            waiting: %{},
            mode: :running,
            lock_token: nil
end
