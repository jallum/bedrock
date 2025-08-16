defmodule Bedrock.DataPlane.Log.Shale.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.BedrockTransaction
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  @type mode :: :locked | :running | :recovering

  @type t :: %__MODULE__{
          cluster: module(),
          director: Director.ref() | nil,
          epoch: Bedrock.epoch() | nil,
          id: Worker.id(),
          foreman: Foreman.ref(),
          path: String.t(),
          segment_recycler: SegmentRecycler.server() | nil,
          #
          last_version: Bedrock.version(),
          writer: Writer.t() | nil,
          active_segment: Segment.t() | nil,
          segments: [Segment.t()],
          pending_pushes: %{
            Bedrock.version() =>
              {encoded_transaction :: BedrockTransaction.encoded(),
               ack_fn :: (:ok | {:error, term()} -> :ok)}
          },
          #
          mode: mode(),
          oldest_version: Bedrock.version(),
          otp_name: Worker.otp_name(),
          params: %{
            default_pull_limit: pos_integer(),
            max_pull_limit: pos_integer()
          },
          waiting_pullers: %{
            Bedrock.version() => [
              {Bedrock.timestamp_in_ms(), reply_to_fn :: (any() -> :ok),
               opts :: [limit: integer(), timeout: timeout()]}
            ]
          }
        }
  defstruct cluster: nil,
            director: nil,
            epoch: nil,
            foreman: nil,
            id: nil,
            path: nil,
            segment_recycler: nil,
            #
            last_version: nil,
            writer: nil,
            segments: [],
            active_segment: nil,
            pending_pushes: %{},
            #
            mode: :locked,
            oldest_version: nil,
            otp_name: nil,
            pending_transactions: %{},
            waiting_pullers: %{},
            params: %{
              default_pull_limit: 100,
              max_pull_limit: 500
            }
end
