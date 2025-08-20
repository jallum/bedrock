defmodule Bedrock.FunctionalTest do
  use ExUnit.Case, async: false

  setup_all do
    working_dir = Path.join(System.tmp_dir!(), "bedrock_#{:erlang.unique_integer([:positive])}")
    Logger.info("Working directory: #{working_dir}")
    File.mkdir_p!(working_dir)

    defmodule Cluster do
      use Bedrock.Cluster,
        otp_app: :example_app,
        name: "test",
        config: [
          capabilities: [:coordination, :log, :storage],
          trace: [:coordinator, :recovery],
          coordinator: [
            path: working_dir
          ],
          storage: [
            path: working_dir
          ],
          log: [
            path: working_dir
          ]
        ]
    end
  end

  defmodule Example.Repo do
    use Bedrock.Repo,
      cluster: Example.Cluster,
      key_codecs: [
        default: Bedrock.KeyCodec.TupleKeyCodec
      ],
      value_codecs: [
        default: Bedrock.ValueCodec.BertValueCodec
      ]
  end
end
