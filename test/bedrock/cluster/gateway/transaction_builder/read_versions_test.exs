defmodule Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersionsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  def create_test_state(opts \\ []) do
    %State{
      state: :valid,
      gateway: Keyword.get(opts, :gateway, :test_gateway),
      transaction_system_layout: %{
        sequencer: Keyword.get(opts, :sequencer, :test_sequencer)
      },
      read_version: Keyword.get(opts, :read_version),
      read_version_lease_expiration: Keyword.get(opts, :read_version_lease_expiration)
    }
  end

  describe "next_read_version/2" do
    test "successfully gets read version and lease" do
      state = create_test_state()

      sequencer_fn = fn :test_sequencer -> {:ok, 12_345} end
      gateway_fn = fn :test_gateway, 12_345 -> {:ok, 5000} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:ok, 12_345, 5000} = result
    end

    test "handles sequencer error" do
      state = create_test_state()

      sequencer_fn = fn :test_sequencer -> {:error, :unavailable} end
      gateway_fn = fn _, _ -> {:ok, 5000} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:error, :unavailable} = result
    end

    test "handles gateway error" do
      state = create_test_state()

      sequencer_fn = fn :test_sequencer -> {:ok, 12_345} end
      gateway_fn = fn :test_gateway, 12_345 -> {:error, :timeout} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:error, :timeout} = result
    end

    test "works with different sequencer and gateway references" do
      state =
        create_test_state(
          sequencer: :custom_sequencer,
          gateway: :custom_gateway
        )

      sequencer_fn = fn :custom_sequencer -> {:ok, 99_999} end
      gateway_fn = fn :custom_gateway, 99_999 -> {:ok, 10_000} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:ok, 99_999, 10_000} = result
    end

    test "handles zero read version" do
      state = create_test_state()

      sequencer_fn = fn :test_sequencer -> {:ok, 0} end
      gateway_fn = fn :test_gateway, 0 -> {:ok, 1000} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:ok, 0, 1000} = result
    end

    test "handles large read version numbers" do
      state = create_test_state()
      large_version = 999_999_999_999

      sequencer_fn = fn :test_sequencer -> {:ok, large_version} end
      gateway_fn = fn :test_gateway, ^large_version -> {:ok, 2000} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:ok, ^large_version, 2000} = result
    end

    test "uses default functions when no opts provided" do
      state = create_test_state()

      # This will fail with the real functions since we don't have real infrastructure
      # but it tests that the default functions are being called
      result = ReadVersions.next_read_version(state)

      # Expect some kind of error since we don't have real sequencer/gateway
      assert {:error, _} = result
    end
  end

  describe "renew_read_version_lease/2" do
    test "successfully renews lease" do
      current_time = 50_000

      state =
        create_test_state(
          read_version: 12_345,
          read_version_lease_expiration: current_time + 1000
        )

      gateway_fn = fn :test_gateway, 12_345 -> {:ok, 5000} end
      time_fn = fn -> current_time end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      result = ReadVersions.renew_read_version_lease(state, opts)

      assert {:ok, new_state} = result
      assert new_state.read_version_lease_expiration == current_time + 5000
    end

    test "handles gateway renewal error" do
      state = create_test_state(read_version: 12_345)

      gateway_fn = fn :test_gateway, 12_345 -> {:error, :read_version_lease_expired} end
      time_fn = fn -> 50_000 end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      result = ReadVersions.renew_read_version_lease(state, opts)

      assert {:error, :read_version_lease_expired} = result
    end

    test "preserves other state fields during renewal" do
      current_time = 60_000

      original_state = %State{
        state: :valid,
        gateway: :test_gateway,
        transaction_system_layout: %{test: "data"},
        read_version: 12_345,
        read_version_lease_expiration: current_time - 1000,
        stack: []
      }

      gateway_fn = fn :test_gateway, 12_345 -> {:ok, 3000} end
      time_fn = fn -> current_time end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      {:ok, new_state} = ReadVersions.renew_read_version_lease(original_state, opts)

      # Verify lease was updated
      assert new_state.read_version_lease_expiration == current_time + 3000

      # Verify other fields preserved
      assert new_state.state == original_state.state
      assert new_state.gateway == original_state.gateway
      assert new_state.transaction_system_layout == original_state.transaction_system_layout
      assert new_state.read_version == original_state.read_version
      assert new_state.tx == original_state.tx
      assert new_state.stack == original_state.stack
    end

    test "works with different gateway references" do
      current_time = 70_000

      state =
        create_test_state(
          gateway: :custom_gateway,
          read_version: 54_321
        )

      gateway_fn = fn :custom_gateway, 54_321 -> {:ok, 8000} end
      time_fn = fn -> current_time end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      result = ReadVersions.renew_read_version_lease(state, opts)

      assert {:ok, new_state} = result
      assert new_state.read_version_lease_expiration == current_time + 8000
    end

    test "handles zero lease duration" do
      current_time = 80_000
      state = create_test_state(read_version: 12_345)

      gateway_fn = fn :test_gateway, 12_345 -> {:ok, 0} end
      time_fn = fn -> current_time end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      result = ReadVersions.renew_read_version_lease(state, opts)

      assert {:ok, new_state} = result
      assert new_state.read_version_lease_expiration == current_time
    end

    test "handles large time values" do
      current_time = 999_999_999_999
      state = create_test_state(read_version: 12_345)

      gateway_fn = fn :test_gateway, 12_345 -> {:ok, 10_000} end
      time_fn = fn -> current_time end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      result = ReadVersions.renew_read_version_lease(state, opts)

      assert {:ok, new_state} = result
      assert new_state.read_version_lease_expiration == current_time + 10_000
    end

    test "uses default functions when no opts provided" do
      state = create_test_state(read_version: 12_345)

      # This will fail with the real functions since we don't have real infrastructure
      result = ReadVersions.renew_read_version_lease(state)

      # Expect some kind of error since we don't have real gateway
      assert {:error, _} = result
    end
  end

  describe "edge cases and error conditions" do
    test "next_read_version with nil transaction_system_layout sequencer" do
      state = create_test_state()
      state = %{state | transaction_system_layout: %{sequencer: nil}}

      sequencer_fn = fn nil -> {:error, :no_sequencer} end
      gateway_fn = fn _, _ -> {:ok, 1000} end

      opts = [sequencer_fn: sequencer_fn, gateway_fn: gateway_fn]

      result = ReadVersions.next_read_version(state, opts)

      assert {:error, :no_sequencer} = result
    end

    test "renew_read_version_lease with nil read_version" do
      state = create_test_state(read_version: nil)

      gateway_fn = fn :test_gateway, nil -> {:error, :invalid_version} end
      time_fn = fn -> 50_000 end

      opts = [gateway_fn: gateway_fn, time_fn: time_fn]

      result = ReadVersions.renew_read_version_lease(state, opts)

      assert {:error, :invalid_version} = result
    end
  end
end
