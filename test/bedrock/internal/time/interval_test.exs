defmodule Bedrock.Internal.Time.IntervalTest do
  use ExUnit.Case, async: true
  alias Bedrock.Internal.Time.Interval

  describe "from/2" do
    test "creates interval from valid unit and non-negative integer" do
      assert Interval.from(:second, 10) == {:second, 10}
    end

    test "raises error for invalid unit" do
      assert_raise FunctionClauseError, fn -> Interval.from(:invalid, 10) end
    end

    test "raises error for negative integer" do
      assert_raise FunctionClauseError, fn -> Interval.from(:second, -1) end
    end
  end

  describe "between/3" do
    test "calculates interval between two DateTime values" do
      start = ~U[2023-01-01T00:00:00Z]
      stop = ~U[2023-01-01T00:00:10Z]
      assert Interval.between(start, stop, :second) == {:second, 10}
    end

    test "defaults to seconds if unit is not provided" do
      start = ~U[2023-01-01T00:00:00Z]
      stop = ~U[2023-01-01T00:00:10Z]
      assert Interval.between(start, stop) == {:second, 10}
    end
  end

  describe "humanize/1" do
    test "humanizes interval with integer value" do
      assert Interval.humanize({:second, 10}) == "10s"
    end

    test "humanizes interval with float value" do
      assert Interval.humanize({:second, 10.5}) == "10.50s"
    end
  end

  describe "normalize/1" do
    test "scales down intervals correctly" do
      assert Interval.normalize({:week, 0.5}) == {:day, 3.5}
      assert Interval.normalize({:day, 0.5}) == {:hour, 12.0}
      assert Interval.normalize({:hour, 0.5}) == {:minute, 30.0}
      assert Interval.normalize({:minute, 0.5}) == {:second, 30.0}
      assert Interval.normalize({:second, 0.5}) == {:millisecond, 500.0}
      assert Interval.normalize({:millisecond, 0.5}) == {:microsecond, 500.0}
      assert Interval.normalize({:microsecond, 0.5}) == {:nanosecond, 500.0}
    end

    test "scales up intervals correctly" do
      assert Interval.normalize({:nanosecond, 1000}) == {:microsecond, 1}
      assert Interval.normalize({:microsecond, 1000}) == {:millisecond, 1}
      assert Interval.normalize({:millisecond, 1000}) == {:second, 1}
      assert Interval.normalize({:second, 60}) == {:minute, 1}
      assert Interval.normalize({:minute, 60}) == {:hour, 1}
      assert Interval.normalize({:hour, 24}) == {:day, 1}
      assert Interval.normalize({:day, 7}) == {:week, 1}
    end

    test "rounds float values correctly" do
      assert Interval.normalize({:second, 1.0}) == {:second, 1}
      assert Interval.normalize({:second, 1.5}) == {:second, 1.5}
    end
  end
end
