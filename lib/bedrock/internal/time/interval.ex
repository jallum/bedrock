defmodule Bedrock.Internal.Time.Interval do
  @units [:week, :day, :hour, :minute, :second, :millisecond, :microsecond, :nanosecond]
  @type unit ::
          :week
          | :day
          | :hour
          | :minute
          | :second
          | :millisecond
          | :microsecond
          | :nanosecond

  @type t :: {unit(), n :: non_neg_integer()}

  def from(unit, n) when is_integer(n) and n >= 0 and unit in @units, do: {unit, n}

  @spec between(DateTime.t(), DateTime.t(), unit()) :: t()
  def between(%DateTime{} = start, %DateTime{} = stop, unit \\ :second),
    do: from(unit, abs(DateTime.diff(stop, start, unit)))

  @spec humanize({unit(), n :: number()}) :: String.t()
  def humanize({_unit, n} = i) when n >= 0, do: i |> normalize() |> do_humanize()

  # Scale down
  def normalize({:week, w}) when w < 1.0, do: normalize({:day, w * 7})
  def normalize({:day, d}) when d < 1.0, do: normalize({:hour, d * 24})
  def normalize({:hour, h}) when h < 1.0, do: normalize({:minute, h * 60})
  def normalize({:minute, m}) when m < 1.0, do: normalize({:second, m * 60})
  def normalize({:second, s}) when s < 1.0, do: normalize({:millisecond, s * 1000})
  def normalize({:millisecond, ms}) when ms < 1.0, do: normalize({:microsecond, ms * 1000})
  def normalize({:microsecond, us}) when us < 1.0, do: normalize({:nanosecond, us * 1000})

  # Scale up
  def normalize({:nanosecond, ns}) when ns >= 1000, do: normalize({:microsecond, div(ns, 1000)})
  def normalize({:microsecond, us}) when us >= 1000, do: normalize({:millisecond, div(us, 1000)})
  def normalize({:millisecond, ms}) when ms >= 1000, do: normalize({:second, div(ms, 1000)})
  def normalize({:second, s}) when s >= 60, do: normalize({:minute, div(s, 60)})
  def normalize({:minute, m}) when m >= 60, do: normalize({:hour, div(m, 60)})
  def normalize({:hour, h}) when h >= 24, do: normalize({:day, div(h, 24)})
  def normalize({:day, d}) when d >= 7, do: normalize({:week, div(d, 7)})

  # Perfect
  def normalize({unit, n}) when is_float(n) do
    round_n = round(n)

    if n == round_n do
      {unit, round_n}
    else
      {unit, n}
    end
  end

  def normalize(perfect = {_unit, n}) when is_integer(n), do: perfect

  defp do_humanize({unit, n}) when is_integer(n),
    do: "#{n}#{unit_abbreviation(unit)}"

  defp do_humanize({unit, n}) when is_float(n),
    do: "#{:io_lib.format("~.2f", [n]) |> List.to_string()}#{unit_abbreviation(unit)}"

  def unit_abbreviation(:nanosecond), do: "ns"
  def unit_abbreviation(:microsecond), do: "μs"
  def unit_abbreviation(:millisecond), do: "ms"
  def unit_abbreviation(:second), do: "s"
  def unit_abbreviation(:minute), do: "m"
  def unit_abbreviation(:hour), do: "h"
  def unit_abbreviation(:day), do: "d"
  def unit_abbreviation(:week), do: "w"
end
