defmodule SmolqueryVictoriaMetrics.Eval.ValueTest do
  use ExUnit.Case, async: true

  alias SmolqueryVictoriaMetrics.Eval.Value

  @inf 1.797_693_134_862_315_7e308

  test "inf/0, neg_inf/0 and inf?/1 read the largest double as the infinity" do
    assert Value.inf() == @inf
    assert Value.neg_inf() == -@inf
    assert Value.inf?(@inf) and Value.inf?(-@inf)
    refute Value.inf?(1.0e300) or Value.inf?(nil)
  end

  test "from_number/1 and clamp/1" do
    assert Value.from_number(:inf) == @inf
    assert Value.from_number(:neg_inf) == -@inf
    assert Value.from_number(:nan) == nil
    assert Value.from_number(2.5) == 2.5
    assert Value.clamp(1.0) == 1.0
  end

  describe "arithmetic" do
    test "a nil operand is nil" do
      for fun <- [
            &Value.add/2,
            &Value.sub/2,
            &Value.mul/2,
            &Value.divide/2,
            &Value.mod/2,
            &Value.atan2/2
          ] do
        assert fun.(nil, 1.0) == nil
        assert fun.(1.0, nil) == nil
      end
    end

    test "infinities follow IEEE rules" do
      assert Value.add(@inf, 1.0) == @inf
      assert Value.add(@inf, -@inf) == nil
      assert Value.sub(@inf, @inf) == nil
      assert Value.mul(@inf, 0.0) == nil
      assert Value.mul(-@inf, 2.0) == -@inf
      assert Value.divide(1.0, @inf) == 0.0
      assert Value.divide(@inf, @inf) == nil
    end

    test "an overflow is the infinity of its sign" do
      assert Value.add(1.0e308, 1.0e308) == @inf
      assert Value.mul(-1.0e200, 1.0e200) == -@inf
      assert Value.divide(1.0e308, 1.0e-10) == @inf
    end

    test "division by zero" do
      assert Value.divide(1.0, 0.0) == @inf
      assert Value.divide(-1.0, 0.0) == -@inf
      assert Value.divide(0.0, 0.0) == nil
    end

    test "division by a negative zero is the infinity of the other sign, as in Go" do
      assert Value.divide(1.0, -0.0) == -@inf
      assert Value.divide(-1.0, -0.0) == @inf
      assert Value.divide(@inf, -0.0) == -@inf
      assert Value.divide(-0.0, -0.0) == nil
    end

    test "mod/2 keeps the sign of the dividend, and is nil by zero" do
      assert Value.mod(5.0, 3.0) == 2.0
      assert Value.mod(-5.0, 3.0) == -2.0
      assert Value.mod(5.0, 0.0) == nil
      assert Value.mod(5.0, @inf) == 5.0
    end

    test "pow/2 as metricsql's Pow over Go's math.Pow" do
      assert Value.pow(2.0, 10.0) == 1024.0
      assert Value.pow(nil, 0.0) == nil
      assert Value.pow(5.0, 0.0) == 1.0
      assert Value.pow(1.0, nil) == 1.0
      assert Value.pow(2.0, nil) == nil
      assert Value.pow(-8.0, 0.5) == nil
      assert Value.pow(0.0, -1.0) == @inf
      assert Value.pow(10.0, 400.0) == @inf
      assert Value.pow(-10.0, 401.0) == -@inf
      assert Value.pow(0.5, @inf) == 0.0
      assert Value.pow(-@inf, 3.0) == -@inf
    end

    test "atan2/2" do
      assert_in_delta Value.atan2(1.0, 1.0), :math.pi() / 4, 1.0e-15
    end
  end

  test "comparisons treat NaN as VictoriaMetrics' binaryop does" do
    assert Value.eq?(nil, nil)
    refute Value.eq?(nil, 1.0)
    assert Value.neq?(nil, 1.0)

    refute Value.gt?(nil, 1.0) or Value.lt?(1.0, nil) or Value.gte?(nil, nil) or
             Value.lte?(nil, 0.0)

    assert Value.gt?(2.0, 1.0) and Value.lt?(1.0, 2.0) and Value.gte?(1.0, 1.0) and
             Value.lte?(1.0, 1.0)
  end

  test "sum/1, quantile/2, quantile_sorted/2, stdvar/1 and sqrt/1 skip nil" do
    assert Value.sum([1.0, nil, 2.0]) == 3.0
    assert Value.sum([nil]) == nil
    assert Value.quantile(0.5, [3.0, nil, 1.0, 2.0]) == 2.0
    assert Value.quantile(-1.0, [1.0]) == -@inf
    assert Value.quantile(2.0, [1.0]) == @inf
    assert Value.quantile(0.5, [nil]) == nil
    assert Value.quantile_sorted(0.25, [0.0, 4.0]) == 1.0
    assert Value.stdvar([1.0, 3.0, nil]) == 1.0
    assert Value.stdvar([nil]) == nil
    assert Value.stdvar([0.0, 1.0e200]) == @inf
    assert Value.stdvar([@inf, 1.0]) == nil
    assert Value.sqrt(-1.0) == nil
    assert Value.sqrt(@inf) == @inf
    assert Value.sqrt(4.0) == 2.0
  end

  test "format/1 writes Go's FormatFloat(v, 'f', -1, 64)" do
    assert Value.format(1.0) == "1"
    assert Value.format(0.1) == "0.1"
    assert Value.format(1.0e21) == "1000000000000000000000"
    assert Value.format(-1.5e-7) == "-0.00000015"
    assert Value.format(nil) == "NaN"
    assert Value.format(@inf) == "+Inf"
    assert Value.format(-@inf) == "-Inf"
  end

  test "format_general/1 writes Go's %g" do
    assert Value.format_general(0.5) == "0.5"
    assert Value.format_general(0.99) == "0.99"
    assert Value.format_general(100_000.0) == "100000"
    assert Value.format_general(1.0e6) == "1e+06"
    assert Value.format_general(1_234_567.0) == "1.234567e+06"
    assert Value.format_general(0.00001) == "1e-05"
    assert Value.format_general(-2.5e-5) == "-2.5e-05"
    assert Value.format_general(0.0) == "0"
    assert Value.format_general(nil) == "NaN"
  end

  test "parse/1 reads what Go's ParseFloat reads" do
    assert Value.parse("-12.34") == -12.34
    assert Value.parse("1e3") == 1000.0
    assert Value.parse(".5") == 0.5
    assert Value.parse("+Inf") == @inf
    assert Value.parse("-inf") == -@inf
    assert Value.parse("1.") == 1.0
    assert Value.parse("-1.") == -1.0
    assert Value.parse("1.e2") == 100.0
    assert Value.parse("1..") == nil
    assert Value.parse("12abc") == nil
    assert Value.parse("") == nil
  end

  describe "from_number/1 on a double read back from a frame" do
    test "Explorer's infinities and NaN read as the stored bounds and nil" do
      assert Value.from_number(:infinity) == Value.inf()
      assert Value.from_number(:neg_infinity) == Value.neg_inf()
      assert Value.from_number(:nan) == nil
      assert Value.from_number(2) == 2.0
    end
  end
end
