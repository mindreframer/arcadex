defmodule Arcadex.ErrorTest do
  use ExUnit.Case, async: true

  alias Arcadex.Error

  describe "ARX001_1A: Error struct tests" do
    test "ARX001_1A_T5: message/1 formats correctly with message only" do
      error = %Error{status: 400, message: "Bad request", detail: nil}

      assert Error.message(error) == "Bad request"
      assert Exception.message(error) == "Bad request"
    end

    test "ARX001_1A_T5: message/1 formats correctly with message and detail" do
      error = %Error{status: 500, message: "Server error", detail: "Connection refused"}

      assert Error.message(error) == "Server error: Connection refused"
      assert Exception.message(error) == "Server error: Connection refused"
    end

    test "ARX001_1A_T5: message/1 formats correctly with detail only" do
      error = %Error{status: nil, message: nil, detail: "Some detail"}

      assert Error.message(error) == "Some detail"
    end

    test "ARX001_1A_T5: message/1 returns unknown error when both nil" do
      error = %Error{status: nil, message: nil, detail: nil}

      assert Error.message(error) == "Unknown error"
    end

    test "ARX001_1A_T5: Error can be raised" do
      assert_raise Error, "Test error", fn ->
        raise Error, message: "Test error"
      end
    end

    test "ARX001_1A_T5: Error can be raised with message and detail" do
      assert_raise Error, "Test error: with detail", fn ->
        raise Error, message: "Test error", detail: "with detail"
      end
    end
  end
end
