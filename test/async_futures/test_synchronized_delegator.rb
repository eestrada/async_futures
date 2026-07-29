# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures/synchronized_delegator'

class TestSynchronizedDelegator < Minitest::Test
  def setup
    @delegated = {}
    @delegator = AsyncFutures::SynchronizedDelegator.new(@delegated)
  end

  def test_method_missing
    assert_kind_of Hash, @delegator.to_h

    assert_same @delegated, @delegator.to_h

    assert_same @delegated, @delegator.method_missing(:to_h)
  end

  def test_respond_to
    # defined
    assert_respond_to @delegator, :method_missing

    # delegated
    assert_respond_to @delegator, :to_h

    # not defined or delegated
    refute_respond_to @delegator, :blah_blah
  end
end
