# frozen_string_literal: true

require_relative 'minitest_helper'

require 'set' # rubocop:disable Lint/RedundantRequireStatement
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

  def test_blocks
    @delegator[:one] = true
    @delegator[:two] = false

    assert_equal 2, @delegator.size

    @delegator.delete_if { |_key, value| !value }

    assert_equal 1, @delegator.size

    assert_includes @delegator, :one
  end

  def test_set_dup
    delegated_set = Set.new
    delegator = AsyncFutures::SynchronizedDelegator.new(delegated_set)

    assert_same delegated_set, delegator.to_set
    refute_same delegated_set, delegator.dup

    # It does dup on the delegator,
    # so a Delegator is returned.
    refute_kind_of Set, delegator.dup
    assert_kind_of AsyncFutures::SynchronizedDelegator, delegator.dup

    # But it does a deep dup,
    # so the internal set is also duped.
    refute_same delegated_set, delegator.dup.to_set
  end
end
