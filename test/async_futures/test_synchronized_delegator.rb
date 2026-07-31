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

    # But it does dup the internal object,
    # so the internal set is also duped.
    refute_same delegated_set, delegator.clone.to_set
  end

  def test_set_frozen_clone
    delegated_set = Set.new
    delegator = AsyncFutures::SynchronizedDelegator.new(delegated_set)

    assert_same delegated_set, delegator.to_set
    refute_same delegated_set, delegator.clone(freeze: true)

    # It does dup on the delegator,
    # so a Delegator is returned.
    refute_kind_of Set, delegator.clone(freeze: true)
    assert_kind_of AsyncFutures::SynchronizedDelegator, delegator.clone(freeze: true)

    # But it does clone the internal object,
    # so the internal set is also cloned.
    refute_same delegated_set, delegator.clone(freeze: true).to_set

    refute_predicate delegator, :frozen?
    assert_predicate delegator.clone(freeze: true), :frozen?
  end

  def test_set_clone
    delegated_set = Set.new
    delegator = AsyncFutures::SynchronizedDelegator.new(delegated_set)

    assert_same delegated_set, delegator.to_set
    refute_same delegated_set, delegator.clone

    # It does dup on the delegator,
    # so a Delegator is returned.
    refute_kind_of Set, delegator.clone
    assert_kind_of AsyncFutures::SynchronizedDelegator, delegator.clone

    # But it does clone the internal object,
    # so the internal set is also cloned.
    refute_same delegated_set, delegator.clone.to_set
  end

  def test_clone_dup_freeze_mutex # rubocop:disable Metrics/AbcSize
    delegated_set = Set.new
    d1 = AsyncFutures::SynchronizedDelegator.new(delegated_set)

    d2 = d1.dup
    d3 = d1.clone
    d4 = d1.clone.freeze.clone
    d5 = d1.clone(freeze: true)
    d6 = d5.clone
    d7 = d6.dup

    refute_same d2.instance_variable_get(:@mutex), d1.instance_variable_get(:@mutex)
    refute_same d3.instance_variable_get(:@mutex), d1.instance_variable_get(:@mutex)
    refute_same d4.instance_variable_get(:@mutex), d1.instance_variable_get(:@mutex)
    refute_same d5.instance_variable_get(:@mutex), d1.instance_variable_get(:@mutex)
    refute_same d6.instance_variable_get(:@mutex), d5.instance_variable_get(:@mutex)
    refute_same d7.instance_variable_get(:@mutex), d6.instance_variable_get(:@mutex)

    refute_predicate d1.__getobj__, :frozen?
    refute_predicate d1.instance_variable_get(:@mutex), :frozen?

    refute_predicate d2.__getobj__, :frozen?
    refute_predicate d2.instance_variable_get(:@mutex), :frozen?

    refute_predicate d3.__getobj__, :frozen?
    refute_predicate d3.instance_variable_get(:@mutex), :frozen?

    assert_predicate d4.__getobj__, :frozen?
    assert_predicate d4.instance_variable_get(:@mutex), :frozen?

    assert_predicate d5.__getobj__, :frozen?
    assert_predicate d5.instance_variable_get(:@mutex), :frozen?

    assert_predicate d6.__getobj__, :frozen?
    assert_predicate d6.instance_variable_get(:@mutex), :frozen?

    refute_predicate d7.__getobj__, :frozen?
    refute_predicate d7.instance_variable_get(:@mutex), :frozen?
  end
end
