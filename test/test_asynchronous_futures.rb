# frozen_string_literal: true

require_relative 'test_helper'

require 'asynchronous_futures'

class TestAsynchronousFutures < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::AsynchronousFutures::VERSION
  end

  def test_it_does_something_useful
    skip 'nothing to test yet'

    assert false # rubocop:disable Minitest/UselessAssertion
  end
end
