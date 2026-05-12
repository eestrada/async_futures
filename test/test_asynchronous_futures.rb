# frozen_string_literal: true

require_relative 'minitest_helper'

require 'async_futures'

class TestAsyncFutures < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::AsyncFutures::VERSION
  end
end
