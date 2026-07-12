# frozen_string_literal: true

require_relative 'lib/async_futures/version'

Gem::Specification.new do |spec|
  spec.name = 'async_futures'
  spec.version = AsyncFutures::VERSION
  spec.authors = ['Ethan Estrada']
  spec.email = ['ethan@misterfidget.com']

  spec.summary = 'A generic Future class for Ruby. Also includes Executor implementations for Ractors, Threads, and Fibers.' # rubocop:disable Layout/LineLength
  # spec.description = 'TODO: Write a longer description or delete this line.'
  spec.homepage = 'https://codeberg.org/eestrada/async_futures'
  spec.license = '0BSD'
  spec.required_ruby_version = '>= 3.3.0'
  spec.metadata['allowed_push_host'] = 'https://rubygems.org/'
  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://codeberg.org/eestrada/async_futures'
  spec.metadata['changelog_uri'] = 'https://codeberg.org/eestrada/async_futures/blob/main/CHANGELOG.md'
  spec.metadata['rubygems_mfa_required'] = 'true'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore test/ .github/ .rubocop.yml])
    end
  end
  spec.bindir = 'exe'
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
