# AsyncFutures

## Launch asynchronous tasks

[![Gem Version](https://badge.fury.io/rb/async_futures.svg?icon=si%3Arubygems)](https://badge.fury.io/rb/async_futures)
[![CI Status](https://github.com/eestrada/async_futures/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/eestrada/async_futures/actions/workflows/main.yml)

This library is heavily inspired by Python's `concurrent.futures` module.
It's API (mostly) follows the same one as the Python
[library](https://docs.python.org/3.13/library/concurrent.futures.html).
There are some differences to make it more Ruby-ish where it makes sense
(e.g. taking blocks instead of callable parameters, etc).
There are also separate submission methods
for concurrent versus non-concurrent tasks
for reasons explained later.

It has a different name for several reasons:

1. The [concurrent-ruby](https://rubygems.org/gems/concurrent-ruby) library
   already exists and is very popular.
   Naming this library `concurrent-futures`
   would place it under the same `Concurrent` module namespace.
   This would be confusing,
   since this project isn't associated with that one.
2. This library does not _require_ that `Executor` implementations support concurrency,
   only asynchrony.
   (See Loris Cro's excellent article
   [Asynchrony is not Concurrency](https://kristoff.it/blog/asynchrony-is-not-concurrency/)
   to understand the way these terms are used in this project's documentation).
   Consequently, this library implements (and supports) `Executor` implementations
   that conform to an asynchronous interface,
   but can in reality run immediately in synchronous modes.
   This is still logically correct based on Loris Cro's definition of asynchrony:
   the possibility for tasks to run out of order
   and still be correct.
   This means tasks run strictly in order
   (i.e. synchronously)
   are _also_ correct.
3. The more straightforward gem names [future](https://rubygems.org/gems/future)
   and [futures](https://rubygems.org/gems/futures)
   were already taken.

This Gem has multiple `Executor` implementations for creating `Future` instances
for tasks executed by `Ractor`, `Thread`, and `Fiber` concurrency primitives.
Users of the library can easily test out the performance differences
of primitives while only changing their code minimally.

Although the base `Executor` module is meant to be used as a mixin interface,
the module can also be run directly
to have a synchronous `Executor` implementation
that runs code immediately
and returns a completed future at the point of submission.
Although this may seem pointless, it has the benefit
that users of this library can trivially change their code execution
from serial to concurrent and back again
simply by using different `Executor` implementations.
In other words, their code does not require multiple complicated code paths
for correctness: it need only supply a different `Executor` instance
to get different performance
(assuming their code logic supports asynchrony
and doesn't require concurrency for correctness).

### Why wouldn't I just use `concurrent-ruby` for concurrency?

`concurrent-ruby` is a good library.
It is mainly focused on `Thread` primitives and thread safety.
If that is all you want/need, then you should use it.

The focus of this library is different.
This is meant to be a uniform
(albeit simple)
interface around _all_ concurrency/async primitives offered by Ruby.
You can indicate async versus concurrent intent
using the `submit` versus `submit_concurrent` methods
on `Executor` implementations.
It should also be possible to use the `Future` class
for things like event based libraries (i.e. async)
that were not intended to be used in this way.
Thus the `Executor` interface is not required
for the use of async futures.

## Installation

Install the gem and add to the application's Gemfile by executing:

```bash
bundle add async_futures
```

If bundler is not being used to manage dependencies, install the gem by executing:

```bash
gem install async_futures
```

## Usage

The test suite and benchmark script
are probably the best place to see example usage.

However, here are some quick introductory examples:


```ruby
require 'async_futures'

# Base `Executor` mixin module (inline execution):
bexec = AsyncFutures::Executor
ftr1 = bexec.submit('world') { |subject| 'hello ' + subject.to_s }
final_string = ftr1.result

begin
   ftr1 = bexec.submit_concurrent('world') { |subject| 'hello ' + subject.to_s }
rescue AsyncFutures::NoConcurrencyError
   # This will always trigger
end

subjects = ['Alice', 'Bob', 'Charlie']
final_strings = bexec.map(subjects) { |subject| 'hello ' + subject.to_s }.to_a

# `ThreadExecutor`:
texec = AsyncFutures::ThreadExecutor.new
ftr1 = texec.submit('world') { |subject| 'hello ' + subject.to_s }
final_string = ftr1.result

begin
   ftr1 = texec.submit_concurrent('world') { |subject| 'hello ' + subject.to_s }
rescue AsyncFutures::NoConcurrencyError
   # This will not trigger by default
   # but may trigger based on some initialize arguments.
   # See docs for details.
end

subjects = ['Alice', 'Bob', 'Charlie']
final_strings = texec.map(subjects) { |subject| 'hello ' + subject.to_s }.to_a

# `FiberExecutor`:
Fiber.set_scheduler(some_scheduler)
fexec = AsyncFutures::FiberExecutor.new

# It isn't necessary to submit within a scheduled fiber
# but if we want to join in the same thread
# we need to run within the scheduler.
# However, cross thread joining is safe.
Fiber.schedule do
   ftr1 = fexec.submit('world') { |subject| 'hello ' + subject.to_s }
   final_string = ftr1.result

   begin
      ftr1 = fexec.submit_concurrent('world') { |subject| 'hello ' + subject.to_s }
   rescue AsyncFutures::NoConcurrencyError
      # This will trigger by default
      # but can be suppressed by an initialize argument.
      # See docs for details.
   end

   subjects = ['Alice', 'Bob', 'Charlie']
   final_strings = fexec.map(subjects) { |subject| 'hello ' + subject.to_s }.to_a
end

# Unsetting the scheduler causes it to run to completion.
# Otherwise, we may need to wait until process exit
# for it to fully finish running.
Fiber.set_scheduler(nil)

# `RactorExecutor`:

# This must be explicitly required
# since it doesn't run on anything prior to Ruby version 4.x
# (the API changed a lot between versions so it only targets 4.x for now).
# Also, the Java based VMs (JRuby and TruffleRuby)
# don't support Ractor primitives *at all* yet.
require 'async_futures/ractor_executor'

rexec = AsyncFutures::RactorExecutor.new
ftr1 = rexec.submit('world') { |subject| 'hello ' + subject.to_s }
final_string = ftr1.result

begin
   ftr1 = rexec.submit_concurrent('world') { |subject| 'hello ' + subject.to_s }
rescue AsyncFutures::NoConcurrencyError
   # This will not currently trigger.
   # This may change in a future version to be more like ThreadExecutor.
end

subjects = ['Alice', 'Bob', 'Charlie']
final_strings = rexec.map(subjects) { |subject| 'hello ' + subject.to_s }.to_a
```

## Development

After checking out the repo, run `bin/setup` to install dependencies.
Then, run `rake test` to run the tests.
You can also run `bin/console` for an interactive prompt
that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.
To release a new version, update the version number in `version.rb`,
and then run `bundle exec rake release`,
which will create a git tag for the version,
push git commits and the created tag,
and push the `.gem` file to [rubygems.org](https://rubygems.org).

### Benchmarks

There is a simple benchmarking script in this repo at `bin/benchmark`.
It takes no command line arguments.
Just read it,
then run it
to see the difference in executor performance
on different types of workloads.

### Documentation

The documentation in this repo uses [Semantic Line breaks](https://sembr.org/).
If you contribute documentation changes, please follow the same convention.

## Contributing

Bug reports and pull requests are welcome on Codeberg at
<https://codeberg.org/eestrada/async_futures>.
