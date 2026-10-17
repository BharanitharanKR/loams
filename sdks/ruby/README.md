# Loams Ruby SDK (preview)

> **Pre-release (preview).** This SDK is published as a preview of Loams. Its API
> can change between previews without notice: pin the exact version, and expect
> breaking changes until 1.0. Report problems at
> <https://github.com/ostrium-labs/loams/issues>.

## Install

```sh
gem install loams --pre
```

or, in a `Gemfile`, pin the preview exactly:

```ruby
gem "loams", "0.1.0.pre.2"
```

RubyGems spells the `v0.1.0-preview.2` release as `0.1.0.pre.2`. The gem needs
Ruby 3.1 or newer and `google-protobuf` 4.x.

## Use

```ruby
require "loams"

client = Loams.client("https://acme.loams.dev")
```

## Develop

```sh
bundle install
bundle exec rake test   # the conformance suite
gem build loams.gemspec # what sdk-release.yml publishes
```

Licensed under Apache-2.0 (see `LICENSE`).
