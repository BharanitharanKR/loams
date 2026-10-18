# frozen_string_literal: true

# The `loams` gem. A preview: the version is a RubyGems prerelease
# (0.1.0.pre.2 for the v0.1.0-preview.2 tag), so `gem install loams` does not
# pick it up without `--pre`. scripts/sdk/sdk-version.py keeps lib/loams/version.rb
# at the release tag; sdk-release.yml builds and publishes this file.
require_relative "lib/loams/version"

Gem::Specification.new do |spec|
  spec.name = "loams"
  spec.version = Loams::VERSION
  spec.authors = ["Ostrium Labs"]
  spec.email = ["security@ostriumlabs.com"]
  spec.summary = "Official Ruby client for Loams (preview)"
  spec.description = "The Loams Ruby SDK. Pre-release: the API may change between previews; pin the exact version."
  spec.homepage = "https://loams.dev"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1"

  spec.metadata = {
    "homepage_uri" => "https://loams.dev",
    "source_code_uri" => "https://github.com/ostrium-labs/loams/tree/main/sdks/ruby",
    "bug_tracker_uri" => "https://github.com/ostrium-labs/loams/issues",
  }

  spec.files = Dir["lib/**/*.rb"] + ["gen/loams-descriptor.binpb", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  spec.add_dependency "google-protobuf", ">= 4.29", "< 5"
end
