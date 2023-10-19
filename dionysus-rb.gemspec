# frozen_string_literal: true

require_relative "lib/dionysus/version"

Gem::Specification.new do |spec|
  spec.name = "dionysus-rb"
  spec.version = Dionysus::VERSION
  spec.authors = ["Karol Galanciak"]
  spec.email = ["karol.galanciak@gmail.com", "karol@bookingsync.com", "dev@bookingsync.com"]

  spec.summary = "A framework on top of Karafka for data transfer/Change Data Capture between apps."
  spec.description = "A framework on top of Karafka for data transfer/Change Data Capture between apps."
  spec.homepage = "https://github.com/BookingSync/dionysus-rb"
  spec.license = "MIT"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/BookingSync/dionysus-rb"
  spec.metadata["changelog_uri"] = "https://github.com/BookingSync/dionysus-rb/blob/master/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    %x(git ls-files -z).split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "bin"
  spec.executables   = %w[karafka_health_check outbox_worker_health_check]
  spec.require_paths = ["lib"]

  spec.add_development_dependency "activerecord", "7.0.0"
  spec.add_development_dependency "bundler"
  spec.add_development_dependency "byebug"
  spec.add_development_dependency "crypt_keeper"
  spec.add_development_dependency "ddtrace"
  spec.add_development_dependency "dogstatsd-ruby"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "redis"
  spec.add_development_dependency "redis-namespace"
  spec.add_development_dependency "redlock"
  spec.add_development_dependency "rspec-sidekiq"
  spec.add_development_dependency "rubocop"
  spec.add_development_dependency "rubocop-performance"
  spec.add_development_dependency "rubocop-rake"
  spec.add_development_dependency "rubocop-rspec"
  spec.add_development_dependency "sentry-ruby"
  spec.add_development_dependency "shoulda-matchers"
  spec.add_development_dependency "timecop"

  spec.add_dependency "activerecord", ">= 5"
  spec.add_dependency "activesupport", ">= 3.2"
  spec.add_dependency "concurrent-ruby"
  spec.add_dependency "dry-monitor"
  spec.add_dependency "file-based-healthcheck"
  spec.add_dependency "hermes-rb"
  spec.add_dependency "karafka", "~> 2.0"
  spec.add_dependency "sidekiq"
  spec.add_dependency "sidekiq-cron"
  spec.add_dependency "sigurd"
  spec.add_dependency "waterdrop", "~> 2.0"

  spec.required_ruby_version = ">= 2.7"
  spec.metadata["rubygems_mfa_required"] = "true"
end
