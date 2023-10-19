# frozen_string_literal: true

class Dionysus::Utils::NullRetryProvider
  def self.retry
    yield
  end
end
