# frozen_string_literal: true

module Dionysus::Utils::KarafkaSentryListener
  class << self
    def on_error_occurred(event)
      Sentry.capture_exception(event[:error])
    end
  end
end
