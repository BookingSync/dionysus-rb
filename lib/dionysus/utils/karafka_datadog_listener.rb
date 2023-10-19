# frozen_string_literal: true

class Dionysus::Utils::KarafkaDatadogListener
  class << self
    def on_error_occurred(event)
      span = tracer.active_span
      span&.set_error(event[:error])
    end

    private

    def tracer
      if Datadog.respond_to?(:tracer)
        Datadog.tracer
      else
        Datadog::Tracing
      end
    end
  end
end
