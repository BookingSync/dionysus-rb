# frozen_string_literal: true

class Dionysus::Producer::Outbox::DatadogTracer
  SERVICE_NAME = "dionysus_outbox_worker"
  private_constant :SERVICE_NAME

  def self.service_name
    SERVICE_NAME
  end

  def trace(event_name, topic)
    tracer.trace(event_name, span_type_key => "worker", service: self.class.service_name,
      on_error: error_handler) do |span|
      span.set_tag("topic", topic)

      yield
    end
  end

  private

  def tracer
    if Datadog.respond_to?(:tracer)
      Datadog.tracer
    else
      Datadog::Tracing
    end
  end

  # datadog 2.0 renamed the `span_type:` keyword of Datadog::Tracing#trace to `type:`.
  # DDTrace is only defined by the pre-2.0 ddtrace gem.
  def span_type_key
    if defined?(DDTrace)
      :span_type
    else
      :type
    end
  end

  def error_handler
    ->(span, error) { span.set_error(error) }
  end
end
