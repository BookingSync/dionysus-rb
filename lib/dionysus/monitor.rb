# frozen_string_literal: true

class Dionysus::Monitor < Dry::Monitor::Notifications
  EVENTS = %w[
    outbox_producer.started
    outbox_producer.stopped
    outbox_producer.shutting_down
    outbox_producer.error
    outbox_producer.publishing_failed
    outbox_producer.published
    outbox_producer.processing_topic
    outbox_producer.processed_topic
    outbox_producer.lock_exists_for_topic
    outbox_producer.heartbeat
  ].freeze

  private_constant :EVENTS

  def initialize
    super(:dionysus)
    EVENTS.each { |event| register_event(event) }
  end

  def subscribe(event)
    return super if events.include?(event.to_s)

    raise UnknownEventError.new(events, event)
  end

  def events
    EVENTS
  end

  class UnknownEventError < StandardError
    attr_reader :available_events, :current_event
    private     :available_events, :current_event

    def initialize(available_events, current_event)
      super()
      @available_events = available_events
      @current_event = current_event
    end

    def message
      "unknown event: #{current_event}, the available events are: #{available_events.join(", ")}"
    end
  end
end
