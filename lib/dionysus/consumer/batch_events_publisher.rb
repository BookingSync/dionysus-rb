# frozen_string_literal: true

class Dionysus::Consumer::BatchEventsPublisher
  attr_reader :config, :topic
  private     :config, :topic

  def initialize(config, topic)
    @config = config
    @topic = topic
  end

  def publish(processed_events)
    processed_events
      .map { |dionysus_event| to_event_data(dionysus_event) }
      .then { |mapped_events| publish_events_batch_via_event_bus(mapped_events) }
      .then { |mapped_events| mapped_events.each { |event_data| publish_single_event_via_event_bus(event_data) } }
  end

  private

  def to_event_data(dionysus_event)
    {
      topic_name: topic.to_s,
      event_name: dionysus_event.event_name,
      model_name: dionysus_event.model_name,
      transformed_data: dionysus_event.transformed_data.to_a,
      local_changes: dionysus_event.local_changes
    }
  end

  def publish_events_batch_via_event_bus(events_batch_data)
    config.event_bus.publish("dionysus.consume_batch", events_batch_data)
    events_batch_data
  end

  def publish_single_event_via_event_bus(event_data)
    config.event_bus.publish("dionysus.consume", event_data)
  end
end
