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
      .map(&method(:to_event_data))
      .each(&method(:publish_via_event_bus))
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

  def publish_via_event_bus(event_data)
    config.event_bus.publish("dionysus.consume", event_data)
  end
end
