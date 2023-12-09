# frozen_string_literal: true

class Dionysus::Producer::Genesis::Streamer::StandardJob < Dionysus::Producer::Genesis::Streamer::BaseJob
  DESTROYED_EVENT_TYPE = "destroyed"
  UPDATED_EVENT_TYPE = "updated"
  GENESIS_TOPIC_SUFFIX = "genesis"

  private

  delegate :configuration, :outbox_model, :outbox_publisher, to: Dionysus::Producer
  delegate :outbox_model, to: :configuration

  def call(item, topic)
    publishable = Dionysus::Producer::Outbox::Publishable.new(item)
    outbox_record = Dionysus::Producer.configuration.outbox_model.new(
      resource_class: publishable.model_name.to_s,
      resource_id: publishable.publishable_id,
      event_name: event_name_for(publishable),
      topic: topic
    )

    options = {}
    options[:genesis_only] = true if genesis_only?(topic)

    outbox_publisher.publish(outbox_record, options)
  end

  def event_name_for(publishable)
    Dionysus::Producer::Outbox::EventName
      .new(publishable.resource_name)
      .for_event_type(event_type(publishable))
  end

  def event_type(publishable)
    return DESTROYED_EVENT_TYPE if publishable.soft_deletable? && publishable.soft_deleted?

    UPDATED_EVENT_TYPE
  end

  def genesis_only?(topic)
    topic.ends_with?(GENESIS_TOPIC_SUFFIX)
  end
end
