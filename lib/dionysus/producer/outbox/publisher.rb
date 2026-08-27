# frozen_string_literal: true

class Dionysus::Producer::Outbox::Publisher
  attr_reader :config
  private :config

  def initialize(config:)
    @config = config
  end

  def publish(outbox_record, options = {})
    return if Dionysus::Producer::Suppressor.suppressed?

    with_consistent_reads do
      instrument("publishing_with_dionysus") do
        resource_class = outbox_record.resource_class.constantize
        primary_key = resource_class.primary_key
        primary_key_value = outbox_record.resource_id
        topic = outbox_record.topic
        resource = resource_class.find_by(primary_key => primary_key_value) ||
          resource_class.new(primary_key => primary_key_value)
        event_name = outbox_record.event_name
        if resource.new_record? && outbox_record.created_event?
          logger.error(
            "Attempted to publish #{resource.class}, id: #{resource.id} but it was deleted, that should never happen!"
          )
          return
        end

        if resource.new_record? && outbox_record.updated_event?
          logger.error(
            "There was an update of #{resource.class}, id: #{resource.id} but it was deleted, that should never happen!"
          )
          return
        end

        published_count = publish_for_top_level_resource(outbox_record, resource, event_name, topic, options) +
          publish_for_dependency(resource, topic, options)
        handle_nothing_published(resource, event_name, topic) if published_count.zero?
        published_count
      end
    end
  end

  def publish_observers(outbox_record)
    return if Dionysus::Producer::Suppressor.suppressed?

    with_consistent_reads do
      instrument("publishing_observers_with_dionysus") do
        resource_class = outbox_record.resource_class.constantize
        primary_key = resource_class.primary_key
        primary_key_value = outbox_record.resource_id
        resource = resource_class.find_by(primary_key => primary_key_value) ||
          resource_class.new(primary_key => primary_key_value)
        changeset = outbox_record.transformed_changeset

        Dionysus::Producer.observers_with_responders_for(resource,
          changeset).each do |observers, responder|
          if observers.count > config.observers_inline_maximum_size
            execute_genesis_for_observers(observers, responder)
          else
            observers.each { |observer_record| publish_observer(observer_record, responder) }
          end
        end
      end
    end
  end

  private

  # The payload is built from the record and its associations. Publishing runs inside the request
  # when publish_after_commit is on, where Rails' query cache is live on that connection, so a row
  # read earlier in the request can be served from the cache while its associations are queried
  # fresh - producing a payload whose updated_at is older than the data it carries. Consumers rank
  # and guard on updated_at, so such a payload is silently discarded, taking its embedded
  # associations with it.
  def with_consistent_reads(&)
    return yield unless config.publish_with_uncached_reads

    ActiveRecord::Base.uncached(&)
  end

  delegate :instrumenter, :error_handler, to: :config
  delegate :instrument, to: :instrumenter
  delegate :logger, to: Dionysus

  def publish_for_top_level_resource(outbox_record, resource, event_name, topic, options)
    Dionysus::Producer.responders_for_model_for_topic(resource.class, topic).sum do |responder|
      partition_key = outbox_record.partition_key.presence || Dionysus::Producer::PartitionKey.new(
        resource
      ).to_key(responder: responder)
      key = Dionysus::Producer::Key.new(resource).to_key

      responder.call(generate_message(event_name, resource), options.merge(partition_key: partition_key, key: key))
      1
    end
  end

  def publish_for_dependency(resource, topic, options)
    Dionysus::Producer.responders_for_dependency_parent_for_topic(resource.class,
      topic).sum do |parent_klass, responder|
      parent_event_name = Dionysus::Producer::Outbox::EventName.new(
        parent_klass.model_name.singular
      ).updated

      if resource.class.reflect_on_association(parent_klass.model_name.singular)
        parent_records = [resource.public_send(parent_klass.model_name.singular)]
      elsif resource.class.reflect_on_association(parent_klass.model_name.plural)
        parent_records = resource.public_send(parent_klass.model_name.plural)
      else
        next 0
      end

      parent_records = parent_records.to_a.compact
      example_parent_record = parent_records.first or next 0
      partition_key = Dionysus::Producer::PartitionKey.new(example_parent_record, config: config)
        .to_key(responder: responder)
      key = Dionysus::Producer::Key.new(example_parent_record).to_key

      parent_records.each do |parent_record|
        responder.call(generate_message(parent_event_name, parent_record),
          options.merge(partition_key: partition_key, key: key))
      end
      parent_records.size
    end
  end

  # Deliberately does not raise: a legitimately deleted parent is indistinguishable from
  # one that is not visible yet, and raising on the former would retry forever.
  def handle_nothing_published(resource, event_name, topic)
    message = "Published nothing for #{resource.class}, id: #{resource.id}, event: #{event_name}, " \
              "topic: #{topic} - no responder matched and no parent could be resolved. " \
              "Make sure it's processed later (e.g. by directly doing it from console)."
    logger.error(message)
    instrumenter.increment("dionysus.publish.nothing_published",
      tags: ["resource:#{resource.class}", "topic:#{topic}"])
    error_handler.capture_message(message)
  end

  def publish_observer(observer_record, responder)
    publishable = Dionysus::Producer::Outbox::Publishable.new(observer_record)
    partition_key = Dionysus::Producer::PartitionKey.new(publishable).to_key(responder: responder)
    key = Dionysus::Producer::Key.new(publishable).to_key
    event_name = Dionysus::Producer::Outbox::EventName.new(publishable.resource_name).updated

    responder.call(generate_message(event_name, publishable), partition_key: partition_key, key: key)
  end

  def generate_message(event_name, resource)
    [[event_name, resource, {}]]
  end

  def execute_genesis_for_observers(observers, responder)
    resource_class = observers.first.class
    primary_key = resource_class.primary_key

    Dionysus::Producer::Genesis::Streamer::StandardJob.enqueue(
      resource_class.where(primary_key => observers),
      resource_class,
      responder.primary_topic,
      number_of_days: 0.1
    )
  end
end
