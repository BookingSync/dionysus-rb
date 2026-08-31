# frozen_string_literal: true

class Dionysus::Producer::Outbox::RecordsProcessor
  attr_reader :config
  private :config

  def initialize(config: Dionysus::Producer.configuration)
    @config = config
  end

  def call(records)
    failed_records = []
    records_to_publish = resolve_records_to_publish(records)
    records_to_publish.each do |record|
      begin
        publish(record)
      rescue => e
        record.handle_error(e)
        record.save!
        failed_records << record
      end
      yield record if block_given?
    end
    published_records = records - failed_records
    mark_as_published(published_records)
    schedule_republishes(records, failed_records)
    records
  end

  private

  delegate :outbox_model, to: :config

  def resolve_records_to_publish(records)
    return records unless config.remove_consecutive_duplicates_before_publishing

    Dionysus::Producer::Outbox::DuplicatesFilter.call(records)
  end

  # `retry_at` rather than a future `created_at`: it is honoured exactly, whereas `created_at` is
  # offset by `outbox_worker_publishing_delay` and feeds `publishing_latency`.
  def schedule_republishes(records, failed_records)
    return unless config.republish_deduplicated_records
    return unless config.remove_consecutive_duplicates_before_publishing

    republish_at = Time.current + config.republish_deduplicated_records_delay
    deduplicated = Dionysus::Producer::Outbox::DuplicatesFilter.deduplicated_records(records)

    (deduplicated - failed_records).each do |record|
      next if record.observer?

      outbox_model.create!(
        resource_class: record.resource_class,
        resource_id: record.resource_id,
        event_name: record.event_name,
        partition_key: record.partition_key,
        topic: record.topic,
        retry_at: republish_at
      )
    end
  end

  def publish(record)
    if record.observer?
      outbox_publisher.publish_observers(record)
    else
      outbox_publisher.publish(record)
    end
  end

  def outbox_publisher
    @outbox_publisher ||= Dionysus::Producer.outbox_publisher
  end

  def mark_as_published(published_records)
    outbox_model
      .where(id: published_records)
      .update_all(published_at: Time.current, error_class: nil, error_message: nil, failed_at: nil, retry_at: nil)
  end
end
