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
    records
  end

  private

  delegate :outbox_model, to: :config

  def resolve_records_to_publish(records)
    return records unless config.remove_consecutive_duplicates_before_publishing

    Dionysus::Producer::Outbox::DuplicatesFilter.call(records)
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
