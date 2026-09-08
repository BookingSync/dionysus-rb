# frozen_string_literal: true

class Dionysus::Producer::EmbeddedTimestampRepair
  TIMESTAMP_ATTRIBUTE = "updated_at"
  PRIMARY_KEY_ATTRIBUTE = "id"
  ISO8601_WITH_OFFSET = %r{\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})\z}

  # Takes the block that serializes, so a repaired record can be serialized again in one place.
  def self.call(records, config, &)
    new(records, config).call(&)
  end

  attr_reader :records, :config
  private :records, :config

  def initialize(records, config)
    @records = records
    @config = config
  end

  def call
    payload, read_at = yield
    return [payload, read_at] unless config.touch_records_behind_their_embedded_records
    return [payload, read_at] unless payload.is_a?(Array)
    return [payload, read_at] unless any_repaired?(payload)

    yield
  end

  private

  # Pairing is positional, and the serializer is supplied by the application, so a payload that does
  # not line up 1:1 could touch one record with another record's children. Fail closed instead.
  # map, not any? - any? short-circuits and would leave the rest of a batch unrepaired
  def any_repaired?(payload)
    return false unless payload.size == Array(records).size

    Array(records).zip(payload).map { |record, record_payload| repaired?(record, record_payload) }.any?
  end

  # A parent whose own timestamp predates a record embedded in it publishes a payload that describes
  # no single moment, and the consumer discards it with its children. Correcting the row keeps the
  # published timestamp equal to the row's own, which is what the republish path relies on.
  def repaired?(record, record_payload)
    return false unless repairable?(record, record_payload)

    published_at = coerce_time(attribute(record_payload, TIMESTAMP_ATTRIBUTE))
    latest = embedded_timestamps(record_payload).max
    return false unless published_at && latest && latest > published_at

    # WHERE guards the write: a stale read must never move the row backwards. When the row is already
    # ahead nothing is written, and the reload alone repairs the payload.
    updated = record.class.where(record.class.primary_key => record.id)
      .where("#{record.class.quoted_table_name}.#{TIMESTAMP_ATTRIBUTE} < ?", latest)
      .update_all(TIMESTAMP_ATTRIBUTE => latest)
    # the row can be deleted between the guard and here, and raising would abort the whole batch
    record.reload
    instrument(record, updated)
    true
  rescue ActiveRecord::RecordNotFound
    false
  end

  def repairable?(record, record_payload)
    record.is_a?(ActiveRecord::Base) && record.persisted? &&
      record_payload.is_a?(Hash) && attribute(record_payload, PRIMARY_KEY_ATTRIBUTE) == record.id
  end

  def embedded_timestamps(record_payload)
    record_payload.each_value.flat_map do |value|
      Array.wrap(value).filter_map do |embedded|
        next unless embedded.is_a?(Hash)

        coerce_time(attribute(embedded, TIMESTAMP_ATTRIBUTE))
      end
    end
  end

  def instrument(record, updated)
    config.instrumenter.increment("dionysus.publish.timestamp_repair",
      tags: ["model:#{record.class}", "outcome:#{updated.zero? ? "stale_read" : "row_behind"}"])
  end

  # A serializer may key its payload either way, and may render the timestamp it is about to publish.
  def attribute(record_payload, name)
    record_payload.key?(name) ? record_payload[name] : record_payload[name.to_sym]
  end

  def coerce_time(value)
    case value
    when ActiveSupport::TimeWithZone, Time then value
    when String then parse_iso8601(value)
    end
  end

  def parse_iso8601(value)
    return nil unless ISO8601_WITH_OFFSET.match?(value)

    time = Time.iso8601(value)
    # Time.iso8601 normalizes an impossible date: 2026-09-31 becomes October 1.
    return nil unless value.start_with?(time.strftime("%Y-%m-%d"))

    time.in_time_zone
  rescue ArgumentError
    nil
  end
end
