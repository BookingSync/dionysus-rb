# frozen_string_literal: true

class Dionysus::Producer::EmbeddedTimestampRepair
  TIMESTAMP_ATTRIBUTE = "updated_at"
  PRIMARY_KEY_ATTRIBUTE = "id"

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

    latest = embedded_timestamps(record_payload).max
    return false unless latest && latest > record_payload[TIMESTAMP_ATTRIBUTE]

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
      record_payload.is_a?(Hash) && record_payload[PRIMARY_KEY_ATTRIBUTE] == record.id &&
      time_like?(record_payload[TIMESTAMP_ATTRIBUTE])
  end

  def embedded_timestamps(record_payload)
    record_payload.each_value.flat_map do |value|
      Array.wrap(value).filter_map do |embedded|
        next unless embedded.is_a?(Hash)

        timestamp = embedded[TIMESTAMP_ATTRIBUTE]
        timestamp if time_like?(timestamp)
      end
    end
  end

  def instrument(record, updated)
    config.instrumenter.increment("dionysus.publish.timestamp_repair",
      tags: ["model:#{record.class}", "outcome:#{updated.zero? ? "stale_read" : "row_behind"}"])
  end

  def time_like?(value)
    value.respond_to?(:acts_like_time?) && value.acts_like_time?
  end
end
