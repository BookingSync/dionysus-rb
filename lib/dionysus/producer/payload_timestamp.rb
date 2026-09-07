# frozen_string_literal: true

class Dionysus::Producer::PayloadTimestamp
  TIMESTAMP_ATTRIBUTE = "updated_at"

  def self.apply(payload, config)
    new(payload, config).apply
  end

  attr_reader :payload, :config
  private :payload, :config

  def initialize(payload, config)
    @payload = payload
    @config = config
  end

  def apply
    return payload unless config.stamp_payload_with_embedded_timestamps
    return payload unless payload.is_a?(Array)

    payload.each { |record_payload| stamp(record_payload) }
  end

  private

  # A payload built across a write carries children newer than its own timestamp, and the
  # consumer discards it with them. publish_consistent_snapshots cannot see that.
  def stamp(record_payload)
    return unless record_payload.is_a?(Hash)

    own = record_payload[TIMESTAMP_ATTRIBUTE]
    return unless time_like?(own)

    latest = embedded_timestamps(record_payload).max
    record_payload[TIMESTAMP_ATTRIBUTE] = latest if latest && latest > own
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

  def time_like?(value)
    value.respond_to?(:acts_like_time?) && value.acts_like_time?
  end
end
