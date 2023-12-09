# frozen_string_literal: true

class Dionysus::Producer::Outbox::DuplicatesFilter
  def self.call(records_to_publish)
    new(records_to_publish).call
  end

  attr_reader :records_to_publish
  private     :records_to_publish

  def initialize(records_to_publish)
    @records_to_publish = records_to_publish
  end

  def call
    records_to_publish
      .slice_when { |record_1, record_2| generate_uniqueness_key(record_1) != generate_uniqueness_key(record_2) }
      .flat_map(&:last)
  end

  private

  def generate_uniqueness_key(record)
    [record.resource_class, record.resource_id, record.event_name, record.topic]
  end
end
