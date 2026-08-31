# frozen_string_literal: true

class Dionysus::Producer::Outbox::DuplicatesFilter
  def self.call(records_to_publish)
    new(records_to_publish).call
  end

  def self.deduplicated_records(records_to_publish)
    new(records_to_publish).deduplicated_records
  end

  attr_reader :records_to_publish
  private     :records_to_publish

  def initialize(records_to_publish)
    @records_to_publish = records_to_publish
  end

  def call
    consecutive_runs.flat_map(&:last)
  end

  # The survivor of every run that collapsed something: the record was written again while an
  # earlier message for it was still queued.
  def deduplicated_records
    consecutive_runs.select { |run| run.size > 1 }.map(&:last)
  end

  private

  def consecutive_runs
    @consecutive_runs ||= records_to_publish
      .slice_when { |record_1, record_2| generate_uniqueness_key(record_1) != generate_uniqueness_key(record_2) }
      .to_a
  end

  def generate_uniqueness_key(record)
    [record.resource_class, record.resource_id, record.event_name, record.topic]
  end
end
