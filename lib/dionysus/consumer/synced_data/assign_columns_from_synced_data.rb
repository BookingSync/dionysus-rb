# frozen_string_literal: true

class Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedData
  attr_reader :config
  private     :config

  def initialize(config: Dionysus::Consumer.configuration)
    @config = config
  end

  def call(collection, columns)
    collection.each { |record| record.update!(hash_of_attributes(record, columns)) }
  end

  private

  delegate :resolve_synced_data_hash_proc, to: :config

  def hash_of_attributes(record, columns)
    columns
      .to_h { |column| [column, fetch_value_from_synced_data(record, column)] }
  end

  def fetch_value_from_synced_data(record, column)
    resolve_synced_data_hash_proc.call(record).stringify_keys[column]
  end
end
