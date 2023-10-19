# frozen_string_literal: true

class Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedDataJob
  include Sidekiq::Worker

  sidekiq_options queue: Dionysus::Consumer::Config.default_sidekiq_queue

  def self.enqueue(model_klass, columns, batch_size: 1000)
    primary_key = model_klass.primary_key
    model_klass.select(:id).find_in_batches(batch_size: batch_size).with_index do |records, index|
      Dionysus.logger.info "[AssignColumnsFromSyncedDataJob] enqueue batch: #{index}"
      model_name = model_klass.model_name.to_s
      ids = records.map { |r| r.public_send(primary_key) }

      set(queue: Dionysus::Consumer.configuration.sidekiq_queue)
        .perform_async(model_name, ids, columns)
    end
  end

  def perform(model_name, ids, columns)
    model_klass = model_name.constantize
    collection = model_klass.where(model_klass.primary_key => ids)

    Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedData.new.call(collection, columns)
  end
end
