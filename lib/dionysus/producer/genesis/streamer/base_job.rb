# frozen_string_literal: true

class Dionysus::Producer::Genesis::Streamer::BaseJob
  include Sidekiq::Worker

  sidekiq_options queue: Dionysus::Producer::Config.default_sidekiq_queue

  ONE_DAY_IN_SECONDS = 60 * 60 * 24
  BATCH_SIZE = 100

  def self.enqueue(relation, model_class, topic, number_of_days: 1, batch_size: BATCH_SIZE)
    distributor = Dionysus::Utils::SidekiqBatchedJobDistributor.new(
      batch_size: batch_size,
      units_count: relation.count,
      time_range_in_seconds: (ONE_DAY_IN_SECONDS * number_of_days)
    )

    relation.in_batches(of: batch_size).each_with_index do |batch_relation, batch_number|
      distributor.enqueue_batch(
        self,
        Dionysus::Producer.configuration.sidekiq_queue,
        batch_number,
        batch_relation.pluck(model_class.primary_key).sort,
        model_class.to_s,
        topic
      )
    end
  end

  def perform(ids, resource_name, topic)
    model_class = resource_name.constantize
    primary_key_column = model_class.primary_key

    model_class
      .where(primary_key_column => ids)
      .find_each { |entity| call(entity, topic) }
  end

  private

  def call(_item, _topic)
    raise "implement me!"
  end
end
