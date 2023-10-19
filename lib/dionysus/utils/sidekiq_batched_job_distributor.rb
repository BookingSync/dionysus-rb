# frozen_string_literal: true

class Dionysus::Utils::SidekiqBatchedJobDistributor
  attr_reader :batch_size, :units_count, :time_range_in_seconds
  private     :batch_size, :units_count, :time_range_in_seconds

  def initialize(batch_size:, units_count:, time_range_in_seconds:)
    @batch_size = batch_size
    @units_count = units_count
    @time_range_in_seconds = time_range_in_seconds
  end

  def number_of_batches
    (units_count.to_d / batch_size).ceil
  end

  def time_per_batch
    (time_range_in_seconds.to_d / number_of_batches).floor
  end

  def enqueue_batch(job_class, queue, batch_number, *job_arguments)
    job_class.set(queue: queue).perform_in(batch_number * time_per_batch, *job_arguments)
  end
end
