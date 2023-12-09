# frozen_string_literal: true

class Dionysus::Producer::Outbox::LatencyTracker
  LatencyTrackerResult = Struct.new(:minimum, :maximum, :average, :highest_since_creation_date)
  private_constant :LatencyTrackerResult

  attr_reader :config, :clock
  private     :config, :clock

  def initialize(config: Dionysus::Producer.configuration, clock: Time)
    @config = config
    @clock = clock
  end

  def calculate(interval: 1.minute)
    records = outbox_model.published_since(interval.ago)
    latencies = records.map(&:publishing_latency)

    LatencyTrackerResult.new(
      latencies.min.to_d,
      latencies.max.to_d,
      calculate_average(latencies),
      calculate_highest_since_creation_date.to_d
    )
  end

  private

  delegate :outbox_model, to: :config

  def calculate_average(latencies)
    if latencies.any?
      latencies.sum.to_d / latencies.size
    else
      0
    end
  end

  def calculate_highest_since_creation_date
    minimum_created_at_from_not_published = outbox_model.not_published.minimum(:created_at) or return 0
    clock.current - minimum_created_at_from_not_published
  end
end
