# frozen_string_literal: true

class Dionysus::Producer::Outbox::DatadogLatencyReporter
  attr_reader :config
  private     :config

  def initialize(config: Dionysus::Producer.configuration)
    @config = config
  end

  def report(latency: generate_latency)
    datadog_statsd_client.gauge("dionysus.producer.outbox.latency.minimum", latency.minimum)
    datadog_statsd_client.gauge("dionysus.producer.outbox.latency.maximum", latency.maximum)
    datadog_statsd_client.gauge("dionysus.producer.outbox.latency.average", latency.average)
    datadog_statsd_client.gauge("dionysus.producer.outbox.latency.highest_since_creation_date",
      latency.highest_since_creation_date)
  end

  private

  delegate :datadog_statsd_client, :datadog_statsd_prefix, to: :config

  def generate_latency
    Dionysus::Producer::Outbox::LatencyTracker.new.calculate
  end
end
