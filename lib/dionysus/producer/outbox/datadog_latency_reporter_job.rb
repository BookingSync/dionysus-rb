# frozen_string_literal: true

class Dionysus::Producer::Outbox::DatadogLatencyReporterJob
  include Sidekiq::Worker

  sidekiq_options queue: Dionysus::Producer::Config.high_priority_sidekiq_queue

  def perform
    Dionysus::Producer::Outbox::DatadogLatencyReporter.new.report
  end
end
