# frozen_string_literal: true

class Dionysus::Producer::Outbox::DatadogLatencyReporterScheduler
  JOB_NAME = "dionysus_producer_outbox_datadog_latency_reporter_job"
  EVERY_MINUTE_IN_CRON_SYNTAX = "* * * * *"
  JOB_CLASS_NAME = "Dionysus::Producer::Outbox::DatadogLatencyReporterJob"
  JOB_DESCRIPTION = "Collect latency metrics from dionysus outbox and send them to Datadog"

  private_constant :JOB_NAME, :EVERY_MINUTE_IN_CRON_SYNTAX, :JOB_CLASS_NAME, :JOB_DESCRIPTION

  attr_reader :config
  private     :config

  def initialize(config: Dionysus::Producer.configuration)
    @config = config
  end

  def add_to_schedule
    find || create
  end

  private

  def find
    Sidekiq::Cron::Job.find(name: JOB_NAME)
  end

  def create
    Sidekiq::Cron::Job.create(create_job_arguments)
  end

  def create_job_arguments
    {
      name: JOB_NAME,
      cron: EVERY_MINUTE_IN_CRON_SYNTAX,
      class: JOB_CLASS_NAME,
      queue: config.high_priority_sidekiq_queue,
      active_job: false,
      description: JOB_DESCRIPTION,
      date_as_argument: false
    }
  end

  def every_minute_to_cron_syntax
    EVERY_MINUTE_IN_CRON_SYNTAX
  end
end
