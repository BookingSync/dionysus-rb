# frozen_string_literal: true

class Dionysus::Producer::Genesis::StreamJob
  include Sidekiq::Worker

  sidekiq_options queue: Dionysus::Producer::Config.default_sidekiq_queue

  def perform(topic, model_klass, from, to, number_of_days, streamer_job, options = {})
    final_options = options.symbolize_keys.reverse_merge(number_of_days: number_of_days)
    Dionysus::Producer::Genesis::Streamer
      .new(job_class: streamer_job.constantize)
      .stream(topic, model_klass.constantize, from, to, final_options)
  end
end
