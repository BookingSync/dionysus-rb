# frozen_string_literal: true

class Dionysus::Producer::Genesis::Streamer
  attr_reader :job_class, :config
  private     :job_class, :config

  def initialize(job_class: Dionysus::Producer::Genesis::Streamer::StandardJob,
    config: Dionysus::Producer.configuration)
    @job_class = job_class
    @config = config
  end

  def stream(topic, model_class, from, to, options = {})
    resources = fetch_resources(model_class, from, to, options)
    job_class.enqueue(
      resources,
      model_class,
      topic,
      number_of_days: options.fetch(:number_of_days, 1),
      batch_size: options.fetch(:batch_size, 100)
    )
  end

  private

  delegate :soft_delete_column, to: :config

  def fetch_resources(resource_class, from, to, options_hash)
    records = resource_class
    records = resource_class.where("updated_at BETWEEN ? AND ?", from, to) if from.present? && to.present?
    if visible_only?(options_hash) && records.column_names.include?(soft_delete_column.to_s)
      records = records.where(soft_delete_column => nil)
    end
    records
  end

  def visible_only?(options_hash)
    options_hash.fetch(:visible_only, false)
  end
end
