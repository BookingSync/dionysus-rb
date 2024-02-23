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
      batch_size: options.fetch(:batch_size, 1000)
    )
  end

  private

  delegate :soft_delete_column, to: :config

  def fetch_resources(resource_class, from, to, options_hash)
    resource_class
      .then { |records| apply_time_range(records, from, to) }
      .then { |records| apply_visibility(records, options_hash) }
      .then { |records| apply_query_conditions(records, options_hash) }
  end

  def apply_time_range(records, from, to)
    records = records.where("updated_at BETWEEN ? AND ?", from, to) if from.present? && to.present?
    records
  end

  def apply_visibility(records, options_hash)
    if visible_only?(options_hash) && records.column_names.include?(soft_delete_column.to_s)
      records = records.where(soft_delete_column => nil)
    end
    records
  end

  def apply_query_conditions(records, options_hash)
    if (query_conditions = options_hash.fetch(:query_conditions, {})).any?
      query_conditions.each { |attr, val| records = records.where(attr => val) }
    end
    records
  end

  def visible_only?(options_hash)
    options_hash.fetch(:visible_only, false)
  end
end
