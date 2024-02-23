# frozen_string_literal: true

class Dionysus::Producer::Genesis
  attr_reader :config
  private :config

  def initialize(config: Dionysus::Producer.configuration)
    @config = config
  end

  def stream(topic:, model:, number_of_days:, from: nil, to: nil,
    streamer_job: Dionysus::Producer::Genesis::Streamer::StandardJob)
    ensure_genesis_not_executed_for_model_that_is_only_a_dependency(model, topic)
    enqueue_stream_model_for_topic(topic, model, from, to, number_of_days, streamer_job)
    publish_genesis_performed(model: model, topic: topic, number_of_days: number_of_days)
  end

  private

  delegate :publisher_service_name, :genesis_consistency_safety_delay, :hermes_event_producer, :sidekiq_queue,
    to: :config
  delegate :responders_for_model_for_topic, :responders_for_dependency_parent_for_topic,
    to: Dionysus::Producer

  def ensure_genesis_not_executed_for_model_that_is_only_a_dependency(model, topic)
    if responders_for_model_for_topic(model,
      topic).empty? && responders_for_dependency_parent_for_topic(model, topic).any?
      raise CannotExecuteGenesisForModelThatIsOnlyDependency.new(model, topic)
    end
  end

  def enqueue_stream_model_for_topic(topic, model, from, to, number_of_days, streamer_job)
    Dionysus::Producer::Genesis::StreamJob
      .set(queue: sidekiq_queue)
      .perform_async(topic.to_s, model.to_s, from.as_json, to.as_json, number_of_days.to_i, streamer_job.to_s, {})
  end

  def publish_genesis_performed(model:, topic:, number_of_days:)
    event = Dionysus::Producer::Genesis::Performed.new(
      model: model.to_s,
      service: publisher_service_name,
      topic: topic,
      start_at: Time.current,
      end_at: Time.current + number_of_days.days + genesis_consistency_safety_delay
    )
    hermes_event_producer.publish(event)
  end

  class CannotExecuteGenesisForModelThatIsOnlyDependency < StandardError
    attr_reader :model, :topic
    private     :model, :topic
    def initialize(model, topic)
      super()
      @model = model
      @topic = topic
    end

    def message
      "Cannot execute genesis for model #{model}, #{topic} because that is only a dependency. Execute it for parent instead"
    end
  end
end
