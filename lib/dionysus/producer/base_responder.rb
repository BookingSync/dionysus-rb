# frozen_string_literal: true

# Based on the responder concept from Karafka 1.4
class Dionysus::Producer::BaseResponder
  class << self
    attr_accessor :topics

    def topic(topic_name)
      self.topics ||= {}
      self.topics[topic_name] = topic_name.to_s
    end

    def call(*data)
      new.call(*data)
    end
  end

  attr_reader :messages_buffer

  def initialize
    @messages_buffer = Hash.new { |h, k| h[k] = [] }
  end

  def call(*data)
    respond(*data)
    deliver
  end

  private

  def deliver
    messages_buffer.each_value do |data_elements|
      data_elements.each do |data, options|
        Karafka.producer.produce_sync(payload: data, **options)
      end
    end
  end

  def respond(*_data)
    raise "implement me"
  end

  def respond_to(topic, data, options = {})
    messages_buffer[topic] << [data.to_json, options.merge(topic: topic.to_s)]
  end
end
