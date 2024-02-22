# frozen_string_literal: true

class Dionysus::Consumer::ParamsBatchProcessor
  attr_reader :config, :topic
  private     :config, :topic

  def initialize(config, topic)
    @config = config
    @topic = topic
  end

  def process(batch, batch_number)
    processed_events = []
    instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}") do
      payload = batch.payload.to_h
      metadata = batch.metadata
      message = payload["message"].to_a

      with_mutex(metadata, config) do
        message.each do |current_event|
          event_name = current_event["event"]
          data = Array.wrap(current_event["data"])
          model_name = current_event["model_name"]

          transformed_data = nil
          config.instrumenter.instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.deserialize") do
            transformed_data = Dionysus::Consumer::Deserializer.new(data).deserialize
          end

          if (applicable_message_filter = find_applicable_message_filter(topic, message, transformed_data))
            applicable_message_filter.notify_about_ignored_message(topic: topic, message: message,
              transformed_data: transformed_data)
            next
          end

          dionysus_event = Dionysus::Consumer::DionysusEvent.new(event_name, model_name,
            transformed_data)
          config.instrumenter.instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist",
            dionysus_event.to_h.except(:transformed_data, :local_changes)) do
            config.transaction_provider.connection_pool.with_connection do
              Dionysus::Consumer::Persistor.new(config, topic).persist(dionysus_event,
                batch_number)
            end
          end
          processed_events << dionysus_event
        end
      end
    end
    processed_events
  end

  private

  delegate :message_filters, to: :config

  def find_applicable_message_filter(topic, message, transformed_data)
    message_filters.find { |f| f.ignore_message?(topic: topic, message: message, transformed_data: transformed_data) }
  end

  def instrument(label, options = {}, &block)
    config.instrumenter.instrument(label, options, &block)
  end

  def with_mutex(metadata, config, &block)
    message_key = metadata.key || SecureRandom.uuid
    config.processing_mutex_provider.send(config.processing_mutex_method_name, "Dionysus-#{message_key}",
      &block)
  end
end
