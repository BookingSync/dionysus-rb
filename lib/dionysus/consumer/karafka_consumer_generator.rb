# frozen_string_literal: true

class Dionysus::Consumer::KarafkaConsumerGenerator
  TOMBSTONE = nil

  def generate(config, topic)
    base_class = topic.consumer_base_class || config.consumer_base_class

    consumer_klass = Class.new(base_class) do
      define_method :consume do
        config.retry_provider.retry do
          processed_events = Concurrent::Array.new
          config.instrumenter.instrument("dionysus.consume.#{topic}") do
            batch_number = 0

            if topic.concurrency
              workers = Dionysus::Consumer::WorkersGroup.new
              messages.each do |batch|
                batch_number += 1 # cannot use each_with_index on params_batch
                worker = Thread.new do
                  Thread.current.report_on_exception = true
                  Thread.current.abort_on_exception = true
                  processed_events.concat(process_batch(config, topic, batch, batch_number))
                end
                workers << worker
              end
              workers.work
            else
              final_params_batch = topic.params_batch_transformation&.call(messages) || messages
              final_params_batch.each do |batch|
                batch_number += 1 # cannot use each_with_index on params_batch
                processed_events.concat(process_batch(config, topic, batch, batch_number))
              end
            end
          end
          Dionysus::Consumer::BatchEventsPublisher.new(config, topic).publish(processed_events)
        end
      end

      private

      define_method :process_batch do |configuration, current_topic, batch, batch_number|
        configuration.transaction_provider.transaction do
          Dionysus::Consumer::ParamsBatchProcessor.new(configuration, current_topic).process(batch,
            batch_number)
        end
      end
    end

    consumer_klass_name = "#{topic.to_s.classify}Consumer"

    Dionysus.send(:remove_const, consumer_klass_name) if Dionysus.const_defined?(consumer_klass_name)
    Dionysus.const_set(consumer_klass_name, consumer_klass)
    consumer_klass
  end
end
