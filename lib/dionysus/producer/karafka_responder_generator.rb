# frozen_string_literal: true

require "active_support/core_ext/string"

class Dionysus::Producer::KarafkaResponderGenerator
  TOMBSTONE = nil

  def generate(config, topic)
    topic_name = topic.to_s
    genesis_topic_name = topic.genesis_to_s if topic.genesis_replica?

    responder_klass = Class.new(Dionysus::Producer::BaseResponder) do
      topic topic_name
      topic genesis_topic_name if topic.genesis_replica?

      define_method :respond do |batch, options = {}|
        config.instrumenter.instrument("dionysus.respond.#{self.class.name}") do
          final_options = {}
          if (partition_key = options.fetch(:partition_key, nil))
            final_options[:partition_key] = partition_key
          end
          if (key = options.fetch(:key, nil))
            final_options[:key] = key
          end

          if genesis_only?(options) && genesis_topic_name.nil?
            raise "cannot execute genesis-only as there is no genesis topic for responder #{self.class.name}"
          end

          if batch.nil?
            unless genesis_only?(options)
              respond_to topic_name, TOMBSTONE, **final_options
              config.event_bus.publish("dionysus.respond", topic_name: topic_name, message: TOMBSTONE,
                options: final_options)
            end
            if topic.genesis_replica?
              respond_to genesis_topic_name, TOMBSTONE, **final_options
              config.event_bus.publish("dionysus.respond", topic_name: genesis_topic_name, message: TOMBSTONE,
                options: final_options)
            end
          else
            message = Array.wrap(batch).map do |event, record_or_records, batch_options|
              records = Array.wrap(record_or_records)
              return if records.empty?

              record = records.sample

              payload = serialize_to_payload(records, topic, batch_options)

              {
                event: event,
                model_name: record.model_name.name,
                data: payload
              }
            end
            unless genesis_only?(options)
              respond_to topic_name, { message: message }, **final_options
              config.event_bus.publish("dionysus.respond", topic_name: topic_name, message: message,
                options: final_options)
            end
            if topic.genesis_replica?
              respond_to genesis_topic_name, { message: message }, **final_options
              config.event_bus.publish("dionysus.respond", topic_name: genesis_topic_name, message: message,
                options: final_options)
            end
          end
        end
      end

      private

      define_method :serialize_to_payload do |records, current_topic, batch_options|
        if batch_options.to_h[:serialize] == false
          records.map(&:as_json)
        else
          record = records.sample

          model_klass = record.class
          dependencies = current_topic
            .models
            .find(-> { NullRegistration.new }) { |model_registration| model_registration.model_klass == model_klass }
            .options
            .to_h
            .fetch(:with, [])

          current_topic.serializer_klass.serialize(records, dependencies: dependencies)
        end
      end

      define_method :genesis_only? do |options|
        options.fetch(:genesis_only, false) == true
      end
    end

    responder_klass.instance_exec(topic) do |dionysus_topic|
      define_singleton_method :publisher_of? do |model_klass|
        dionysus_topic.publishes_model?(model_klass)
      end

      define_singleton_method :publisher_for_topic? do |current_topic|
        if dionysus_topic.genesis_replica?
          dionysus_topic.to_s == current_topic.to_s || dionysus_topic.genesis_to_s == current_topic.to_s
        else
          dionysus_topic.to_s == current_topic.to_s
        end
      end

      define_singleton_method :publisher_of_model_for_topic? do |model_klass, current_topic|
        dionysus_topic.publishes_model?(model_klass) && publisher_for_topic?(current_topic)
      end

      define_singleton_method :partition_key do
        dionysus_topic.partition_key
      end

      define_singleton_method :primary_topic do
        responder_klass.topics.values.first
      end
    end

    responder_klass_name = "#{topic.to_s.classify}Responder"

    Dionysus.send(:remove_const, responder_klass_name) if Dionysus.const_defined?(responder_klass_name)
    Dionysus.const_set(responder_klass_name, responder_klass)
    responder_klass
  end

  class NullRegistration
    def options
      {}
    end
  end
end
