# frozen_string_literal: true

require "active_support/core_ext/string"
require "time"

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

              # consumers rank duplicates by this stamp, so it has to describe the attempt actually
              # published - with retries that is the last one, not the first
              payload, read_at = serialize_consistently(records, topic, batch_options)
              serialized_at = config.include_serialized_at_in_payload ? read_at : nil

              event_payload = {
                event: event,
                model_name: record.model_name.name,
                data: payload
              }
              event_payload[:serialized_at] = serialized_at.iso8601(6) if serialized_at
              event_payload
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

      # A serializer reads a record's own columns and then queries its associations, so anything
      # committed in between lands in the payload beside a timestamp taken before it - the payload
      # describes no single moment. Consumers rank on that timestamp and then compare it against
      # what they have stored, so a payload whose timestamp predates its own contents loses the
      # comparison and is discarded, taking the has_many records embedded in it along with it.
      #
      # Measured on the affected topic in production: serialization spans 95ms at the median and up
      # to 3.5s, and 4.7% of records receiving more than one message had the surviving message
      # report a timestamp older than one already published.
      #
      # So serialize, then check whether the records moved underneath it. If they did, the payload
      # is not a snapshot of anything: reload and serialize again. Retries are bounded, and the last
      # attempt is published rather than dropped - a payload that may be torn still beats no message.
      # returns [payload, read_at] - read_at is when the attempt that produced this payload began
      define_method :serialize_consistently do |records, current_topic, batch_options|
        unless config.publish_consistent_snapshots
          read_at = Time.now.utc
          next [serialize_to_payload(records, current_topic, batch_options), read_at]
        end

        max_attempts = config.max_snapshot_attempts
        attempts = 0
        loop do
          read_at = Time.now.utc
          before = snapshot_of(records)
          payload = serialize_to_payload(records, current_topic, batch_options)
          attempts += 1

          if before.nil?
            # nothing to compare against, so the guard did not run on this message at all
            break [instrument_snapshot("unsupported", attempts, records, current_topic, payload), read_at]
          end
          if before == committed_snapshot_of(records)
            break [instrument_snapshot("consistent", attempts, records, current_topic, payload), read_at]
          end
          if attempts >= max_attempts
            # published anyway: a payload that may be torn beats no message. Nothing downstream can
            # tell this apart from a clean one - the payload carries no marker and the consumer sees
            # an ordinary message - so this counter is the only place the outcome is ever visible.
            break [instrument_snapshot("exhausted", attempts, records, current_topic, payload), read_at]
          end

          records.each { |record| record.reload if record.is_a?(ActiveRecord::Base) && record.persisted? }
        end
      end

      # One counter rather than several: the denominator, the retry distribution and the failure
      # rate all have to come from the same series or none of them can be read as a rate.
      define_method :instrument_snapshot do |result, attempts, records, current_topic, payload|
        config.instrumenter.increment("dionysus.publish.consistent_snapshot",
          tags: ["result:#{result}", "attempts:#{attempts}", "topic:#{current_topic}",
            "model:#{records.first.class}"])
        payload
      end

      # nil means there is nothing to compare - a record without timestamps, or not a record at all
      define_method :snapshot_of do |records|
        stamps = records.map { |record| record.updated_at if record.respond_to?(:updated_at) }
        stamps.any?(&:nil?) ? nil : stamps
      end

      define_method :committed_snapshot_of do |records|
        records.map do |record|
          next record.updated_at unless record.is_a?(ActiveRecord::Base) && record.persisted?

          # publishing runs inside the request when publish_after_commit is on, where the query
          # cache is live on this connection - a cached read here would report that nothing moved
          # and hand back the torn payload the check exists to catch
          record.class.uncached do
            record.class.where(record.class.primary_key => record.id).pick(:updated_at)
          end
        end
      end

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
