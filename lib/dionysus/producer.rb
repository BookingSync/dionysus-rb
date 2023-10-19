# frozen_string_literal: true

class Dionysus::Producer
  def self.configuration
    @configuration ||= Dionysus::Producer::Config.new
  end

  def self.configure
    yield configuration
  end

  def self.registry
    configuration.registry
  end

  def self.declare(&config)
    registry = Dionysus::Producer::Registry.new

    registry.instance_eval(&config)
    configure do |configuration|
      configuration.registry = registry
    end
  end

  def self.outbox
    Dionysus::Producer::Outbox.new(configuration.outbox_model, config: configuration)
  end

  def self.outbox_publisher
    Dionysus::Producer::Outbox::Publisher.new(config: configuration)
  end

  def self.reset!
    return if registry.nil?

    registry.registrations.values.flat_map(&:producers).each do |producer_class|
      Dionysus.send(:remove_const, producer_class.name.demodulize.to_sym) if producer_class.name
    end
    @configuration = Dionysus::Producer::Config.new
  end

  def self.responders_for(model_klass)
    return [] if registry.nil?

    registry.registrations.each.with_object([]) do |(_, registration), responders|
      registration.producers.select { |producer| producer.publisher_of?(model_klass) }.each do |producer|
        responders << producer
      end
    end
  end

  def self.responders_for_model_for_topic(model_klass, topic)
    responders_for(model_klass).select { |responder| responder.publisher_of_model_for_topic?(model_klass, topic) }
  end

  def self.responders_for_dependency_parent(model_klass)
    return [] if registry.nil?

    registry.registrations.values.each.with_object([]) do |registration, accum|
      registration.topics.each do |topic|
        topic
          .models
          .select { |model_registration| model_registration.options[:with].to_a.include?(model_klass) }
          .each do |model_registration|
          accum << [model_registration.model_klass, topic.producer]
        end
      end
    end
  end

  def self.responders_for_dependency_parent_for_topic(model_klass, topic)
    responders_for_dependency_parent(model_klass).select do |_model, responder|
      responder.publisher_for_topic?(topic)
    end
  end

  def self.start_outbox_worker(threads_number:)
    runners = (1..threads_number).map do
      Dionysus::Producer::Outbox::Runner.new(config: configuration)
    end
    executor = Sigurd::Executor.new(runners, sleep_seconds: 5, logger: Dionysus.logger)
    signal_handler = Sigurd::SignalHandler.new(executor)
    signal_handler.run!
  end

  def self.topics_models_mapping
    return {} if registry.nil?

    registry
      .registrations
      .values
      .flat_map(&:topics)
      .to_h do |topic|
        [
          topic.to_s,
          topic.models.to_h { |registration| [registration.model_klass, registration.options.fetch(:with, [])] }
        ]
      end
  end

  def self.observers_with_responders_for(resource, changeset)
    return [] if registry.nil?

    registry.registrations.values.each.with_object([]) do |registration, accum|
      registration.topics.each do |topic|
        topic
          .models
          .select { |model_registration| model_registration.observes?(resource, changeset) }
          .each do |model_registration|
            association_name = model_registration.association_name_for_observable(resource, changeset)
            methods_chain = association_name.to_s.split(".")
            association_or_associations = methods_chain.inject(resource) do |record, method_name|
              record.public_send(method_name)
            end

            accum << [Array.wrap(association_or_associations).compact, topic.producer]
          end
      end
    end
  end
end
