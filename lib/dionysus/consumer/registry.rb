# frozen_string_literal: true

class Dionysus::Consumer::Registry
  attr_reader :container
  private :container

  def initialize
    @container = {}
  end

  def namespace(namespace, &block)
    registration = Registration.new(namespace)
    registration.instance_eval(&block)
    container[namespace] = registration
  end

  def registrations
    container
  end

  class Registration
    attr_reader :namespace, :topics, :deserializer_klass, :consumers

    def initialize(namespace)
      @namespace = namespace
      @topics = []
      @deserializer_klass = nil
      @consumers = []
    end

    def deserializer(deserializer_klass)
      @deserializer_klass = deserializer_klass
    end

    def topic(name, options = {}, &block)
      new_topic = Topic.new(namespace, name, deserializer_klass, options, &block)
      consumer = Dionysus::Consumer::KarafkaConsumerGenerator.new.generate(
        Dionysus::Consumer.configuration, new_topic
      )
      consumers << consumer
      new_topic.consumer = consumer
      topics << new_topic
    end

    class Topic
      attr_reader :namespace, :name, :deserializer_klass, :options, :extensions_block

      attr_accessor :consumer

      def initialize(namespace, name, deserializer_klass, options = {}, &block)
        @namespace = namespace
        @name = name
        @deserializer_klass = deserializer_klass
        @options = options
        @extensions_block = block
      end

      def to_s
        Dionysus::TopicName.new(namespace, name).to_s
      end

      def sidekiq_worker
        options.fetch(:worker, nil)
      end

      def sidekiq_backend?
        options.fetch(:sidekiq, false)
      end

      def consumer_base_class
        options.fetch(:consumer_base_class, nil)
      end

      def concurrency
        options.fetch(:concurrency, nil)
      end

      def params_batch_transformation
        options.fetch(:params_batch_transformation,
          Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy.new)
      end
    end
  end
end
