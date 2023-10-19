# frozen_string_literal: true

class Dionysus::Producer::Registry
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
    attr_reader :namespace, :topics, :serializer_klass, :producers

    def initialize(namespace)
      @namespace = namespace
      @topics = []
      @serializer_klass = nil
      @producers = []
    end

    def serializer(serializer_klass)
      @serializer_klass = serializer_klass
    end

    def topic(name, options = {}, &block)
      new_topic = Topic.new(namespace, name, serializer_klass, options)
      new_topic.instance_eval(&block)
      producer = Dionysus::Producer::KarafkaResponderGenerator.new.generate(
        Dionysus::Producer.configuration, new_topic
      )
      producers << producer
      new_topic.producer = producer
      topics << new_topic
    end

    class Topic
      GENESIS_SUFFIX = "genesis"
      private_constant :GENESIS_SUFFIX

      attr_reader :namespace, :name, :serializer_klass, :options, :models

      attr_accessor :producer

      def initialize(namespace, name, serializer_klass, options = {})
        @namespace = namespace
        @name = name
        @options = options
        @serializer_klass = serializer_klass
        @models = []
      end

      def to_s
        Dionysus::TopicName.new(namespace, name).to_s
      end

      def genesis_to_s
        Dionysus::TopicName.new(namespace, "#{name}_#{GENESIS_SUFFIX}").to_s if genesis_replica?
      end

      def partition_key
        options.fetch(:partition_key, nil)
      end

      def genesis_replica?
        options.fetch(:genesis_replica, false) == true
      end

      def publish(model_klass, model_registrations_options = {})
        @models << ModelRegistration.new(model_klass, model_registrations_options)
      end

      def publishes_model?(model_klass)
        models.any? { |registration| registration.model_klass == model_klass }
      end

      class ModelRegistration
        attr_reader :model_klass, :options

        def initialize(model_klass, options = {})
          @model_klass = model_klass
          @options = options
          validate_and_set_up
        end

        def observes?(resource, changeset)
          observer_config_for(resource, changeset).present?
        end

        def association_name_for_observable(resource, changeset)
          observer_config_for(resource, changeset).fetch(:association_name)
        end

        def observables_config
          options.fetch(:observe, [])
        end

        private

        def validate_and_set_up
          validate_options
          set_up_as_publishables
        end

        def validate_options
          return unless options.key?(:observe)

          options[:observe].each do |observer_config|
            model = observer_config.fetch(:model)
            association_name = observer_config.fetch(:association_name)
            _attributes = observer_config.fetch(:attributes)

            if association_name.is_a?(Symbol) && !model.instance_methods.include?(association_name.to_sym)
              raise ArgumentError.new("association name :#{association_name} does not exist on model #{model}")
            end
          end
        end

        def set_up_as_publishables
          [model_klass, dependencies_from_parent, observables]
            .flatten
            .select { |model| model.instance_of?(Class) && model.ancestors.include?(ActiveRecord::Base) }
            .each { |model| ensure_model_is_publishable(model) }
        end

        def dependencies_from_parent
          options.fetch(:with, [])
        end

        def observables
          options.fetch(:observe, []).map { |hash| hash.fetch(:model) }
        end

        def ensure_model_is_publishable(model)
          model.include(publishable_module) unless model.included_modules.include?(publishable_module)
        end

        def publishable_module
          Dionysus::Producer::Outbox::ActiveRecordPublishable
        end

        def observer_config_for(resource, changeset)
          resource_model_name = resource.model_name.to_s
          changeset_attributes = changeset.keys.map(&:to_sym)

          options[:observe].to_a.find do |observer_config|
            config_model_name = observer_config.fetch(:model).to_s
            config_attributes = observer_config.fetch(:attributes).to_a.map(&:to_sym)

            config_model_name == resource_model_name && (config_attributes & changeset_attributes).any?
          end
        end
      end
    end
  end
end
