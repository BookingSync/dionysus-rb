# frozen_string_literal: true

class Dionysus::Producer::Registry::Validator
  attr_reader :registry
  private :registry

  def initialize(registry: Dionysus::Producer.registry)
    @registry = registry
  end

  def validate_columns
    registry.registrations.each_value do |registration|
      registration.topics.each do |topic|
        topic
          .models
          .flat_map(&:observables_config)
          .each { |observable_config| validate_observable(observable_config) }
      end
    end
  end

  private

  def validate_observable(observable_config)
    model = observable_config.fetch(:model)
    attributes = observable_config.fetch(:attributes)

    return if attributes.all? { |attribute| model.column_names.include?(attribute.to_s) }

    raise ArgumentError.new("some attributes #{attributes} do not exist on model #{model}")
  end
end
