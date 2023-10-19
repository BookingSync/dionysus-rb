# frozen_string_literal: true

class Dionysus::Producer::ModelSerializer
  attr_reader :record, :include, :context_serializer
  private     :record, :include, :context_serializer

  def initialize(record, include:, context_serializer:)
    @record = record
    @include = include
    @context_serializer = context_serializer
  end

  def self.attributes(*names)
    Array(names).each do |name|
      attribute(name, {})
    end
  end

  def self.attribute(name, options = {})
    declared_attributes << [name, options]

    define_method(name) do
      record.public_send(name)
    end
  end

  def self.has_one(name, options = {})
    declared_to_one_relationships << [name, options]

    define_method(name) do
      record.public_send(name)
    end

    define_method("#{name}_id") do
      record.public_send("#{name}_id")
    end
  end

  def self.has_many(name, options = {})
    declared_to_many_relationships << [name, options]

    define_method(name) do
      record.public_send(name)
    end

    define_method("#{name.to_s.singularize}_ids") do
      record.public_send("#{name.to_s.singularize}_ids")
    end
  end

  def self.declared_attributes
    @declared_attributes ||= []
  end

  def self.declared_to_one_relationships
    @declared_to_one_relationships ||= []
  end

  def self.declared_to_many_relationships
    @declared_to_many_relationships ||= []
  end

  def as_json
    {}.tap do |payload|
      declared_attributes.each do |declared_attribute, _options|
        payload[declared_attribute] = send(declared_attribute)
      end
      payload["links"] = {}
      declared_to_one_relationships.each do |declared_relationship, _options|
        payload["links"][declared_relationship] = send("#{declared_relationship}_id")
      end
      declared_to_many_relationships.each do |declared_relationship, _options|
        payload["links"][declared_relationship] = send("#{declared_relationship.to_s.singularize}_ids")
      end

      include.each do |relationship_to_include|
        relationship_to_include = relationship_to_include.to_sym

        if declared_to_one_relationships.to_h.key?(relationship_to_include)
          payload[relationship_to_include] =
            context_serializer.serialize(send(relationship_to_include), dependencies: []).first
        end

        if declared_to_many_relationships.to_h.key?(relationship_to_include)
          payload[relationship_to_include] =
            context_serializer.serialize(send(relationship_to_include), dependencies: [])
        end
      end
    end.deep_stringify_keys
  end

  private

  def declared_attributes
    self.class.declared_attributes
  end

  def declared_to_one_relationships
    self.class.declared_to_one_relationships
  end

  def declared_to_many_relationships
    self.class.declared_to_many_relationships
  end
end
