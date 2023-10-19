# frozen_string_literal: true

require "active_support/core_ext/array/wrap"

class Dionysus::Consumer::Deserializer
  attr_reader :data

  def initialize(data)
    @data = data.to_a
  end

  def deserialize
    data.map { |serialized_payload| SerializedRecord.new(serialized_payload) }.map do |serialized_record|
      transformed_payload = DeserializedRecord.new
      transformed_payload = populate_attributes_with_relationships(serialized_record, transformed_payload)

      transformed_payload = assign_attributes(serialized_record, transformed_payload)

      serialized_record.expected_has_many_relationships.each do |relationship_name|
        deserialize_has_many_relationship(serialized_record, relationship_name, transformed_payload)
      end

      serialized_record.expected_has_one_relationships.each do |relationship_name|
        deserialize_has_one_relationship(serialized_record, relationship_name, transformed_payload)
      end

      transformed_payload
    end
  end

  private

  def populate_attributes_with_relationships(serialized_record, transformed_payload)
    serialized_record.links.each do |relationship_name, value|
      transformed_payload.populate_attributes_with_relationship(relationship_name, value)
    end

    transformed_payload
  end

  def assign_attributes(serialized_record, transformed_payload)
    transformed_payload.synced_id = serialized_record.id
    transformed_payload.synced_created_at = serialized_record.created_at if serialized_record.has_created_at?
    transformed_payload.synced_updated_at = serialized_record.updated_at if serialized_record.has_updated_at?
    transformed_payload.synced_canceled_at = serialized_record.canceled_at if serialized_record.has_canceled_at?

    serialized_record.plain_attributes.each do |attribute, value|
      transformed_payload.attributes[attribute] = value
    end

    transformed_payload
  end

  def deserialize_has_many_relationship(serialized_record, relationship_name, transformed_payload)
    value = serialized_record[relationship_name]
    deserialized_relationship = (value && Dionysus::Consumer::Deserializer.new(value).deserialize) || nil
    relationship_model_name = serialized_record.model_name_for_relationship(relationship_name)
    transformed_payload.has_many << [relationship_model_name, deserialized_relationship]
    transformed_payload.delete(relationship_name)
    transformed_payload
  end

  def deserialize_has_one_relationship(serialized_record, relationship_name, transformed_payload)
    value = serialized_record[relationship_name]
    deserialized_relationship = Dionysus::Consumer::Deserializer.new(Array.wrap(value)).deserialize
    relationship_model_name = serialized_record.model_name_for_relationship(relationship_name)
    transformed_payload.has_one << [relationship_model_name, deserialized_relationship.first]
    transformed_payload.delete(relationship_name)
    transformed_payload
  end

  class DeserializedRecord < SimpleDelegator
    def initialize
      super(canonical_format)
    end

    def transformed_payload
      __getobj__
    end

    def attributes
      transformed_payload.fetch(:attributes)
    end

    def has_many
      transformed_payload.fetch(:has_many)
    end

    def has_one
      transformed_payload.fetch(:has_one)
    end

    def synced_id
      attributes.fetch("synced_id") { "synced_id not found in #{attributes}! Something is very wrong." }
    end

    def synced_id=(val)
      attributes["synced_id"] = val
    end

    def synced_created_at
      attributes["synced_created_at"]
    end

    def synced_created_at=(val)
      attributes["synced_created_at"] = val
    end

    def synced_updated_at
      attributes["synced_updated_at"]
    end

    def synced_updated_at=(val)
      attributes["synced_updated_at"] = val
    end

    def synced_canceled_at
      attributes["synced_canceled_at"]
    end

    def synced_canceled_at=(val)
      attributes["synced_canceled_at"] = val
    end

    def populate_attributes_with_relationship(relationship_name, value)
      if value.respond_to?(:to_hash)
        attributes["synced_#{relationship_name}_id"] = value["id"]
        attributes["synced_#{relationship_name}_type"] = value["type"]
      elsif value.respond_to?(:to_ary)
        relationship_name = ActiveSupport::Inflector.singularize(relationship_name)
        attributes["synced_#{relationship_name}_ids"] = value
      else
        attributes["synced_#{relationship_name}_id"] = value
      end
    end

    def has_synced_canceled_at?
      attributes.key?("synced_canceled_at")
    end

    private

    def canonical_format
      { attributes: {}, has_many: [], has_one: [] }
    end
  end

  class SerializedRecord
    RESERVED_ATTRIBUTES = %w[links id created_at updated_at canceled_at].freeze
    private_constant :RESERVED_ATTRIBUTES

    attr_reader :payload
    private     :payload

    delegate :key?, :[], to: :payload

    def initialize(payload)
      @payload = payload
    end

    def plain_attributes
      payload.except(*RESERVED_ATTRIBUTES, *expected_has_many_relationships, *expected_has_one_relationships)
    end

    def expected_has_many_relationships
      to_many_foreign_keys.keys
    end

    def expected_has_one_relationships
      to_one_foreign_keys.keys
    end

    def model_name_for_relationship(relationship_name)
      if polymorphic_relationship?(relationship_name) && links[relationship_name].key?("type")
        links[relationship_name]["type"]
      else
        relationship_name
      end
    end

    def id
      payload["id"]
    end

    def created_at
      payload["created_at"]
    end

    def updated_at
      payload["updated_at"]
    end

    def canceled_at
      payload["canceled_at"]
    end

    def has?(key)
      key?(key)
    end

    def has_created_at?
      has?("created_at")
    end

    def has_updated_at?
      has?("updated_at")
    end

    def has_canceled_at?
      has?("canceled_at")
    end

    def links
      payload["links"].to_h
    end

    private

    def to_one_foreign_keys
      links.reject { |_, value| value.respond_to?(:to_ary) }
    end

    def to_many_foreign_keys
      links.select { |_, value| value.respond_to?(:to_ary) }
    end

    def polymorphic_relationship?(relationship_name)
      links[relationship_name]&.respond_to?(:to_hash)
    end
  end
end
