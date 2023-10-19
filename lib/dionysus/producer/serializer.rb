# frozen_string_literal: true

class Dionysus::Producer::Serializer
  def self.serialize(record_or_records, dependencies: [])
    new(record_or_records, dependencies: dependencies).serialize
  end

  attr_reader :records, :dependencies
  private     :records, :dependencies

  def initialize(record_or_records, dependencies: [])
    @records = Array.wrap(record_or_records).compact

    @dependencies = dependencies
  end

  def serialize
    records.map do |record|
      serializer = resolve_serializer_for_record(record)
      serializer.as_json
    end
  end

  private

  def resolve_serializer_for_record(record)
    if record.persisted?
      infer_serializer.new(record, include: include, context_serializer: self.class)
    else
      deleted_record_serializer(record)
    end
  end

  def infer_serializer
    raise "implement me!"
  end

  def deleted_record_serializer(record)
    Dionysus::Producer::DeletedRecordSerializer.new(record, include: include,
      context_serializer: self.class)
  end

  def model_klass
    records.first&.class
  end

  def include
    dependencies.to_a.map do |model_klass|
      [model_klass.model_name.plural.to_sym, model_klass.model_name.singular.to_sym]
    end.flatten
  end
end
