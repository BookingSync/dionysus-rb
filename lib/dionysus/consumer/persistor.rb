# frozen_string_literal: true

class Dionysus::Consumer::Persistor
  attr_reader :config, :topic
  private     :config, :topic

  def initialize(config, topic)
    @config = config
    @topic = topic
  end

  def persist(dionysus_event, batch_number)
    if dionysus_event.generic_event?
      if dionysus_event.created? && topic.options[:import] == true
        persist_via_dionysus_create(dionysus_event, batch_number)
      elsif dionysus_event.destroyed? && topic.options[:import] == true
        persist_via_dionysus_destroy(dionysus_event, batch_number)
      else
        persist_standard_event(dionysus_event, batch_number)
      end
    else
      log_unknown_event_type(dionysus_event)
    end
  end

  private

  def persist_via_dionysus_create(dionysus_event, batch_number)
    model_klass = find_model_klass(dionysus_event) or return
    config.instrumenter.instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.dionysus_import") do
      model_klass.dionysus_import(dionysus_event.transformed_data)
    end
  end

  def persist_via_dionysus_destroy(dionysus_event, batch_number)
    model_klass = find_model_klass(dionysus_event) or return
    config.instrumenter.instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.dionysus_destroy") do
      model_klass.dionysus_destroy(dionysus_event.transformed_data)
    end
  end

  def find_model_klass(dionysus_event)
    config.model_factory.for_model(dionysus_event.model_name)
  end

  def persist_standard_event(dionysus_event, batch_number)
    Array.wrap(dionysus_event.transformed_data).each do |deseralized_record|
      model_klass = find_model_klass(dionysus_event) or return
      attributes = deseralized_record.attributes
      has_one_relationships = deseralized_record.has_one
      has_many_relationships = deseralized_record.has_many
      synced_id = deseralized_record.synced_id

      if synced_id.nil?
        Dionysus.logger.error("[Dionysus] synced_id nil for #{deseralized_record}, that should never happen!")
        next
      end

      record = Dionysus::Consumer::SynchronizableModel.new(config,
        model_klass.find_or_initialize_by(config.synced_id_attribute => synced_id))
      event_updated_at = deseralized_record.synced_updated_at || deseralized_record.synced_created_at

      next unless record.persist_with_dionysus?(event_updated_at)

      record.assign_attributes_from_dionysus(attributes)
      if dionysus_event.destroyed?
        instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.remove_with_dionysus") do
          record.remove_with_dionysus(deseralized_record) if dionysus_event.aggregate_root?
        end
      else
        instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.restore_with_dionysus") do
          record.restore_with_dionysus if record.restorable?(deseralized_record)
        end
      end

      dionysus_event.local_changes[[dionysus_event.model_name, synced_id]] = record.changes if record.changes.present?

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.save") do
        record.save unless record.destroyed?
      end

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_many_relationships") do
        has_many_relationships.each do |relationship_name, relationship_records|
          persist_to_many_relationship(dionysus_event, relationship_name, record, relationship_records,
            batch_number)
        end
      end

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_one_relationships") do
        has_one_relationships.each do |relationship_name, relationship_record|
          persist_to_one_relationship(dionysus_event, relationship_name, record, relationship_record,
            batch_number)
        end
      end
    end
  end

  def log_unknown_event_type(dionysus_event)
    Dionysus.logger.debug("[Dionysus] unknown event type #{dionysus_event.event_name}")
  end

  def persist_to_one_relationship(original_event, relationship_name, parent_model_record, record, batch_number)
    instrumentation_arguments = {
      event_name: original_event.event_name,
      parent_model_record: parent_model_record.model_name.to_s,
      relationship_name: relationship_name
    }
    instrument(
      "dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_one_relationship.#{relationship_name}", instrumentation_arguments
    ) do
      return if parent_model_record.nil? || record.nil?

      records = Array.wrap(record)
      dionysus_event = Dionysus::Consumer::DionysusEvent.new(original_event.event_name,
        relationship_name, records, aggregate_root: false)

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_one_relationship.#{relationship_name}.persist") do
        persist(dionysus_event, batch_number)
        original_event.local_changes.merge!(dionysus_event.local_changes)
      end

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_one_relationship.#{relationship_name}.resolve_to_one_association") do
        parent_model_record.resolve_to_one_association(relationship_name, record.synced_id)
      end
    end
  end

  def persist_to_many_relationship(original_event, relationship_name, parent_model_record, records, batch_number)
    instrumentation_arguments = {
      event_name: original_event.event_name,
      parent_model_record: parent_model_record.model_name.to_s,
      relationship_name: relationship_name
    }
    instrument(
      "dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_many_relationship.#{relationship_name}", instrumentation_arguments
    ) do
      return if parent_model_record.nil? || records.nil?

      dionysus_event = Dionysus::Consumer::DionysusEvent.new(original_event.event_name,
        relationship_name, records, aggregate_root: false)

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_many_relationship.#{relationship_name}.persist") do
        persist(dionysus_event, batch_number)
        original_event.local_changes.merge!(dionysus_event.local_changes)
      end

      synced_ids_of_related_records = records.map(&:synced_id)
      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_many_relationship.#{relationship_name}.resolve_to_many_association") do
        parent_model_record.resolve_to_many_association(relationship_name, synced_ids_of_related_records)
      end
    end
  end

  def instrument(label, options = {}, &block)
    config.instrumenter.instrument(label, options, &block)
  end
end
