# frozen_string_literal: true

class Dionysus::Consumer::Persistor
  attr_reader :config, :topic
  private     :config, :topic

  def initialize(config, topic)
    @config = config
    @topic = topic
  end

  def persist(dionysus_event, batch_number, under_stale_parent: false)
    if dionysus_event.generic_event?
      if dionysus_event.created? && topic.options[:import] == true
        persist_via_dionysus_create(dionysus_event, batch_number)
      elsif dionysus_event.destroyed? && topic.options[:import] == true
        persist_via_dionysus_destroy(dionysus_event, batch_number)
      else
        persist_standard_event(dionysus_event, batch_number, under_stale_parent: under_stale_parent)
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

  def persist_standard_event(dionysus_event, batch_number, under_stale_parent: false)
    Array.wrap(dionysus_event.transformed_data).each do |deseralized_record|
      model_klass = find_model_klass(dionysus_event) or return
      synced_id = deseralized_record.synced_id

      if synced_id.nil?
        Dionysus.logger.error("[Dionysus] synced_id nil for #{deseralized_record}, that should never happen!")
        next
      end

      record = Dionysus::Consumer::SynchronizableModel.new(config,
        model_klass.find_or_initialize_by(config.synced_id_attribute => synced_id))
      event_updated_at = deseralized_record.synced_updated_at || deseralized_record.synced_created_at
      persistable = persistable?(record, event_updated_at, under_stale_parent)

      apply_record(dionysus_event, record, deseralized_record, synced_id, batch_number) if persistable

      # children re-enter persist and are guarded on their own timestamp; only the child list is untrusted
      persist_relationships(dionysus_event, record, deseralized_record, batch_number,
        under_stale_parent: under_stale_parent || !persistable)
    end
  end

  def persistable?(record, event_updated_at, under_stale_parent)
    if under_stale_parent
      record.advances_dionysus_state?(event_updated_at)
    else
      record.persist_with_dionysus?(event_updated_at)
    end
  end

  def apply_record(dionysus_event, record, deseralized_record, synced_id, batch_number)
    record.assign_attributes_from_dionysus(deseralized_record.attributes)
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
  end

  def persist_relationships(dionysus_event, record, deseralized_record, batch_number, under_stale_parent:)
    instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_many_relationships") do
      deseralized_record.has_many.each do |relationship|
        persist_to_many_relationship(dionysus_event, relationship, record, batch_number, under_stale_parent: under_stale_parent)
      end
    end

    instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_one_relationships") do
      deseralized_record.has_one.each do |relationship|
        persist_to_one_relationship(dionysus_event, relationship, record, batch_number, under_stale_parent: under_stale_parent)
      end
    end
  end

  def log_unknown_event_type(dionysus_event)
    Dionysus.logger.debug("[Dionysus] unknown event type #{dionysus_event.event_name}")
  end

  def persist_to_one_relationship(original_event, relationship, parent_model_record, batch_number,
    under_stale_parent: false)
    relationship_name, record = relationship
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
        persist(dionysus_event, batch_number, under_stale_parent: under_stale_parent)
        original_event.local_changes.merge!(dionysus_event.local_changes)
      end

      return if under_stale_parent

      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_one_relationship.#{relationship_name}.resolve_to_one_association") do
        parent_model_record.resolve_to_one_association(relationship_name, record.synced_id)
      end
    end
  end

  def persist_to_many_relationship(original_event, relationship, parent_model_record, batch_number,
    under_stale_parent: false)
    relationship_name, records = relationship
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
        persist(dionysus_event, batch_number, under_stale_parent: under_stale_parent)
        original_event.local_changes.merge!(dionysus_event.local_changes)
      end

      # the purge needs a complete current list, which a payload already judged stale cannot provide
      return if under_stale_parent

      synced_ids_of_related_records = records.map(&:synced_id)
      instrument("dionysus.consume.#{topic}.batch_number_#{batch_number}.persist.persist_to_many_relationship.#{relationship_name}.resolve_to_many_association") do
        parent_model_record.resolve_to_many_association(relationship_name, synced_ids_of_related_records)
      end
    end
  end

  def instrument(label, options = {}, &)
    config.instrumenter.instrument(label, options, &)
  end
end
