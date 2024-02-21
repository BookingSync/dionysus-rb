# frozen_string_literal: true

class Dionysus::Producer::Outbox
  attr_reader :outbox_model, :config
  private     :outbox_model, :config

  def initialize(outbox_model, config:)
    @outbox_model = outbox_model
    @config = config
  end

  def insert_created(record)
    insert(Publishable.new(record), :created)
  end

  def insert_updated(record)
    publishable = Publishable.new(record)
    return [] unless publishable.previously_changed?

    if publishable.respond_to?(config.soft_delete_column)
      event_type = event_type_for_update_of_soft_deletable_record(publishable) or return []
      insert(publishable, event_type)
    else
      insert(publishable, :updated)
    end
  end

  def insert_destroyed(record)
    publishable = Publishable.new(record)

    insert(publishable, :destroyed)
  end

  private

  delegate :transaction_provider, :transactional_outbox_enabled, to: :config

  def insert(publishable, event_type)
    return [] unless transactional_outbox_enabled

    transaction_provider.transaction do
      publishable.topics.map do |topic|
        attributes = {
          resource: publishable.model,
          event_name: event_name_for(publishable, event_type),
          partition_key: partition_key_for_publishable_for_topic(publishable, topic),
          topic: topic
        }
        attributes[:changeset] = publishable.changeset if observer_topic?(topic)

        outbox_model.create!(attributes)
      end
    end
  end

  def partition_key_for_publishable_for_topic(publishable, topic)
    responder = Dionysus::Producer
      .responders_for_model_for_topic(publishable.model_class, topic)
      .first or return

    Dionysus::Producer::PartitionKey.new(publishable).to_key(responder: responder)
  end

  def observer_topic?(topic)
    topic == Dionysus::Producer::Outbox::Model.observer_topic
  end

  def event_name_for(publishable, event_type)
    Dionysus::Producer::Outbox::EventName
      .new(publishable.resource_name)
      .for_event_type(event_type)
  end

  def event_type_for_update_of_soft_deletable_record(publishable)
    if publishable.previous_changes_include_canceled?
      event_type_for_update_of_soft_deletable_record_for_soft_delete_state_change(publishable)
    else
      event_type_for_update_of_soft_deletable_record_for_standard_state_change(publishable)
    end
  end

  def event_type_for_update_of_soft_deletable_record_for_soft_delete_state_change(publishable)
    if publishable.previous_changes_uncanceled?
      :created
    elsif publishable.previous_changes_canceled? || publishable.previous_changes_still_canceled?
      :destroyed
    elsif publishable.previous_changed_still_visible?
      raise "that should never happen: a cannot be still visible when it was soft-deleted"
    else
      raise "that should never happen"
    end
  end

  def event_type_for_update_of_soft_deletable_record_for_standard_state_change(publishable)
    if publishable.visible? || (publishable.soft_deleted? && publishable.dionysus_publish_updates_after_soft_delete?)
      :updated
    elsif publishable.soft_deleted?
      nil # deliberately not returning any event because the record is configured as such
    else
      raise "that should never happen"
    end
  end
end
