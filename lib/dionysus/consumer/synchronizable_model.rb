# frozen_string_literal: true

class Dionysus::Consumer::SynchronizableModel < SimpleDelegator
  attr_reader :config
  private :config

  def initialize(config, model)
    @config = config
    super(model)
  end

  def model
    __getobj__
  end

  def synced_at
    if respond_to?(synced_updated_at_timestamp_attribute)
      public_send(synced_updated_at_timestamp_attribute)
    else
      public_send(synced_created_at_timestamp_attribute)
    end
  end

  def persist_with_dionysus?(event_updated_at)
    (synced_at && event_updated_at && event_updated_at >= synced_at) || synced_at.nil? || event_updated_at.nil?
  end

  def assign_attributes_from_dionysus(attributes)
    public_send("#{synced_data_attribute}=", attributes)
    reverse_mapping = config.attributes_mapping_for_model(model.model_name).to_a.map(&:reverse).to_h

    assignable_attributes = extract_assignable_attributes(attributes)
      .map { |key, value| apply_mapping(reverse_mapping, key, value) }
      .select { |attribute, _| respond_to?("#{attribute}=") }
      .to_h

    assign_attributes(assignable_attributes)
  end

  def remove_with_dionysus(deseralized_record)
    if soft_deleteable_but_cannot_be_soft_deleted_via_attribute_assignment?(deseralized_record)
      public_send(soft_delete_strategy)
    elsif (deseralized_record.has_synced_canceled_at? && !respond_to?("#{soft_deleted_at_timestamp_attribute}=")) ||
        !deseralized_record.has_synced_canceled_at?

      destroy
    end
  end

  def restore_with_dionysus
    model.public_send("#{soft_deleted_at_timestamp_attribute}=", nil)
  end

  def restorable?(deseralized_record)
    respond_to?("#{soft_deleted_at_timestamp_attribute}=") && !deseralized_record.has_synced_canceled_at?
  end

  private

  delegate :soft_delete_strategy, :synced_created_at_timestamp_attribute,
    :synced_updated_at_timestamp_attribute, :soft_deleted_at_timestamp_attribute,
    :synced_data_attribute, to: :config

  def soft_deleteable_but_cannot_be_soft_deleted_via_attribute_assignment?(deseralized_record)
    (respond_to?(soft_delete_strategy) && !deseralized_record.has_synced_canceled_at?) ||
      (respond_to?(soft_delete_strategy) && !respond_to?("#{soft_deleted_at_timestamp_attribute}="))
  end

  def extract_assignable_attributes(attributes)
    attributes.clone.tap do |hash|
      if synced_created_at_timestamp_attribute.to_s != "synced_created_at"
        hash[synced_created_at_timestamp_attribute] =
          hash["synced_created_at"]
      end
      if synced_updated_at_timestamp_attribute.to_s != "synced_updated_at"
        hash[synced_updated_at_timestamp_attribute] =
          hash["synced_updated_at"]
      end
      if soft_deleted_at_timestamp_attribute.to_s != "synced_canceled_at"
        hash[soft_deleted_at_timestamp_attribute] =
          hash["synced_canceled_at"]
      end
    end
  end

  def apply_mapping(reverse_mapping, key, value)
    if reverse_mapping.key?(key.to_sym)
      [reverse_mapping[key.to_sym], value]
    else
      [key, value]
    end
  end
end
