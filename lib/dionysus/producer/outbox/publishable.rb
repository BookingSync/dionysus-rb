# frozen_string_literal: true

class Dionysus::Producer::Outbox::Publishable < SimpleDelegator
  attr_reader :model, :config, :soft_delete_column
  private     :config, :soft_delete_column

  def initialize(model, config: Dionysus::Producer.configuration)
    @model = model
    @config = config
    @soft_delete_column = config.soft_delete_column.to_s
    super(model)
  end

  def model_class
    model.class
  end

  def primary_key_attribute
    model.class.primary_key
  end

  def publishable_id
    model.public_send(primary_key_attribute)
  end

  def model_name
    model.class.model_name
  end

  def resource_name
    model_name.singular
  end

  def previously_changed?
    previous_changes.present?
  end

  def previous_changes_include_canceled?
    !!previous_changes[soft_delete_column]
  end

  def previous_changes_uncanceled?
    previous_changes_include_canceled? && previous_changes[soft_delete_column][0].present? && visible?
  end

  def previous_changes_canceled?
    previous_changes_include_canceled? && previous_changes[soft_delete_column][0].blank? && soft_deleted?
  end

  def previous_changes_still_canceled?
    previous_changes_include_canceled? && previous_changes[soft_delete_column][0].present? && soft_deleted?
  end

  # TODO: Check if this is needed
  def previous_changed_still_visible?
    previous_changes_include_canceled? && previous_changes[soft_delete_column][0].blank? && visible?
  end

  def soft_deleted?
    public_send(soft_delete_column).present?
  end

  def soft_deletable?
    model.respond_to?(soft_delete_column)
  end

  def visible?
    !soft_deleted?
  end

  def topics
    top_level_topics = Dionysus::Producer
      .responders_for(model.class)
      .map(&:primary_topic)
    topics_from_dependencies = Dionysus::Producer
      .responders_for_dependency_parent(model.class)
      .map(&:last)
      .map(&:primary_topic)

    [top_level_topics, topics_from_dependencies]
      .flatten
      .uniq
      .tap { |standard_topics| standard_topics << observer_topic if add_observer_topic? }
  end

  def changeset
    if model.destroyed?
      {
        model.class.primary_key => [model.public_send(model.class.primary_key), nil],
        "created_at" => [model.created_at, nil]
      }
    else
      model.previous_changes
    end
  end

  private

  delegate :observer_topic, to: Dionysus::Producer::Outbox::Model
  delegate :observers_with_responders_for, to: Dionysus::Producer
  delegate :outbox_model, to: :config

  def add_observer_topic?
    outbox_model.handles_changeset? && observers_with_responders_for(self, changeset).any?
  end
end
