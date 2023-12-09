# frozen_string_literal: true

class Dionysus::Consumer::DionysusEvent
  attr_reader :event_name, :model_name, :transformed_data, :local_changes

  def initialize(event_name, model_name, transformed_data, aggregate_root: true)
    @event_name = event_name.to_s
    @model_name = model_name.to_s
    @transformed_data = transformed_data
    @local_changes = {}
    @aggregate_root = aggregate_root
  end

  def created?
    event_name.end_with?("created")
  end

  def updated?
    event_name.end_with?("updated")
  end

  def destroyed?
    event_name.end_with?("destroyed")
  end

  def generic_event?
    created? || updated? || destroyed?
  end

  def aggregate_root?
    @aggregate_root == true
  end

  def to_h
    {
      event_name: event_name,
      model_name: model_name,
      transformed_data: transformed_data,
      local_changes: local_changes
    }
  end
end
