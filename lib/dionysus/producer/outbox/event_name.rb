# frozen_string_literal: true

class Dionysus::Producer::Outbox::EventName
  attr_reader :resource_name
  private :resource_name

  def initialize(resource_name)
    @resource_name = resource_name
  end

  def created
    "#{resource_name}_created"
  end

  def updated
    "#{resource_name}_updated"
  end

  def destroyed
    "#{resource_name}_destroyed"
  end

  def for_event_type(type)
    public_send(type)
  end
end
