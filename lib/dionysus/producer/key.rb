# frozen_string_literal: true

class Dionysus::Producer::Key
  attr_reader :resource
  private :resource

  def initialize(resource)
    @resource = resource
  end

  def to_key
    "#{resource.model_name}:#{resource.id}"
  end
end
