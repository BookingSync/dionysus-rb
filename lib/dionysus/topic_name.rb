# frozen_string_literal: true

class Dionysus::TopicName
  attr_reader :namespace, :name
  private     :namespace, :name

  def initialize(namespace, name)
    @namespace = namespace
    @name = name
  end

  def to_s
    "#{namespace}_#{name}"
  end
end
