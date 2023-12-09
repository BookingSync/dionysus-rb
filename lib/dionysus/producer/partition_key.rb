# frozen_string_literal: true

class Dionysus::Producer::PartitionKey
  attr_reader :resource, :config
  private     :resource, :config

  def initialize(resource, config: Dionysus::Producer.configuration)
    @resource = resource
    @config = config
  end

  def to_key(responder:)
    if has_custom_partition_key?(responder)
      apply_custom_partition_key(responder)
    else
      apply_default_partition_key
    end
  end

  private

  def has_custom_partition_key?(responder)
    responder.partition_key.present?
  end

  def apply_custom_partition_key(responder)
    apply_partition_key(responder.partition_key)
  end

  def apply_default_partition_key
    apply_partition_key(config.default_partition_key)
  end

  def apply_partition_key(partition_key)
    if partition_key.respond_to?(:call)
      resolved_partition_key = partition_key.call(resource)
      resolved_partition_key&.to_s
    elsif resource.respond_to?(partition_key)
      resource.public_send(partition_key).to_i.to_s
    end
  end
end
