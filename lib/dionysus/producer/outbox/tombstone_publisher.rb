# frozen_string_literal: true

class Dionysus::Producer::Outbox::TombstonePublisher
  TOMBSTONE = nil
  private_constant :TOMBSTONE

  attr_reader :config
  private :config

  def initialize(config: Dionysus::Producer.configuration)
    @config = config
  end

  def tombstone(resource, responder, options = {})
    partition_key = options.fetch(:partition_key) do
      Dionysus::Producer::PartitionKey.new(resource, config: config).to_key(responder: responder)
    end
    key = options.fetch(:key) { Dionysus::Producer::Key.new(resource).to_key }

    responder.call(TOMBSTONE, partition_key: partition_key, key: key)
  end
end
