# frozen_string_literal: true

class Dionysus::Producer::Outbox::Producer
  attr_reader :config
  private :config

  def initialize(config: Dionysus::Producer.configuration)
    @config = config
  end

  def call(topic, batch_size: config.outbox_publishing_batch_size)
    outbox_model.fetch_publishable(batch_size, topic).to_a.tap do |records_to_publish|
      records_processor.call(records_to_publish) do |record|
        yield record if block_given?
      end
    end
  end

  private

  delegate :outbox_model, to: :config

  def records_processor
    @records_processor ||= Dionysus::Producer::Outbox::RecordsProcessor.new
  end
end
