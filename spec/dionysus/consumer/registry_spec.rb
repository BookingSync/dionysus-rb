# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::Registry do
  describe "#namespace/#registrations" do
    subject(:registrations) { registry.registrations }

    let(:registry) { described_class.new }

    before do
      registry.namespace :v3 do
        deserializer Struct.new(:name).new("deserializer")

        topic :rentals
      end

      registry.namespace :v4 do
        deserializer Struct.new(:name).new("deserializer_v4")

        topic :rentals, mapper: "mapper" do
          dead_letter_queue(topic: "dead_messages", max_retries: 2)
        end
        topic :bookings, sidekiq: true, worker: "MyCustomWorkerClass", consumer_base_class: Karafka::BaseConsumer,
          concurrency: true, params_batch_transformation: "params_batch_transformation"
      end
    end

    it "performs the actual registration that is available under registrations attribute
    and generates Karafka Consumers" do
      expect(registrations.keys).to eq %i[v3 v4]

      expect(registrations[:v3].topics.count).to eq 1
      expect(registrations[:v3].deserializer_klass.name).to eq "deserializer"
      expect(registrations[:v3].consumers.count).to eq 1
      expect(registrations[:v3].consumers).to eq [Dionysus::V3RentalConsumer]
      expect(registrations[:v3].namespace).to eq :v3
      expect(registrations[:v3].topics.first.name).to eq :rentals
      expect(registrations[:v3].topics.first.namespace).to eq :v3
      expect(registrations[:v3].topics.first.to_s).to eq "v3_rentals"
      expect(registrations[:v3].topics.first.consumer).to eq Dionysus::V3RentalConsumer
      expect(registrations[:v3].topics.first.deserializer_klass.name).to eq "deserializer"
      expect(registrations[:v3].topics.first.options).to eq({})
      expect(registrations[:v3].topics.first.sidekiq_backend?).to be false
      expect(registrations[:v3].topics.first.sidekiq_worker).to be_nil
      expect(registrations[:v3].topics.first.consumer_base_class).to be_nil
      expect(registrations[:v3].topics.first.concurrency).to be_nil
      expect(registrations[:v3].topics.first.params_batch_transformation).to be_a(
        Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy
      )
      expect(registrations[:v3].topics.first.extensions_block).to be_nil

      expect(registrations[:v4].topics.count).to eq 2
      expect(registrations[:v4].deserializer_klass.name).to eq "deserializer_v4"
      expect(registrations[:v4].consumers.count).to eq 2
      expect(registrations[:v4].consumers).to eq [Dionysus::V4RentalConsumer,
        Dionysus::V4BookingConsumer]
      expect(registrations[:v4].namespace).to eq :v4
      expect(registrations[:v4].topics.first.name).to eq :rentals
      expect(registrations[:v4].topics.first.namespace).to eq :v4
      expect(registrations[:v4].topics.first.to_s).to eq "v4_rentals"
      expect(registrations[:v4].topics.first.consumer).to eq Dionysus::V4RentalConsumer
      expect(registrations[:v4].topics.first.deserializer_klass.name).to eq "deserializer_v4"
      expect(registrations[:v4].topics.first.options).to eq(mapper: "mapper")
      expect(registrations[:v4].topics.first.sidekiq_backend?).to be false
      expect(registrations[:v4].topics.first.sidekiq_worker).to be_nil
      expect(registrations[:v4].topics.first.consumer_base_class).to be_nil
      expect(registrations[:v4].topics.first.concurrency).to be_nil
      expect(registrations[:v4].topics.first.params_batch_transformation).to be_a(
        Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy
      )
      expect(registrations[:v4].topics.first.extensions_block).to be_present
      expect(registrations[:v4].topics.last.name).to eq :bookings
      expect(registrations[:v4].topics.last.namespace).to eq :v4
      expect(registrations[:v4].topics.last.to_s).to eq "v4_bookings"
      expect(registrations[:v4].topics.last.consumer).to eq Dionysus::V4BookingConsumer
      expect(registrations[:v4].topics.last.deserializer_klass.name).to eq "deserializer_v4"
      expect(registrations[:v4].topics.last.options).to eq(sidekiq: true, worker: "MyCustomWorkerClass",
        consumer_base_class: Karafka::BaseConsumer, concurrency: true,
        params_batch_transformation: "params_batch_transformation")
      expect(registrations[:v4].topics.last.sidekiq_backend?).to be true
      expect(registrations[:v4].topics.last.sidekiq_worker).to eq "MyCustomWorkerClass"
      expect(registrations[:v4].topics.last.consumer_base_class).to eq Karafka::BaseConsumer
      expect(registrations[:v4].topics.last.concurrency).to be true
      expect(registrations[:v4].topics.last.params_batch_transformation).to eq "params_batch_transformation"
      expect(registrations[:v4].topics.last.extensions_block).to be_nil
    end
  end
end
