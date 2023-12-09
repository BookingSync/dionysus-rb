# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::TombstonePublisher do
  describe "#tombstone" do
    subject(:tombstone) do
      publisher.tombstone(resource, responder, partition_key: partition_key, key: key)
    end

    let(:publisher) { described_class.new }
    let(:responder) { Dionysus::V102RentalResponder }

    before do
      Dionysus::Producer.declare do
        namespace :v102 do
          serializer(Class.new do
            def self.serialize(records, dependencies:)
              records.map(&:as_json)
            end
          end)

          topic :rentals do
            publish ExampleResource
          end
        end
      end
    end

    describe "without delivery to Kafka" do
      let(:resource) { ExampleResource.new(account_id: 10, id: 20) }

      before do
        allow(responder).to receive(:call).and_call_original
        allow(Karafka.producer).to receive(:produce_sync).and_call_original
      end

      context "when partition key and key are specified" do
        let(:partition_key) { "1" }
        let(:key) { "1" }

        it "publishes tombstone to the responder" do
          tombstone

          expect(responder).to have_received(:call).with(nil, key: key, partition_key: partition_key)
          expect(Karafka.producer).to have_received(:produce_sync).with(
            payload: nil.to_json, key: key, partition_key: partition_key, topic: "v102_rentals"
          )
        end
      end

      context "when partition key or key are not specified" do
        subject(:tombstone) { publisher.tombstone(resource, responder) }

        it "publishes tombstone to the responder and resolves keys based on the passed resource" do
          tombstone

          expect(responder).to have_received(:call).with(nil, key: "ExampleResource:#{resource.id}",
            partition_key: resource.account_id.to_s)
          expect(Karafka.producer).to have_received(:produce_sync).with(
            payload: nil.to_json, key: "ExampleResource:#{resource.id}", partition_key: resource.account_id.to_s,
            topic: "v102_rentals"
          )
        end
      end
    end
  end
end
