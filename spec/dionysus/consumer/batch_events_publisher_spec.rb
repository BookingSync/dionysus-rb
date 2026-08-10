# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::BatchEventsPublisher do
  describe "#publish" do
    subject(:publish) { described_class.new(config, topic).publish(processed_events) }

    let(:config) { Dionysus::Consumer::Config.new.tap { |conf| conf.event_bus = event_bus } }
    let(:topic) { :v3_rentals }
    let(:processed_events) { [dionysus_event] }
    let(:dionysus_event) do
      Dionysus::Consumer::DionysusEvent.new("rental_created", "Rental", [{ "id" => 1 }])
    end
    let(:event_bus) do
      Class.new do
        attr_reader :published

        def initialize
          @published = []
        end

        def publish(name, payload)
          @published << [name, payload]
        end
      end.new
    end

    it "publishes the batch first and then every event on its own" do
      publish

      expect(event_bus.published.map(&:first)).to eq ["dionysus.consume_batch", "dionysus.consume"]
      expect(event_bus.published.first.last).to eq [
        {
          topic_name: "v3_rentals",
          event_name: "rental_created",
          model_name: "Rental",
          transformed_data: [{ "id" => 1 }],
          local_changes: {}
        }
      ]
    end

    context "when there are no events to publish" do
      let(:processed_events) { [] }

      it "does not publish an empty batch" do
        publish

        expect(event_bus.published).to be_empty
      end
    end
  end
end
