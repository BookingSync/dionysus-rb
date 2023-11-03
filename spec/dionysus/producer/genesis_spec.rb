# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Genesis do
  describe "#stream", :freeze_time do
    subject(:stream) do
      described_class.new.stream(topic: topic, model: model, from: 1.day.ago, to: 1.day.from_now,
        number_of_days: 7)
    end

    let(:topic) { "v102_rentals" }
    let(:model) { ExampleResource }
    let(:routing_key) { "dionysus.producer.genesis.performed" }
    let(:event_payload) do
      {
        model: model.to_s,
        service: "dionysus",
        topic: topic,
        start_at: Time.current.as_json,
        end_at: (7.days.from_now + consistency_safety_delay).as_json
      }
    end
    let(:job_klass) { "Dionysus::Producer::Genesis::Streamer::StandardJob" }
    let(:consistency_safety_delay) { 60.seconds }

    before do
      allow(Dionysus::Producer::Genesis::StreamJob).to receive(:set).and_call_original

      Dionysus::Producer.declare do
        namespace :v102 do
          serializer(Class.new do
            def self.serialize(records, dependencies:)
              records.map { |record| { id: record.id } }
            end
          end)

          topic :rentals do
            publish ExampleResource
            publish ExamplePublishableCancelableResource, with: [ExampleResource]
          end

          topic :other do
            publish ExamplePublishableCancelableResource, with: [ExampleResource]
          end
        end
      end
    end

    around do |example|
      hermes_config = Hermes.configuration
      hermes_application_prefix = hermes_config.application_prefix
      hermes_original_clock = Hermes.configuration.clock
      hermes_original_adapter = hermes_config.adapter

      Hermes.configure do |configuration|
        configuration.clock = Time
        configuration.adapter = :in_memory
        configuration.application_prefix = "genesis_spec"
      end
      Hermes::Publisher.instance.current_adapter.store.clear

      example.run

      Hermes.configure do |configuration|
        configuration.clock = hermes_original_clock
        configuration.adapter = hermes_original_adapter
        configuration.application_prefix = hermes_application_prefix
      end
    end

    context "when all checks pass" do
      it "enqueues a stream job" do
        expect(Dionysus::Producer::Genesis::StreamJob).not_to have_enqueued_sidekiq_job(
          topic, model.to_s, 1.day.ago.as_json, 1.day.from_now.as_json, 7, job_klass
        )

        stream

        expect(Dionysus::Producer::Genesis::StreamJob).to have_enqueued_sidekiq_job(
          topic, model.to_s, 1.day.ago.as_json, 1.day.from_now.as_json, 7, job_klass
        )
      end

      it "uses a queue from the config" do
        stream

        expect(Dionysus::Producer::Genesis::StreamJob).to have_received(:set)
          .with(queue: :dionysus)
      end

      context "when Hermes Event producer is provided in the config" do
        before do
          allow(Dionysus::Producer.configuration).to receive(:hermes_event_producer).and_return(Hermes::EventProducer)
        end

        it "publishes a genesis performed event" do
          expect do
            stream
          end.to publish_async_message(routing_key).with_event_payload(event_payload)
        end
      end

      context "when Hermes Event producer is not provided in the config" do
        before do
          allow(Dionysus::Producer.configuration).to receive(:hermes_event_producer).and_return(Dionysus::Utils::NullHermesEventProducer)
        end

        it "does not publish a genesis performed event" do
          expect do
            stream
          end.not_to publish_async_message(routing_key).with_event_payload(event_payload)
        end
      end
    end

    context "when some checks don't pass" do
      context "when model is just a dependency" do
        let(:topic) { "v102_other" }

        it { is_expected_block.to raise_error(%r{Cannot execute genesis for model}) }
      end
    end
  end
end
