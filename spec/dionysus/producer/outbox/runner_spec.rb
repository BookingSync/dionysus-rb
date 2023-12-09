# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::Runner, :freeze_time, :with_outbox_config do
  describe "#start" do
    subject(:start) { runner.start }

    let(:runner) { described_class.new(logger: logger) }
    let(:database_connection_provider) { double(connection: double(:connection, reconnect!: true)) }
    let(:logger) do
      Class.new do
        def initialize
          @info = []
          @debug = []
          @error = []
        end

        def info(message)
          @info << message
        end

        def debug(message)
          @debug << message
        end

        def error(message)
          @error << message
        end
      end.new
    end

    let!(:outbox_record_1) do
      DionysusOutbox.create!(resource_class: resource_1.class, resource_id: resource_1.id, event_name: event_name,
        created_at: Time.current, topic: "v102_rentals", published_at: nil)
    end
    let!(:resource_1) { ExampleResource.create!(account_id: account_id) }
    let!(:outbox_record_2) do
      DionysusOutbox.create!(resource_class: resource_2.class, resource_id: resource_2.id, event_name: event_name,
        created_at: 1.year.from_now, topic: "v102_rentals_another_topic", published_at: nil)
    end
    let!(:resource_2) { ExampleResource.create!(account_id: account_id) }
    let(:account_id) { 2 }
    let(:event_name) { "example_resource_created" }

    before do
      Dionysus::Producer.configure do |conf|
        conf.database_connection_provider = database_connection_provider
        conf.outbox_model = DionysusOutbox
        conf.default_partition_key = :id
      end

      Dionysus::Producer.declare do
        namespace :v102 do
          serializer(Class.new do
            def self.serialize(records, dependencies:)
              records.map { |record| { id: record.id } }
            end
          end)

          topic :rentals, partition_key: :account_id do
            publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
          end

          topic :rentals_another_topic, partition_key: :account_id do
            publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
          end
        end
      end

      DionysusOutbox.where.not(id: [outbox_record_1, outbox_record_2]).destroy_all

      allow(Karafka.producer).to receive(:produce_sync).and_call_original
      allow(database_connection_provider.connection).to receive(:reconnect!)
      allow(Dionysus::Utils::NullLockClient).to receive(:lock).and_call_original
      allow(Dionysus.monitor).to receive(:instrument).and_call_original
    end

    context "when error happens" do
      let(:error) { StandardError.new("error") }

      before do
        allow(Dionysus::Utils::NullErrorHandler).to receive(:capture_exception).and_call_original
      end

      context "when the error is caused by something different than a publishing error" do
        before do
          allow(database_connection_provider).to receive(:connection).and_raise(error)
        end

        it "raises and reports error" do
          expect do
            start
          end.to raise_error(error)

          expect(Dionysus::Utils::NullErrorHandler).to have_received(:capture_exception).with(error)
          expect(Dionysus.monitor).to have_received(:instrument).with("outbox_producer.error",
            error: error, error_message: error.message)
        end
      end

      context "when the error is caused by something by publishing error" do
        before do
          allow(Karafka.producer).to receive(:produce_sync).and_raise(error)
          Thread.new { start }
          sleep 0.5
        end

        after do
          runner.stop
        end

        it "does not raise the error but it reports the error" do
          expect(Dionysus::Utils::NullErrorHandler).to have_received(:capture_exception)
            .with(StandardError.new("error")).at_least(:once)
          expect(Dionysus.monitor).to have_received(:instrument)
            .with("outbox_producer.publishing_failed", outbox_record: instance_of(DionysusOutbox)).at_least(:once)
        end
      end
    end

    context "when no error happens" do
      before do
        Thread.new { start }
        sleep 0.5
      end

      after do
        runner.stop
      end

      it "ensures that the database connection is established" do
        expect(database_connection_provider.connection).to have_received(:reconnect!)
      end

      it "uses Redlock" do
        expect(Dionysus::Utils::NullLockClient).to have_received(:lock).with(
          "dionysus_v102_rentals_lock", 10_000
        )
      end

      it "publishes records to Kafka in the right order" do
        expect(Karafka.producer).to have_received(:produce_sync).with(
          payload: {
            "message" => [
              {
                "event" => "example_resource_created",
                "model_name" => "ExampleResource",
                "data" => [{ id: resource_1.id }]
              }
            ]
          }.to_json,
          key: "ExampleResource:#{resource_1.id}", partition_key: account_id.to_s, topic: "v102_rentals"
        )
        expect(Karafka.producer).to have_received(:produce_sync).with(
          payload: {
            "message" => [
              {
                "event" => "example_resource_created",
                "model_name" => "ExampleResource",
                "data" => [{ id: resource_2.id }]
              }
            ]
          }.to_json,
          key: "ExampleResource:#{resource_2.id}", partition_key: account_id.to_s, topic: "v102_rentals_another_topic"
        )
      end

      it "marks the records as published" do
        expect(outbox_record_1.reload.published_at).to eq Time.current
        expect(outbox_record_2.reload.published_at).to eq Time.current
      end

      it "handles instrumentation" do
        expect(Dionysus.monitor).to have_received(:instrument).with("outbox_producer.started")
        expect(Dionysus.monitor).to have_received(:instrument).with("outbox_producer.processing_topic",
          topic: "v102_rentals")
        expect(Dionysus.monitor).to have_received(:instrument).with("outbox_producer.published",
          outbox_record: instance_of(DionysusOutbox)).at_least(:once)
        expect(Dionysus.monitor).to have_received(:instrument).with("outbox_producer.processed_topic",
          topic: "v102_rentals")
        expect(Dionysus.monitor).to have_received(:instrument).with("outbox_producer.heartbeat")
          .at_least(:once)
      end
    end
  end
end
