# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::Publisher do
  let(:outbox_record) do
    DionysusOutbox.new(
      resource_class: resource.class,
      resource_id: resource.id,
      event_name: event_name,
      topic: topic,
      changeset: changeset,
      partition_key: partition_key
    )
  end
  let(:resource) { ExampleResource.create!(account_id: account_id) }
  let(:changeset) do
    {}
  end
  let(:account_id) { 2 }
  let(:event_name) { "example_resource_created" }
  let(:topic) { "v102_rentals" }
  let(:partition_key) { nil }
  let(:other_topic) { "v102_other_rentals" }
  let(:instrumenter) { Dionysus::Utils::NullInstrumenter }
  let(:timeout) { 60 }
  let(:client_id) { "delivery_boy" }
  let(:event_bus) do
    Class.new do
      attr_reader :messages

      def initialize
        @messages = []
      end

      def publish(_message, payload)
        @messages << { message: payload }
      end
    end.new
  end
  let(:producer_config) { Dionysus::Producer.configuration }
  let(:observers_inline_maximum_size) { 100 }

  before do
    Dionysus::Producer.configure do |conf|
      conf.instrumenter = instrumenter
      conf.outbox_model = DionysusOutbox
      conf.default_partition_key = :id
      conf.transaction_provider = ActiveRecord::Base
      conf.observers_inline_maximum_size = observers_inline_maximum_size
    end
    allow(Dionysus).to receive(:logger).and_return(Logger.new($stdout))
    allow(Dionysus.logger).to receive(:error).and_call_original

    Dionysus::Producer.declare do
      namespace :v102 do
        serializer(Class.new do
          def self.serialize(records, dependencies:)
            records.map { |record| { id: record.id } }
          end
        end)

        topic :rentals, genesis_replica: true, partition_key: :account_id do
          publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
        end

        topic :other_rentals, genesis_replica: true, partition_key: :account_id do
          publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
        end

        topic :observer_to_one, genesis_replica: true do
          publish ExampleResource, observe: [
            {
              model: ExamplePublishableResource,
              attributes: %i[created_at],
              association_name: :example_resource
            },
            {
              model: ExamplePublishableCancelableResource,
              attributes: %i[id],
              association_name: :example_resources
            }
          ]
        end

        topic :observer_to_many, genesis_replica: true do
          publish ExampleResource, observe: [
            {
              model: ExamplePublishableCancelableResource,
              attributes: %i[updated_at],
              association_name: :example_resources
            },
            {
              model: ExamplePublishableResource,
              attributes: %i[id],
              association_name: :example_resource
            }
          ]
        end
      end
    end
  end

  describe "#publish" do
    subject(:publish) { described_class.new(config: producer_config).publish(outbox_record) }

    let(:publish_with_options) do
      described_class.new(config: producer_config).publish(outbox_record, publish_options)
    end
    let(:publish_options) do
      {}
    end

    describe "with delivery to Kafka" do
      it "works with real Kafka" do
        expect do
          publish
        end.not_to raise_error
      end
    end

    describe "without delivery to Kafka" do
      context "when producer is suppressed" do
        before do
          allow(Karafka.producer).to receive(:produce_sync).and_call_original
        end

        around do |example|
          Dionysus::Producer::Suppressor.suppress!

          example.run

          Dionysus::Producer::Suppressor.unsuppress!
        end

        it "doesn't deliver anything" do
          publish

          expect(Karafka.producer).not_to have_received(:produce_sync)
        end
      end

      context "when producer is not suppressed" do
        before do
          allow(instrumenter).to receive(:instrument).and_call_original
        end

        it "uses instrumentation" do
          publish

          expect(instrumenter).to have_received(:instrument).with("publishing_with_dionysus").at_least(1)
        end

        context "when a resource is something that has a responder" do
          before do
            allow(Karafka.producer).to receive(:produce_sync).and_call_original
            Dionysus::Producer.responders_for(ExampleResource).each do |responder|
              allow(responder).to receive(:call).and_call_original
            end
          end

          context "when partition key is persisted" do
            let(:partition_key) { "123123" }

            context "when it's genesis-only" do
              let(:publish_options) do
                {
                  genesis_only: true
                }
              end

              it "publishes messages to responders for the given topic" do
                publish_with_options

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                  .to all have_received(:call).with(
                    [["example_resource_created", resource, {}]],
                    key: "ExampleResource:#{resource.id}",
                    partition_key: "123123",
                    genesis_only: true
                  ).at_least(:once)
                expect(Karafka.producer).not_to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: "123123", topic: "v102_rentals"
                )
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: "123123", topic: "v102_rentals_genesis"
                ).at_least(:once)
              end
            end

            context "when no options are provided" do
              it "publishes messages to responders for the given topic" do
                publish

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                  .to all have_received(:call).with(
                    [["example_resource_created", resource, {}]],
                    key: "ExampleResource:#{resource.id}",
                    partition_key: "123123"
                  ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: "123123", topic: "v102_rentals"
                ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: "123123", topic: "v102_rentals_genesis"
                ).at_least(:once)
              end
            end

            describe "edge-cases" do
              context "when the event is _created and the record does not exist" do
                before do
                  ExampleResource.where(id: resource.id).delete_all
                end

                it "does not publish anything" do
                  publish

                  expect(Karafka.producer).not_to have_received(:produce_sync)
                end

                it "logs an error" do
                  publish

                  expect(Dionysus.logger).to have_received(:error)
                    .with(%r{Attempted to publish})
                end
              end

              context "when the event is _updated and the record does not exist" do
                let(:event_name) { "example_resource_updated" }

                before do
                  ExampleResource.where(id: resource.id).delete_all
                end

                it "does not publish anything" do
                  publish

                  expect(Karafka.producer).not_to have_received(:produce_sync)
                end

                it "logs an error" do
                  publish

                  expect(Dionysus.logger).to have_received(:error)
                    .with(%r{There was an update of})
                end
              end
            end
          end

          context "when partition key is not persisted" do
            context "when it's genesis-only" do
              let(:publish_options) do
                {
                  genesis_only: true
                }
              end

              it "publishes messages to responders for the given topic" do
                publish_with_options

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                  .to all have_received(:call).with(
                    [["example_resource_created", resource, {}]],
                    key: "ExampleResource:#{resource.id}",
                    partition_key: account_id.to_s,
                    genesis_only: true
                  ).at_least(:once)
                expect(Karafka.producer).not_to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                )
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: account_id.to_s, topic: "v102_rentals_genesis"
                ).at_least(:once)
              end
            end

            context "when no options are provided" do
              it "publishes messages to responders for the given topic" do
                publish

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                  .to all have_received(:call).with(
                    [["example_resource_created", resource, {}]],
                    key: "ExampleResource:#{resource.id}",
                    partition_key: account_id.to_s
                  ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_created",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{resource.id}", partition_key: account_id.to_s, topic: "v102_rentals_genesis"
                ).at_least(:once)
              end
            end
          end
        end

        context "when a resource is something that does not have a responder but is a dependency" do
          context "when it is a to-one relationship" do
            let(:event_name) { "example_publishable_resource_created" }

            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              Dionysus::Producer.responders_for(ExampleResource).each do |responder|
                allow(responder).to receive(:call).and_call_original
              end
            end

            context "when the parent exists" do
              let(:resource) { ExamplePublishableResource.create!(example_resource_id: parent.id) }
              let(:parent) { ExampleResource.create!(account_id: account_id) }

              context "when it's genesis-only" do
                let(:publish_options) do
                  {
                    genesis_only: true
                  }
                end

                it "publishes messages to responders for the given topic but for the parent
                as this is treated as the update on parent's side" do
                  publish_with_options

                  expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                  expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                    .to all have_received(:call).with(
                      [["example_resource_updated", parent, {}]],
                      key: "ExampleResource:#{parent.id}",
                      partition_key: account_id.to_s,
                      genesis_only: true
                    ).at_least(:once)
                  expect(Karafka.producer).not_to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: parent.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{parent.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                  )
                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: parent.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{parent.id}", partition_key: account_id.to_s, topic: "v102_rentals_genesis"
                  ).at_least(:once)
                end
              end

              context "when no options are provided" do
                it "publishes messages to responders for the given topic but for the parent
                as this is treated as the update on parent's side" do
                  publish

                  expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                  expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                    .to all have_received(:call).with(
                      [["example_resource_updated", parent, {}]],
                      key: "ExampleResource:#{parent.id}",
                      partition_key: account_id.to_s
                    ).at_least(:once)
                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: parent.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{parent.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                  ).at_least(:once)
                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: parent.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{parent.id}", partition_key: account_id.to_s, topic: "v102_rentals_genesis"
                  ).at_least(:once)
                end
              end
            end

            context "when the parent does not exist" do
              let(:resource) { ExamplePublishableResource.create!(example_resource_id: nil) }

              it "does not publish anything and does not blow up" do
                publish

                expect(Karafka.producer).not_to have_received(:produce_sync)
              end
            end
          end

          context "when it is a many-to-many relationship" do
            let(:event_name) { "example_publishable_cancelable_resource_created" }

            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              Dionysus::Producer.responders_for(ExampleResource).each do |responder|
                allow(responder).to receive(:call).and_call_original
              end
            end

            context "when the parents exist" do
              let!(:resource) { ExamplePublishableCancelableResource.create! }
              let!(:record) do
                ExampleResource.create!(account_id: account_id, example_publishable_cancelable_resource_id: resource.id)
              end

              context "when it's genesis-only" do
                let(:publish_options) do
                  {
                    genesis_only: true
                  }
                end

                it "publishes messages to responders for the given topic but for the parent
                as this is treated as the update on parent's side" do
                  publish_with_options

                  expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                  expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                    .to all have_received(:call).with(
                      [["example_resource_updated", record, {}]],
                      key: "ExampleResource:#{record.id}",
                      partition_key: account_id.to_s,
                      genesis_only: true
                    ).at_least(:once)
                  expect(Karafka.producer).not_to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: record.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{record.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                  )
                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: record.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{record.id}", partition_key: account_id.to_s, topic: "v102_rentals_genesis"
                  ).at_least(:once)
                end
              end

              context "when no options are provided" do
                it "publishes messages to responders for the given topic but for the parent
                as this is treated as the update on parent's side" do
                  publish

                  expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                  expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource, topic))
                    .to all have_received(:call).with(
                      [["example_resource_updated", record, {}]],
                      key: "ExampleResource:#{record.id}",
                      partition_key: account_id.to_s
                    ).at_least(:once)
                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: record.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{record.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                  ).at_least(:once)
                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_updated",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: record.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{record.id}", partition_key: account_id.to_s, topic: "v102_rentals_genesis"
                  ).at_least(:once)
                end
              end
            end

            context "when the parents do not exist" do
              let!(:resource) { ExamplePublishableCancelableResource.create! }
              let!(:record) do
                ExampleResource.create!(account_id: account_id, example_publishable_cancelable_resource_id: nil)
              end

              it "does not publish anything and does not blow up" do
                publish

                expect(Karafka.producer).not_to have_received(:produce_sync)
              end
            end
          end
        end

        context "when a resource neither has a responder, nor is a dependency" do
          let(:resource) do
            DionysusOutbox.create!(resource_class: "Whatever", resource_id: 1, event_name: "created",
              topic: "rentals")
          end

          it "does not do anything and surely doesn't blow up" do
            expect do
              publish
            end.not_to raise_error
          end
        end
      end
    end
  end

  describe "#publish_observers" do
    subject(:publish_observers) do
      described_class.new(config: producer_config).publish_observers(outbox_record)
    end

    before do
      Dionysus::Producer.configure do |conf|
        conf.default_partition_key = :account_id
      end
    end

    context "when producer is suppressed" do
      around do |example|
        Dionysus::Producer::Suppressor.suppress!

        example.run

        Dionysus::Producer::Suppressor.unsuppress!
      end

      let(:outbox_record) do
        DionysusOutbox.new(
          resource_class: resource.class,
          resource_id: resource.id,
          event_name: event_name,
          topic: "v102_rentals",
          changeset: changeset
        )
      end
      let(:changeset) do
        {
          "created_at" => [nil, "2018-01-01T00:00:00.000Z"]
        }
      end
      let(:observer_resource) { ExampleResource.create!(account_id: account_id) }
      let(:resource) do
        ExamplePublishableResource.create!(account_id: account_id, example_resource: observer_resource)
      end

      before do
        allow(Karafka.producer).to receive(:produce_sync).and_call_original
      end

      it "does not publish anything" do
        publish_observers

        expect(Karafka.producer).not_to have_received(:produce_sync)
      end
    end

    context "when producer is not suppressed" do
      context "when there is an observer defined" do
        context "when at least one of the specified attributes has changed" do
          context "when there is a to-one relationship" do
            let(:outbox_record) do
              DionysusOutbox.new(
                resource_class: resource.class,
                resource_id: resource.id,
                event_name: event_name,
                topic: "__outbox__observer__",
                changeset: changeset
              )
            end
            let(:changeset) do
              {
                "created_at" => [nil, "2018-01-01T00:00:00.000Z"]
              }
            end
            let(:resource) do
              ExamplePublishableResource.create!(account_id: account_id, example_resource: observer_resource)
            end

            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              Dionysus::Producer.responders_for(ExampleResource).each do |responder|
                allow(responder).to receive(:call).and_call_original
              end
            end

            context "when the related record is nil" do
              let(:observer_resource) { nil }

              it "does not publish anything" do
                publish_observers

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::V102RentalResponder).not_to have_received(:call)
                expect(Dionysus::V102ObserverToManyResponder).not_to have_received(:call)
                expect(Dionysus::V102ObserverToOneResponder).not_to have_received(:call)
              end
            end

            context "when the related record is present" do
              let(:observer_resource) { ExampleResource.create!(account_id: account_id) }

              it "publishes the observers" do
                publish_observers

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::V102RentalResponder).not_to have_received(:call)
                expect(Dionysus::V102ObserverToManyResponder).not_to have_received(:call)
                expect(Dionysus::V102ObserverToOneResponder).to have_received(:call)
                expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource,
                  :observer_to_one))
                  .to all have_received(:call).with(
                    [["example_resource_updated", observer_resource, {}]],
                    key: "ExampleResource:#{observer_resource.id}",
                    partition_key: account_id.to_s
                  ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_updated",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: observer_resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{observer_resource.id}", partition_key: account_id.to_s,
                  topic: "v102_observer_to_one"
                ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_updated",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: observer_resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{observer_resource.id}", partition_key: account_id.to_s,
                  topic: "v102_observer_to_one_genesis"
                ).at_least(:once)
              end
            end
          end

          context "when there is a to-many relationship" do
            let(:outbox_record) do
              DionysusOutbox.new(
                resource_class: resource.class,
                resource_id: resource.id,
                event_name: event_name,
                topic: "__outbox__observer__",
                changeset: changeset
              )
            end
            let(:changeset) do
              {
                "updated_at" => [nil, "2018-01-01T00:00:00.000Z"]
              }
            end
            let(:observer_resource) { ExampleResource.create!(account_id: account_id) }
            let(:resource) do
              ExamplePublishableCancelableResource.create!(account_id: account_id,
                example_resources: [observer_resource])
            end

            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              Dionysus::Producer.responders_for(ExampleResource).each do |responder|
                allow(responder).to receive(:call).and_call_original
              end
              allow(Dionysus::Producer::Genesis::Streamer::StandardJob).to receive(:enqueue)
                .and_call_original
            end

            context "when observers_inline_maximum_size is reached" do
              let(:observers_inline_maximum_size) { 0 }

              it "enqueues Dionysus::Producer::Genesis::Streamer::StandardJob" do
                expect(Dionysus::Producer::Genesis::Streamer::StandardJob).not_to have_enqueued_sidekiq_job

                publish_observers

                expect(Dionysus::Producer::Genesis::Streamer::StandardJob).to have_received(:enqueue)
                expect(Dionysus::Producer::Genesis::Streamer::StandardJob).to have_enqueued_sidekiq_job(
                  [observer_resource.id], "ExampleResource", "v102_observer_to_many"
                )
              end
            end

            context "when observers_inline_maximum_size is not reached" do
              it "does not enqueue Dionysus::Producer::Genesis::Streamer::StandardJob" do
                publish_observers

                expect(Dionysus::Producer::Genesis::Streamer::StandardJob).not_to have_received(:enqueue)
              end

              it "publishes the observers" do
                publish_observers

                expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
                expect(Dionysus::V102RentalResponder).not_to have_received(:call)
                expect(Dionysus::V102ObserverToOneResponder).not_to have_received(:call)
                expect(Dionysus::V102ObserverToManyResponder).to have_received(:call)
                expect(Dionysus::Producer.responders_for_model_for_topic(ExampleResource,
                  :observer_to_many))
                  .to all have_received(:call).with(
                    [["example_resource_updated", observer_resource, {}]],
                    key: "ExampleResource:#{observer_resource.id}",
                    partition_key: account_id.to_s
                  ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_updated",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: observer_resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{observer_resource.id}", partition_key: account_id.to_s,
                  topic: "v102_observer_to_many"
                ).at_least(:once)
                expect(Karafka.producer).to have_received(:produce_sync).with(
                  payload: {
                    "message" => [
                      {
                        "event" => "example_resource_updated",
                        "model_name" => "ExampleResource",
                        "data" => [{ id: observer_resource.id }]
                      }
                    ]
                  }.to_json,
                  key: "ExampleResource:#{observer_resource.id}", partition_key: account_id.to_s,
                  topic: "v102_observer_to_many_genesis"
                ).at_least(:once)
              end
            end
          end
        end

        context "when none one of the specified attributes have changed" do
          context "when there is a to-one relationship" do
            let(:outbox_record) do
              DionysusOutbox.new(
                resource_class: resource.class,
                resource_id: resource.id,
                event_name: event_name,
                topic: "__outbox__observer__",
                changeset: changeset
              )
            end
            let(:changeset) do
              {
                "updated_at" => [nil, "2018-01-01T00:00:00.000Z"]
              }
            end
            let(:observer_resource) { ExampleResource.create!(account_id: account_id) }
            let(:resource) do
              ExamplePublishableResource.create!(account_id: account_id, example_resource: observer_resource)
            end

            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              Dionysus::Producer.responders_for(ExampleResource).each do |responder|
                allow(responder).to receive(:call).and_call_original
              end
            end

            it "does not publish the observers" do
              publish_observers

              expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
              expect(Dionysus::V102RentalResponder).not_to have_received(:call)
              expect(Dionysus::V102ObserverToManyResponder).not_to have_received(:call)
              expect(Dionysus::V102ObserverToOneResponder).not_to have_received(:call)
            end
          end

          context "when there is a to-many relationship" do
            let(:outbox_record) do
              DionysusOutbox.new(
                resource_class: resource.class,
                resource_id: resource.id,
                event_name: event_name,
                topic: "__outbox__observer__",
                changeset: changeset
              )
            end
            let(:changeset) do
              {
                "created_at" => [nil, "2018-01-01T00:00:00.000Z"]
              }
            end
            let(:observer_resource) { ExampleResource.create!(account_id: account_id) }
            let(:resource) do
              ExamplePublishableCancelableResource.create!(account_id: account_id,
                example_resources: [observer_resource])
            end

            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              Dionysus::Producer.responders_for(ExampleResource).each do |responder|
                allow(responder).to receive(:call).and_call_original
              end
            end

            it "does not publish the observers" do
              publish_observers

              expect(Dionysus::V102OtherRentalResponder).not_to have_received(:call)
              expect(Dionysus::V102RentalResponder).not_to have_received(:call)
              expect(Dionysus::V102ObserverToOneResponder).not_to have_received(:call)
              expect(Dionysus::V102ObserverToManyResponder).not_to have_received(:call)
            end
          end
        end
      end

      context "when there is no observer defined" do
        let(:outbox_record) do
          DionysusOutbox.new(
            resource_class: resource.class,
            resource_id: resource.id,
            event_name: event_name,
            topic: "__outbox__observer__",
            changeset: changeset
          )
        end
        let(:changeset) do
          {
            "created_at" => [nil, "2018-01-01T00:00:00.000Z"]
          }
        end
        let(:resource) { ExampleResource.create!(account_id: account_id) }

        before do
          allow(Karafka.producer).to receive(:produce_sync).and_call_original
        end

        it "does not publish anything" do
          publish_observers

          expect(Karafka.producer).not_to have_received(:produce_sync)
        end
      end
    end
  end
end
