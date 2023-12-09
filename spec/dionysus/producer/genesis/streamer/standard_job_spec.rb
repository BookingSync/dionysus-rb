# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Genesis::Streamer::StandardJob do
  it { is_expected.to be_processed_in :dionysus }

  describe ".enqueue", :freeze_time do
    subject(:enqueue) do
      described_class.enqueue(resources, model_class, topic, number_of_days: number_of_days, batch_size: batch_size)
    end

    let(:number_of_days) { 2 }
    let(:batch_size) { 1 }
    let(:resource_1) { ExampleResource.create(account_id: 1) }
    let(:resource_2) { ExampleResource.create(account_id: 2) }
    let(:resources) { ExampleResource.where(id: [resource_1.id, resource_2.id]) }
    let(:model_class) { ExampleResource }
    let(:topic) { "other" }

    before do
      allow(described_class).to receive(:set).and_return(described_class)
      allow(described_class).to receive(:perform_in).and_call_original
    end

    it "distributes batches over time" do
      expect(described_class).not_to have_enqueued_sidekiq_job([resource_1.id], "ExampleResource", topic)
      expect(described_class).not_to have_enqueued_sidekiq_job([resource_2.id], "ExampleResource", topic)

      enqueue

      expect(described_class).to have_enqueued_sidekiq_job([resource_1.id], "ExampleResource", topic)
      expect(described_class).to have_enqueued_sidekiq_job([resource_2.id], "ExampleResource", topic)
      expect(described_class).to have_received(:perform_in).with(0, [resource_1.id], "ExampleResource", topic)
      expect(described_class).to have_received(:perform_in).with(86_400, [resource_2.id], "ExampleResource", topic)
    end
  end

  describe "#perform" do
    subject(:perform) { described_class.new.perform(ids, model_class.to_s, topic) }

    let(:ids) { [resource_1.id] }

    before do
      Dionysus::Producer.configure do |config|
        config.outbox_model = DionysusOutbox
        config.transaction_provider = ActiveRecord::Base
      end

      Dionysus::Producer.declare do
        namespace :v102 do
          serializer(Class.new do
            def self.serialize(records, dependencies:)
              records.map { |record| { id: record.id } }
            end
          end)

          topic :rentals, genesis_replica: true do
            publish ExampleResource
            publish ExamplePublishableCancelableResource
          end

          topic :other do
            publish ExampleResource
            publish ExamplePublishableCancelableResource
          end
        end
      end
    end

    context "when executing for Genesis-only" do
      let(:topic) { "v102_rentals_genesis" }

      context "when resource is not soft-deletable" do
        let(:resource_1) { ExampleResource.create(account_id: 1) }
        let(:model_class) { ExampleResource.to_s }

        before do
          allow(Dionysus::Producer.responders_for_model_for_topic(model_class,
            topic).first).to receive(:call).and_call_original
        end

        it "publishes update events" do
          perform

          expect(Dionysus::Producer.responders_for_model_for_topic(model_class, topic))
            .to all have_received(:call).with(
              [["example_resource_updated", resource_1, {}]],
              key: "ExampleResource:#{resource_1.id}",
              partition_key: resource_1.account_id.to_s,
              genesis_only: true
            )
        end
      end

      context "when resource is soft-deletable" do
        context "when resource is soft-deleted" do
          let(:resource_1) { ExamplePublishableCancelableResource.create(account_id: 1, canceled_at: Time.current) }
          let(:model_class) { ExamplePublishableCancelableResource }

          before do
            allow(Dionysus::Producer.responders_for_model_for_topic(model_class,
              topic).first).to receive(:call).and_call_original
          end

          it "publishes destroyed events" do
            perform

            expect(Dionysus::Producer.responders_for_model_for_topic(model_class, topic))
              .to all have_received(:call).with(
                [["example_publishable_cancelable_resource_destroyed", resource_1, {}]],
                key: "ExamplePublishableCancelableResource:#{resource_1.id}",
                partition_key: resource_1.account_id.to_s,
                genesis_only: true
              )
          end
        end

        context "when resource is not soft-deleted" do
          let(:resource_1) { ExamplePublishableCancelableResource.create(account_id: 1) }
          let(:model_class) { ExamplePublishableCancelableResource.to_s }

          before do
            allow(Dionysus::Producer.responders_for_model_for_topic(model_class,
              topic).first).to receive(:call).and_call_original
          end

          it "publishes update events" do
            perform

            expect(Dionysus::Producer.responders_for_model_for_topic(model_class, topic))
              .to all have_received(:call).with(
                [["example_publishable_cancelable_resource_updated", resource_1, {}]],
                key: "ExamplePublishableCancelableResource:#{resource_1.id}",
                partition_key: resource_1.account_id.to_s,
                genesis_only: true
              )
          end
        end
      end
    end

    context "when not executing for Genesis-only" do
      let(:topic) { "v102_rentals" }

      context "when resource is not soft-deletable" do
        let(:resource_1) { ExampleResource.create(account_id: 1) }
        let(:model_class) { ExampleResource.to_s }

        before do
          allow(Dionysus::Producer.responders_for_model_for_topic(model_class,
            topic).first).to receive(:call).and_call_original
        end

        it "publishes update events" do
          perform

          expect(Dionysus::Producer.responders_for_model_for_topic(model_class, topic))
            .to all have_received(:call).with(
              [["example_resource_updated", resource_1, {}]],
              key: "ExampleResource:#{resource_1.id}",
              partition_key: resource_1.account_id.to_s
            )
        end
      end

      context "when resource is soft-deletable" do
        context "when resource is soft-deleted" do
          let(:resource_1) { ExamplePublishableCancelableResource.create(account_id: 1, canceled_at: Time.current) }
          let(:model_class) { ExamplePublishableCancelableResource }

          before do
            allow(Dionysus::Producer.responders_for_model_for_topic(model_class,
              topic).first).to receive(:call).and_call_original
          end

          it "publishes destroyed events" do
            perform

            expect(Dionysus::Producer.responders_for_model_for_topic(model_class, topic))
              .to all have_received(:call).with(
                [["example_publishable_cancelable_resource_destroyed", resource_1, {}]],
                key: "ExamplePublishableCancelableResource:#{resource_1.id}",
                partition_key: resource_1.account_id.to_s
              )
          end
        end

        context "when resource is not soft-deleted" do
          let(:resource_1) { ExamplePublishableCancelableResource.create(account_id: 1) }
          let(:model_class) { ExamplePublishableCancelableResource.to_s }

          before do
            allow(Dionysus::Producer.responders_for_model_for_topic(model_class,
              topic).first).to receive(:call).and_call_original
          end

          it "publishes update events" do
            perform

            expect(Dionysus::Producer.responders_for_model_for_topic(model_class, topic))
              .to all have_received(:call).with(
                [["example_publishable_cancelable_resource_updated", resource_1, {}]],
                key: "ExamplePublishableCancelableResource:#{resource_1.id}",
                partition_key: resource_1.account_id.to_s
              )
          end
        end
      end
    end
  end
end
