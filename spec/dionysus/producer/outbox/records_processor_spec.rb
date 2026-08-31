# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::RecordsProcessor do
  describe "#call", :freeze_time do
    subject(:call) { records_processor.call(records_to_publish) }

    before do
      Dionysus::Producer.configure do |conf|
        conf.outbox_model = DionysusOutbox
        conf.transaction_provider = ActiveRecord::Base
        conf.default_partition_key = :id
        conf.remove_consecutive_duplicates_before_publishing = true
      end
    end

    describe "with config" do
      let(:records_processor) { described_class.new }
      let!(:outbox_record_1) do
        DionysusOutbox.create!(resource_class: resource_1.class, resource_id: resource_1.id,
          event_name: event_name, created_at: Time.current, topic: "v102_rentals",
          error_class: "StandardError", error_message: "error", attempts: 1, retry_at: 1.minute.ago,
          failed_at: 10.minutes.ago)
      end
      let!(:resource_1) { ExampleResource.create!(account_id: account_id) }
      let!(:outbox_record_2) do
        DionysusOutbox.create!(resource_class: resource_3.class, resource_id: resource_3.id,
          event_name: event_name, created_at: 1.year.ago, topic: "v102_rentals")
      end
      let!(:resource_3) { ExampleResource.create!(account_id: account_id) }
      let(:account_id) { 2 }
      let(:event_name) { "example_resource_created" }
      let(:records_to_publish) { [outbox_record_1, outbox_record_2] }

      before do
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

            topic :other_rentals, partition_key: :account_id do
              publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
            end

            topic :observer_to_one do
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
          end
        end

        allow(Karafka.producer).to receive(:produce_sync).and_call_original
      end

      before do
        # the first stub is odd, but it ensures that the same instance is going to be returned
        # as we don't want to have this attribute cached
        allow(Dionysus::Producer).to receive(:outbox_publisher)
          .and_return(Dionysus::Producer.outbox_publisher)
        allow(Dionysus::Producer.outbox_publisher).to receive(:publish).and_call_original
        allow(Dionysus::Producer.outbox_publisher).to receive(:publish_observers).and_call_original
        DionysusOutbox.where.not(id: [outbox_record_1, outbox_record_2]).destroy_all
      end

      context "when the topic is a standard one" do
        context "when there are records to publish" do
          context "when there are no errors" do
            it "publishes records to Kafka in the right order" do
              call

              expect(Karafka.producer).to have_received(:produce_sync).with(
                payload: {
                  "message" => [
                    {
                      "event" => "example_resource_created",
                      "model_name" => "ExampleResource",
                      "data" => [{ id: resource_3.id }]
                    }
                  ]
                }.to_json,
                key: "ExampleResource:#{resource_3.id}", partition_key: account_id.to_s, topic: "v102_rentals"
              )
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
            end

            it "marks records as published and clears the error attributes for these records if applicable",
              :freeze_time do
              expect do
                call
              end.to change { outbox_record_1.reload.published_at }.from(nil).to(Time.current)
                .and change { outbox_record_2.reload.published_at }.from(nil).to(Time.current)
                .and change { outbox_record_1.error_message }.from("error").to(nil)
                .and change { outbox_record_1.error_class }.from("StandardError").to(nil)
                .and change { outbox_record_1.failed_at }.from(10.minutes.ago).to(nil)
                .and change { outbox_record_1.retry_at }.from(1.minute.ago).to(nil)
            end

            it "returns all the records that were fetched to be published" do
              expect(call).to contain_exactly(outbox_record_2, outbox_record_1)
            end

            it "uses #publish method only" do
              call

              expect(Dionysus::Producer.outbox_publisher).to have_received(:publish).at_least(:once)
              expect(Dionysus::Producer.outbox_publisher).not_to have_received(:publish_observers)
            end

            context "when the block is provided" do
              subject(:call) do
                records_processor.call(records_to_publish) do |record|
                  record.update!(resource_class: "CHANGED")
                end
              end

              it { is_expected_block.to change { outbox_record_1.reload.resource_class }.to("CHANGED") }
              it { is_expected_block.to change { outbox_record_2.reload.resource_class }.to("CHANGED") }
            end

            describe "filtering duplicates" do
              context "when duplicates are filtered" do
                before do
                  allow(Dionysus::Producer::Outbox::DuplicatesFilter).to receive(:call)
                    .and_return([outbox_record_1])
                end

                it "publishes filtered records" do
                  call

                  expect(Karafka.producer).not_to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_created",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: resource_3.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{resource_3.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                  )
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
                end

                it "marks records as published and clears the error attributes for these records if applicable",
                  :freeze_time do
                  expect do
                    call
                  end.to change { outbox_record_1.reload.published_at }.from(nil).to(Time.current)
                    .and change { outbox_record_2.reload.published_at }.from(nil).to(Time.current)
                    .and change { outbox_record_1.error_message }.from("error").to(nil)
                    .and change { outbox_record_1.error_class }.from("StandardError").to(nil)
                    .and change { outbox_record_1.failed_at }.from(10.minutes.ago).to(nil)
                    .and change { outbox_record_1.retry_at }.from(1.minute.ago).to(nil)
                end

                it "returns all the records that were fetched to be published" do
                  expect(call).to contain_exactly(outbox_record_2, outbox_record_1)
                end
              end

              context "when filtering duplicates is disabled" do
                before do
                  allow(Dionysus::Producer.configuration).to receive(
                    :remove_consecutive_duplicates_before_publishing
                  ).and_return(false)
                end

                it "publishes all provided records to Kafka in the right order" do
                  call

                  expect(Karafka.producer).to have_received(:produce_sync).with(
                    payload: {
                      "message" => [
                        {
                          "event" => "example_resource_created",
                          "model_name" => "ExampleResource",
                          "data" => [{ id: resource_3.id }]
                        }
                      ]
                    }.to_json,
                    key: "ExampleResource:#{resource_3.id}", partition_key: account_id.to_s, topic: "v102_rentals"
                  )
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
                end
              end
            end
          end

          context "when there are errors" do
            before do
              allow(Karafka.producer).to receive(:produce_sync).and_call_original
              allow(Karafka.producer).to receive(:produce_sync).with(
                payload: {
                  "message" => [
                    {
                      "event" => "example_resource_created",
                      "model_name" => "ExampleResource",
                      "data" => [{ id: resource_3.id }]
                    }
                  ]
                }.to_json,
                key: "ExampleResource:#{resource_3.id}", partition_key: account_id.to_s, topic: "v102_rentals"
              )
                .and_raise(StandardError.new("something went really wrong"))
            end

            it "publishes records to Kafka in the right order" do
              call

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
            end

            it "marks records as published only if they were indeed published
            and clears the error attributes for these records", :freeze_time do
              expect do
                call
              end.to change { outbox_record_1.reload.published_at }.from(nil).to(Time.current)
                .and avoid_changing { outbox_record_2.reload.published_at }.from(nil)
                .and change { outbox_record_1.error_message }.from("error").to(nil)
                .and change { outbox_record_1.error_class }.from("StandardError").to(nil)
                .and change { outbox_record_1.failed_at }.from(10.minutes.ago).to(nil)
                .and change { outbox_record_1.retry_at }.from(1.minute.ago).to(nil)
            end

            it "returns all the records that were fetched to be published" do
              expect(call).to contain_exactly(outbox_record_2, outbox_record_1)
            end

            it "uses #publish method only" do
              call

              expect(Dionysus::Producer.outbox_publisher).to have_received(:publish).at_least(:once)
              expect(Dionysus::Producer.outbox_publisher).not_to have_received(:publish_observers)
            end

            it "assigns the error attributes" do
              expect do
                call
              end.to change { outbox_record_2.reload.error_class }.to("StandardError")
                .and change { outbox_record_2.error_message }.to("something went really wrong")
                .and change { outbox_record_2.failed_at }.to(Time.current)
                .and change { outbox_record_2.retry_at }.to(10.seconds.from_now)
                .and change { outbox_record_2.attempts }.from(0).to(1)
                .and avoid_changing { outbox_record_1.error_class }
            end

            context "when the block is provided" do
              subject(:call) do
                records_processor.call(records_to_publish) do |record|
                  record.update!(resource_class: "CHANGED")
                end
              end

              it { is_expected_block.to change { outbox_record_1.reload.resource_class }.to("CHANGED") }
              it { is_expected_block.to change { outbox_record_2.reload.resource_class }.to("CHANGED") }
            end
          end
        end

        context "when there are no records to publish" do
          let(:records_to_publish) { [] }

          it "does not publish records to Kafka" do
            call

            expect(Karafka.producer).not_to have_received(:produce_sync)
          end

          it "does not mark records as published" do
            expect do
              call
            end.to avoid_changing { outbox_record_1.reload.published_at }
              .and avoid_changing { outbox_record_2.reload.published_at }
          end
        end
      end

      context "when the topic is for the observers" do
        before do
          Dionysus::Producer.configure do |conf|
            conf.default_partition_key = :account_id
          end
          DionysusOutbox.where.not(id: outbox_record.id).where(topic: topic).delete_all
        end

        let!(:outbox_record) do
          DionysusOutbox.create!(
            resource_class: resource.class,
            resource_id: resource.id,
            event_name: event_name,
            created_at: 1.year.ago,
            topic: topic,
            changeset: { "created_at" => [1.day.ago.as_json, Time.current.as_json] }
          )
        end
        let!(:topic) { Dionysus::Producer::Outbox::Model.observer_topic }
        let!(:observer_resource) { ExampleResource.create!(account_id: account_id) }
        let!(:resource) do
          ExamplePublishableResource.create!(account_id: account_id, example_resource: observer_resource)
        end
        let(:records_to_publish) { [outbox_record] }

        it "publishes records to Kafka in the right order" do
          call

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
          )
        end

        it "marks records as published", :freeze_time do
          expect do
            call
          end.to change { outbox_record.reload.published_at }.from(nil).to(Time.current)
        end

        it "returns the records that have been published" do
          expect(call).to contain_exactly(outbox_record)
        end

        it "uses #publish_observers method only" do
          call

          expect(Dionysus::Producer.outbox_publisher).to have_received(:publish_observers)
            .at_least(:once)
          expect(Dionysus::Producer.outbox_publisher).not_to have_received(:publish)
        end
      end
    end
  end

  describe "#call scheduling a republish for deduplicated records", :freeze_time do
    subject(:call) { described_class.new.call(records_to_publish) }

    # no live resource: the publisher is stubbed, so nothing loads it, and creating one would fire
    # the publishing callbacks another example in this file has already declared topics for
    let(:resource_id) { "1" }
    let(:other_resource_id) { "2" }
    let(:event_name) { "example_resource_updated" }
    let(:topic) { "v102_rentals" }
    let(:delay) { 30 }
    let(:scheduled) { DionysusOutbox.where(published_at: nil).where.not(retry_at: nil) }

    def outbox_record_for(id, on_topic: topic)
      DionysusOutbox.create!(resource_class: "ExampleResource", resource_id: id, event_name:,
        topic: on_topic, partition_key: "2", created_at: Time.current)
    end

    before do
      Dionysus::Producer.configure do |conf|
        conf.outbox_model = DionysusOutbox
        conf.transaction_provider = ActiveRecord::Base
        conf.remove_consecutive_duplicates_before_publishing = true
        conf.republish_deduplicated_records = true
        conf.republish_deduplicated_records_delay = delay
      end
      allow(Dionysus::Producer).to receive(:outbox_publisher)
        .and_return(instance_double(Dionysus::Producer::Outbox::Publisher,
          publish: 1, publish_observers: 1))
      DionysusOutbox.delete_all
    end

    after do
      Dionysus::Producer.configure do |conf|
        conf.republish_deduplicated_records = false
        conf.republish_deduplicated_records_delay = 30
      end
    end

    context "when a run of consecutive duplicates collapsed" do
      let!(:first) { outbox_record_for(resource_id) }
      let!(:second) { outbox_record_for(resource_id) }
      let(:records_to_publish) { [first, second] }

      it "schedules exactly one republish, addressed at the same resource, event and topic" do
        expect { call }.to change { scheduled.count }.from(0).to(1)

        expect(scheduled.first).to have_attributes(resource_class: "ExampleResource", resource_id:,
          event_name:, topic:, partition_key: "2")
      end

      it "schedules it with retry_at rather than a future created_at, so publishing_latency stays honest" do
        call

        expect(scheduled.first).to have_attributes(retry_at: 30.seconds.from_now, created_at: Time.current)
      end

      it "leaves the republish distinguishable from a record awaiting an error retry" do
        call

        expect(scheduled.first).to have_attributes(failed_at: nil, error_class: nil, error_message: nil,
          attempts: 0)
      end

      context "when a delay is configured" do
        let(:delay) { 90 }

        it "honours it" do
          call

          expect(scheduled.first.retry_at).to eq(90.seconds.from_now)
        end
      end

      context "when the feature is off" do
        before { Dionysus::Producer.configure { |c| c.republish_deduplicated_records = false } }

        it { is_expected_block.not_to change { scheduled.count } }
      end

      context "when duplicates are not being removed in the first place" do
        before do
          Dionysus::Producer.configure do |conf|
            conf.remove_consecutive_duplicates_before_publishing = false
          end
        end

        it { is_expected_block.not_to change { scheduled.count } }
      end

      context "when publishing the survivor failed" do
        before do
          allow(Dionysus::Producer.outbox_publisher).to receive(:publish)
            .and_raise(StandardError, "boom")
        end

        it "schedules nothing, because handle_error already retries that record" do
          expect { call }.not_to change { scheduled.where(error_class: nil).count }
        end
      end
    end

    context "when nothing collapsed" do
      let!(:first) { outbox_record_for(resource_id) }
      let!(:second) { outbox_record_for(other_resource_id) }
      let(:records_to_publish) { [first, second] }

      it { is_expected_block.not_to change { scheduled.count } }
    end

    context "when the duplicated records are observers" do
      let!(:first) { outbox_record_for(resource_id, on_topic: "__outbox_observer__") }
      let!(:second) { outbox_record_for(resource_id, on_topic: "__outbox_observer__") }
      let(:records_to_publish) { [first, second] }

      it "skips them, since they carry a changeset and publish through a different path" do
        expect { call }.not_to change { scheduled.count }
      end
    end

    context "when a scheduled republish later comes due on its own" do
      let!(:republish) { outbox_record_for(resource_id) }
      let(:records_to_publish) { [republish] }

      it "is a run of one, so it does not schedule anything further" do
        expect { call }.not_to change { scheduled.count }
      end
    end
  end
end
