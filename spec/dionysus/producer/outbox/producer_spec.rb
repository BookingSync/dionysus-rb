# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::Producer do
  describe "#call", :freeze_time do
    subject(:call) { outbox_producer.call(topic, batch_size: batch_size) }

    let(:outbox_producer) { described_class.new }
    let(:topic) { "v102_rentals" }
    let(:batch_size) { 2 }

    let(:timeout) { 60 }

    before do
      Dionysus::Producer.configure do |conf|
        conf.outbox_model = DionysusOutbox
        conf.transaction_provider = ActiveRecord::Base
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

    context "when there are some non-published records" do
      # only 2 non-published records will be published due to batch size
      # the records are sorted by created at so the one with lowest created_at will be published first
      # the records are scoped to v102_rentals topic
      # if there is an error, only the ones with non-future retry_at will be published
      # that means tha only outbox_record_1 and outbox_record_3 will be published here
      let!(:outbox_record_1) do
        DionysusOutbox.create!(resource_class: resource_1.class, resource_id: resource_1.id,
          event_name: event_name, created_at: Time.current, topic: "v102_rentals",
          error_class: "StandardError", error_message: "error", attempts: 1, retry_at: 1.minute.ago,
          failed_at: 10.minutes.ago)
      end
      let!(:resource_1) { ExampleResource.create!(account_id: account_id) }
      let!(:outbox_record_2) do
        DionysusOutbox.create!(resource_class: resource_2.class, resource_id: resource_2.id,
          event_name: event_name, created_at: 1.year.from_now, topic: "v102_rentals")
      end
      let!(:resource_2) { ExampleResource.create!(account_id: account_id) }
      let!(:outbox_record_3) do
        DionysusOutbox.create!(resource_class: resource_3.class, resource_id: resource_3.id,
          event_name: event_name, created_at: 1.year.ago, topic: "v102_rentals")
      end
      let!(:resource_3) { ExampleResource.create!(account_id: account_id) }
      let!(:outbox_record_4) do
        DionysusOutbox.create!(resource_class: resource_4.class, resource_id: resource_4.id,
          event_name: event_name, created_at: 2.years.ago, topic: "bookings")
      end
      let!(:resource_4) { ExampleResource.create!(account_id: account_id) }
      let!(:outbox_record_5) do
        DionysusOutbox.create!(resource_class: resource_5.class, resource_id: resource_5.id,
          event_name: event_name, created_at: 2.years.ago, topic: "v102_rentals", retry_at: 1.day.from_now)
      end
      let!(:resource_5) { ExampleResource.create!(account_id: account_id) }
      let(:account_id) { 2 }
      let(:event_name) { "example_resource_created" }

      before do
        # the first stub is odd, but it ensures that the same instance is going to be returned
        # as we don't want to have this attribute cached
        allow(Dionysus::Producer).to receive(:outbox_publisher)
          .and_return(Dionysus::Producer.outbox_publisher)
        allow(Dionysus::Producer.outbox_publisher).to receive(:publish).and_call_original
        allow(Dionysus::Producer.outbox_publisher).to receive(:publish_observers).and_call_original
        DionysusOutbox.where.not(id: [outbox_record_1, outbox_record_2, outbox_record_3, outbox_record_4,
          outbox_record_5]).destroy_all
      end

      context "when the topic is a standard one" do
        context "when there are records to publish for a given topic" do
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
                .and change { outbox_record_3.reload.published_at }.from(nil).to(Time.current)
                .and avoid_changing { outbox_record_2.reload.published_at }.from(nil)
                .and avoid_changing { outbox_record_4.reload.published_at }.from(nil)
                .and avoid_changing { outbox_record_5.reload.published_at }.from(nil)
                .and change { outbox_record_1.error_message }.from("error").to(nil)
                .and change { outbox_record_1.error_class }.from("StandardError").to(nil)
                .and change { outbox_record_1.failed_at }.from(10.minutes.ago).to(nil)
                .and change { outbox_record_1.retry_at }.from(1.minute.ago).to(nil)
            end

            it "returns all the records that were fetched to be published" do
              expect(call).to match_array([outbox_record_3, outbox_record_1])
            end

            it "uses #publish method only" do
              call

              expect(Dionysus::Producer.outbox_publisher).to have_received(:publish).at_least(:once)
              expect(Dionysus::Producer.outbox_publisher).not_to have_received(:publish_observers)
            end

            context "when the block is provided" do
              subject(:call) do
                outbox_producer.call(topic, batch_size: batch_size) do |record|
                  record.update!(resource_class: "CHANGED")
                end
              end

              it { is_expected_block.to change { outbox_record_1.reload.resource_class }.to("CHANGED") }
              it { is_expected_block.to change { outbox_record_3.reload.resource_class }.to("CHANGED") }
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
            and clears the error attributes for these records",
              :freeze_time do
              expect do
                call
              end.to change { outbox_record_1.reload.published_at }.from(nil).to(Time.current)
                .and avoid_changing { outbox_record_3.reload.published_at }.from(nil)
                .and avoid_changing { outbox_record_2.reload.published_at }.from(nil)
                .and avoid_changing { outbox_record_4.reload.published_at }.from(nil)
                .and avoid_changing { outbox_record_5.reload.published_at }.from(nil)
                .and change { outbox_record_1.error_message }.from("error").to(nil)
                .and change { outbox_record_1.error_class }.from("StandardError").to(nil)
                .and change { outbox_record_1.failed_at }.from(10.minutes.ago).to(nil)
                .and change { outbox_record_1.retry_at }.from(1.minute.ago).to(nil)
            end

            it "returns all the records that were fetched to be published" do
              expect(call).to match_array([outbox_record_3, outbox_record_1])
            end

            it "uses #publish method only" do
              call

              expect(Dionysus::Producer.outbox_publisher).to have_received(:publish).at_least(:once)
              expect(Dionysus::Producer.outbox_publisher).not_to have_received(:publish_observers)
            end

            it "assigns the error attributes" do
              expect do
                call
              end.to change { outbox_record_3.reload.error_class }.to("StandardError")
                .and change { outbox_record_3.error_message }.to("something went really wrong")
                .and change { outbox_record_3.failed_at }.to(Time.current)
                .and change { outbox_record_3.retry_at }.to(10.seconds.from_now)
                .and change { outbox_record_3.attempts }.from(0).to(1)
                .and avoid_changing { outbox_record_1.error_class }
            end

            context "when the block is provided" do
              subject(:call) do
                outbox_producer.call(topic, batch_size: batch_size) do |record|
                  record.update!(resource_class: "CHANGED")
                end
              end

              it { is_expected_block.to change { outbox_record_1.reload.resource_class }.to("CHANGED") }
              it { is_expected_block.to change { outbox_record_3.reload.resource_class }.to("CHANGED") }
            end
          end
        end

        context "when there are no records to publish for a given topic" do
          let(:topic) { "other" }

          it "does not publish records to Kafka" do
            call

            expect(Karafka.producer).not_to have_received(:produce_sync)
          end

          it "does not mark records as published" do
            expect do
              call
            end.to avoid_changing { outbox_record_1.reload.published_at }
              .and avoid_changing { outbox_record_3.reload.published_at }
              .and avoid_changing { outbox_record_2.reload.published_at }
              .and avoid_changing { outbox_record_4.reload.published_at }
              .and avoid_changing { outbox_record_5.reload.published_at }
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
            .and avoid_changing { outbox_record_3.reload.published_at }.from(nil)
            .and avoid_changing { outbox_record_2.reload.published_at }.from(nil)
            .and avoid_changing { outbox_record_4.reload.published_at }.from(nil)
            .and avoid_changing { outbox_record_5.reload.published_at }.from(nil)
        end

        it "returns the records that have been published" do
          expect(call).to match_array([outbox_record])
        end

        it "uses #publish_observers method only" do
          call

          expect(Dionysus::Producer.outbox_publisher).to have_received(:publish_observers)
            .at_least(:once)
          expect(Dionysus::Producer.outbox_publisher).not_to have_received(:publish)
        end
      end
    end

    context "when there are no records that are already published" do
      let!(:outbox_record_1) do
        DionysusOutbox.create!(resource_class: resource_1.class, resource_id: resource_1.id,
          event_name: event_name, published_at: Time.current, topic: "v102_rentals")
      end
      let!(:resource_1) { ExampleResource.create!(account_id: account_id) }
      let(:account_id) { 2 }
      let(:event_name) { "example_resource_created" }

      before do
        DionysusOutbox.where.not(id: [outbox_record_1]).destroy_all
      end

      it "does not publish anything" do
        call

        expect(Karafka.producer).not_to have_received(:produce_sync)
      end

      it "does not update the timestamps" do
        expect do
          call
        end.not_to change { outbox_record_1.reload.published_at }
      end
    end
  end
end
