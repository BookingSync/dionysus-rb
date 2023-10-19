# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Genesis::Streamer do
  describe "#stream" do
    subject(:stream) { streamer.stream(topic, model, from, to, options) }

    let(:streamer) { described_class.new(job_class: streamer_job) }
    let(:streamer_job) { Dionysus::Producer::Genesis::Streamer::StandardJob }
    let(:topic) { "v3_rentals" }
    let(:model) { ExamplePublishableCancelableResource }

    before do
      Dionysus::Producer.configure do |config|
        config.transaction_provider = ActiveRecord::Base
        config.outbox_model = DionysusOutbox
      end

      Dionysus::Producer.declare do
        namespace :v3 do
          Struct.new(:name).new("serializer")

          topic :rentals, partition_key: :account_id do
            publish "Rental"
            publish "RentalsTax"
          end
        end
      end

      allow(streamer_job).to receive(:enqueue).and_call_original
    end

    context "when no options and no timeline are specified" do
      let(:from) { nil }
      let(:to) { nil }
      let(:options) do
        {}
      end

      let!(:example_resource_1) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 5.days.ago, canceled_at: 1.day.ago)
      end
      let!(:example_resource_2) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 4.days.ago)
      end
      let!(:example_resource_3) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 1.days.ago)
      end
      let!(:other_resource) { ExampleResource.create(account_id: 1, updated_at: 4.days.ago) }

      it "enqueues streamer_job using default batch size for all records from a given resource" do
        stream

        expect(streamer_job).to have_received(:enqueue).with(
          ExamplePublishableCancelableResource,
          model,
          topic,
          number_of_days: 1,
          batch_size: 100
        )
      end
    end

    context "when options and timeline are specified" do
      let(:from) { 4.days.ago }
      let(:to) { 2.days.ago }
      let(:options) do
        {
          batch_size: 10,
          number_of_days: 7,
          visible_only: true
        }
      end

      let!(:example_resource_1) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 5.days.ago, canceled_at: 1.day.ago)
      end
      let!(:example_resource_2) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 3.days.ago)
      end
      let!(:example_resource_3) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 1.days.ago)
      end
      let!(:example_resource_4) do
        ExamplePublishableCancelableResource.create(account_id: 1, updated_at: 3.days.ago, canceled_at: 1.day.ago)
      end
      let!(:other_resource) { ExampleResource.create(account_id: 1, updated_at: 4.days.ago) }

      it "enqueues streamer_job using default batch size for all records from a given resource" do
        stream

        expect(streamer_job).to have_received(:enqueue).with(
          ExamplePublishableCancelableResource.where(id: [example_resource_2.id]).to_a,
          model,
          topic,
          number_of_days: 7,
          batch_size: 10
        )
      end
    end

    context "when :visible_only is specified for non-soft-deletable model" do
      let(:from) { nil }
      let(:to) { nil }
      let(:options) do
        {
          batch_size: 10,
          number_of_days: 7,
          visible_only: true
        }
      end
      let(:model) { ExampleResource }
      let!(:other_resource) { ExampleResource.create(account_id: 1, updated_at: 4.days.ago) }

      it "enqueues streamer_job using default batch size for all records from a given resource" do
        stream

        expect(streamer_job).to have_received(:enqueue).with(
          ExampleResource,
          model,
          topic,
          number_of_days: 7,
          batch_size: 10
        )
      end
    end
  end
end
