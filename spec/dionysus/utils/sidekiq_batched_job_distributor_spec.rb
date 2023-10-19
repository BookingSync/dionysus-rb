# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::SidekiqBatchedJobDistributor do
  describe "#number_of_batches" do
    subject(:number_of_batches) do
      described_class.new(batch_size: batch_size, units_count: units_count,
        time_range_in_seconds: time_range_in_seconds).number_of_batches
    end

    let(:batch_size) { 1000 }
    let(:units_count) { 123_456 }
    let(:time_range_in_seconds) { double }

    it "returns number of batches based on the units count and batch size" do
      expect(number_of_batches).to eq 124
    end
  end

  describe "#time_per_batch" do
    subject(:time_per_batch) do
      described_class.new(batch_size: batch_size, units_count: units_count,
        time_range_in_seconds: time_range_in_seconds).time_per_batch
    end

    let(:batch_size) { 1000 }
    let(:units_count) { 123_456 }
    let(:time_range_in_seconds) { 10_000 }

    it "returns expected time per batch" do
      expect(time_per_batch).to eq 80
    end
  end

  describe "#enqueue_batch" do
    subject(:enqueue_batch) { distributor.enqueue_batch(DummyJob, "default", batch_number, argument1, argument2) }

    let(:distributor) do
      described_class.new(batch_size: batch_size, units_count: units_count,
        time_range_in_seconds: time_range_in_seconds)
    end
    let(:batch_size) { 1000 }
    let(:units_count) { 123_456 }
    let(:time_range_in_seconds) { 10_000 }
    let(:job_class) do
      Class.new do
        include Sidekiq::Worker

        sidekiq_options queue: :other_queue

        def perform(_whatever, _it_takes); end
      end
    end
    let(:batch_number) { 5 }
    let(:time_per_batch) { 80 }
    let(:argument1) { ["foo"] }
    let(:argument2) { "bar" }

    before do
      stub_const("DummyJob", job_class)
      allow(DummyJob).to receive(:set).with(queue: "default").and_return(DummyJob)
      allow(DummyJob).to receive(:perform_in).with(batch_number * time_per_batch, argument1, argument2)
        .and_call_original
    end

    it "enqueues job to be performed in a time based on the batch number and time per batch" do
      enqueue_batch

      expect(DummyJob).to have_received(:perform_in).with(batch_number * time_per_batch, argument1, argument2)
    end
  end
end
