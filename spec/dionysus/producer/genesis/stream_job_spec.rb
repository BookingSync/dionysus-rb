# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Genesis::StreamJob do
  it { is_expected.to be_processed_in :dionysus }

  describe "#perform" do
    subject(:perform) { described_class.new.perform(topic, model.to_s, from, to, number_of_days, streamer_job.to_s) }

    let(:streamer_job) { Dionysus::Producer::Genesis::Streamer::StandardJob }
    let(:streamer) { Dionysus::Producer::Genesis::Streamer.new(job_class: streamer_job) }
    let(:topic) { "v3_rentals" }
    let(:model) { ExampleResource }
    let(:from) { 1.day.ago }
    let(:to) { 1.day.from_now }
    let(:number_of_days) { 7 }

    before do
      allow(Dionysus::Producer::Genesis::Streamer).to receive(:new)
        .with(job_class: streamer_job).and_return(streamer)
      allow(streamer).to receive(:stream).and_call_original
    end

    it "calls the streamer job" do
      perform

      expect(streamer).to have_received(:stream).with(topic, model, from, to, number_of_days: number_of_days)
    end
  end
end
