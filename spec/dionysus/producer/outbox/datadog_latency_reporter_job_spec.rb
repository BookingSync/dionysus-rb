# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::DatadogLatencyReporterJob, type: :job do
  it { is_expected.to be_processed_in :dionysus_high_priority }

  describe "#perform" do
    subject(:perform) { described_class.new.perform }

    it "calls Dionysus::Producer::Outbox::DatadogLatencyReporter" do
      expect_any_instance_of(Dionysus::Producer::Outbox::DatadogLatencyReporter).to receive(:report)

      perform
    end
  end
end
