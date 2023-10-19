# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::DatadogLatencyReporter, :freeze_time do
  describe "#report" do
    subject(:report) { described_class.new.report }

    let(:datadog_statsd_client) { Datadog::Statsd.new("localhost", 8125, namespace: "gem") }
    let!(:entry_1) do
      DionysusOutbox.create!(created_at: 2.seconds.ago, published_at: 1.second.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let!(:entry_2) do
      DionysusOutbox.create!(created_at: 5.seconds.ago, published_at: 2.seconds.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let!(:entry_3) do
      DionysusOutbox.create!(created_at: 10.seconds.ago, published_at: nil, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end

    before do
      DionysusOutbox.where.not(id: [entry_1.id, entry_2.id, entry_3.id]).delete_all
      allow(datadog_statsd_client).to receive(:gauge).and_call_original

      Dionysus::Producer.configure do |conf|
        conf.outbox_model = DionysusOutbox
        conf.datadog_statsd_client = datadog_statsd_client
      end
    end

    it "reports latency metrics to datadog" do
      report

      expect(datadog_statsd_client).to have_received(:gauge).with(
        "dionysus.producer.outbox.latency.minimum", 1
      )
      expect(datadog_statsd_client).to have_received(:gauge).with(
        "dionysus.producer.outbox.latency.maximum", 3
      )
      expect(datadog_statsd_client).to have_received(:gauge).with(
        "dionysus.producer.outbox.latency.average", 2
      )
      expect(datadog_statsd_client).to have_received(:gauge).with(
        "dionysus.producer.outbox.latency.highest_since_creation_date", 10
      )
    end
  end
end
