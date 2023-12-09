# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::LatencyTracker, :freeze_time do
  describe "#calculate" do
    subject(:calculate) { described_class.new.calculate }

    before do
      DionysusOutbox.delete_all
      Dionysus::Producer.configure do |config|
        config.outbox_model = DionysusOutbox
      end
    end

    context "when records exists for the given interval" do
      let!(:entry_1) do
        DionysusOutbox.create!(created_at: 2.seconds.ago, published_at: 1.second.ago, resource_class: "",
          resource_id: "", event_name: "", topic: "")
      end
      let!(:entry_2) do
        DionysusOutbox.create!(created_at: 5.seconds.ago, published_at: 2.seconds.ago, resource_class: "",
          resource_id: "", event_name: "", topic: "")
      end
      let!(:entry_3) do
        DionysusOutbox.create!(created_at: 120.seconds.ago, published_at: 61.seconds.ago, resource_class: "",
          resource_id: "", event_name: "", topic: "")
      end
      let!(:entry_4) do
        DionysusOutbox.create!(created_at: 121.seconds.ago, published_at: nil, resource_class: "",
          resource_id: "", event_name: "", topic: "")
      end
      let!(:entry_5) do
        DionysusOutbox.create!(created_at: 2.seconds.ago, published_at: nil, resource_class: "",
          resource_id: "", event_name: "", topic: "")
      end

      it "calculates min, max and avg latencies and highest_since_creation_date for the default 1 minute interval" do
        expect(calculate.minimum).to eq 1
        expect(calculate.average).to eq 2
        expect(calculate.maximum).to eq 3
        expect(calculate.highest_since_creation_date).to eq 121
      end
    end

    context "when records do not exist for the given interval" do
      let!(:entry_1) do
        DionysusOutbox.create!(created_at: 120.seconds.ago, published_at: 61.seconds.ago, resource_class: "",
          resource_id: "", event_name: "", topic: "")
      end

      it "calculates min, max and avg latencies and highest_since_creation_date for the default 1 minute interval" do
        expect(calculate.minimum).to eq 0
        expect(calculate.average).to eq 0
        expect(calculate.maximum).to eq 0
        expect(calculate.highest_since_creation_date).to eq 0
      end
    end
  end
end
