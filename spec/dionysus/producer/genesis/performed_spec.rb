# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Genesis::Performed, :freeze_time do
  let(:event) { described_class.new(attributes) }
  let(:attributes) do
    {
      model: "Rental",
      service: "bookingsync",
      topic: "v3_rentals",
      start_at: 1.day.ago,
      end_at: Time.current
    }
  end

  describe "initialization" do
    it "raises error when any of the param is missing" do
      expect do
        described_class.new(model: "Rental")
      end.to raise_error(%r{:service is missing})
    end
  end

  describe ".routing_key" do
    subject(:routing_key) { described_class.routing_key }

    it { is_expected.to eq "dionysus.producer.genesis.performed" }
  end

  describe "#as_json" do
    subject(:as_json) { event.as_json }

    let(:attributes) do
      {
        model: "Rental",
        service: "bookingsync",
        topic: "v3_rentals",
        start_at: 1.day.ago,
        end_at: Time.current
      }
    end
    let(:expected_hash) do
      {
        "model" => "Rental",
        "service" => "bookingsync",
        "topic" => "v3_rentals",
        "start_at" => 1.day.ago.as_json,
        "end_at" => Time.current.as_json
      }
    end

    it { is_expected.to eq expected_hash }
  end

  describe "#version" do
    subject(:version) { event.version }

    it { is_expected.to eq 1 }
  end
end
