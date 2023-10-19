# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::DuplicatesFilter do
  describe "#call" do
    subject(:call) { described_class.call(records_to_publish) }

    let(:records_to_publish) do
      [record_1, record_2, record_3, record_4, record_5, record_6, record_7, record_8, record_9]
    end

    let(:record_1) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 1, event_name: "booking_created",
        topic: "v102_bookings")
    end
    let(:record_2) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 1, event_name: "booking_created",
        topic: "v103_bookings")
    end
    let(:record_3) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 2, event_name: "booking_created",
        topic: "v102_bookings")
    end
    let(:record_4) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 2, event_name: "booking_updated",
        topic: "v103_bookings")
    end
    let(:record_5) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 3, event_name: "booking_updated",
        topic: "v103_bookings")
    end
    let(:record_6) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 2, event_name: "booking_updated",
        topic: "v103_bookings")
    end
    let(:record_7) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 2, event_name: "booking_updated",
        topic: "v103_bookings")
    end
    let(:record_8) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 2, event_name: "booking_updated",
        topic: "v103_bookings")
    end
    let(:record_9) do
      DionysusOutbox.new(resource_class: "Booking", resource_id: 3, event_name: "booking_updated",
        topic: "v103_bookings")
    end

    it "removes duplicates in a way that only the consecutive duplicated are removed
    and the last from the duplicates is preserved" do
      expect(call).to eq([record_1, record_2, record_3, record_4, record_5, record_8, record_9])
    end
  end
end
