# frozen_string_literal: true

RSpec.describe Dionysus::Producer::PayloadTimestamp do
  describe ".apply" do
    subject(:apply) { described_class.apply(payload, config) }

    let(:config) do
      Dionysus::Producer::Config.new.tap do |config|
        config.stamp_payload_with_embedded_timestamps = true
      end
    end

    let(:parent_stamp) { Time.utc(2026, 9, 7, 1, 46, 34, 186_604) }
    let(:child_stamp) { Time.utc(2026, 9, 7, 1, 46, 35, 277_948) }

    # reproduces booking 22664722, v3_bookings partition 0, offsets 22344121-22344127
    context "when an embedded child is newer than the parent" do
      let(:payload) do
        [{
          "id" => 22_664_722,
          "updated_at" => parent_stamp,
          "links" => { "client" => 1 },
          "payments" => [
            { "id" => 15_414_080, "updated_at" => Time.utc(2026, 9, 7, 1, 46, 34, 95_500) },
            { "id" => 15_414_081, "updated_at" => child_stamp }
          ]
        }]
      end

      it "reports the age of the payload rather than of the parent row" do
        expect(apply.first["updated_at"]).to eq child_stamp
      end
    end

    context "when the parent is newer than every embedded child" do
      let(:payload) do
        [{ "updated_at" => child_stamp, "payments" => [{ "updated_at" => parent_stamp }] }]
      end

      it "leaves the timestamp alone" do
        expect(apply.first["updated_at"]).to eq child_stamp
      end
    end

    context "when a to-one relationship is embedded" do
      let(:payload) do
        [{ "updated_at" => parent_stamp, "client" => { "updated_at" => child_stamp } }]
      end

      it "counts it too" do
        expect(apply.first["updated_at"]).to eq child_stamp
      end
    end

    context "when nothing is embedded" do
      let(:payload) { [{ "updated_at" => parent_stamp, "links" => { "client" => 1 } }] }

      it "leaves the timestamp alone" do
        expect(apply.first["updated_at"]).to eq parent_stamp
      end
    end

    context "when the payload carries no timestamp" do
      let(:payload) { [{ "id" => 1, "payments" => [{ "updated_at" => child_stamp }] }] }

      it "adds none" do
        expect(apply.first).not_to have_key("updated_at")
      end
    end

    context "when the timestamps are not times" do
      let(:payload) do
        [{ "updated_at" => "2026-09-07", "payments" => [{ "updated_at" => "2026-09-08" }] }]
      end

      it "leaves them alone rather than comparing across types" do
        expect(apply.first["updated_at"]).to eq "2026-09-07"
      end
    end

    context "when the payload is not an array of record hashes" do
      let(:payload) { { "updated_at" => parent_stamp } }

      it "returns it untouched" do
        expect(apply).to eq payload
      end
    end

    context "when the config flag is off" do
      let(:config) { Dionysus::Producer::Config.new }
      let(:payload) do
        [{ "updated_at" => parent_stamp, "payments" => [{ "updated_at" => child_stamp }] }]
      end

      it "leaves the payload untouched" do
        expect(apply.first["updated_at"]).to eq parent_stamp
      end
    end
  end
end
