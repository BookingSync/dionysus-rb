# frozen_string_literal: true

RSpec.describe Dionysus::Producer::PayloadTimestamp do
  describe ".apply" do
    subject(:apply) { described_class.apply(payload, config) }

    let(:config) do
      Dionysus::Producer::Config.new.tap do |config|
        config.stamp_payload_with_embedded_timestamps = true
      end
    end
    let(:earliest_stamp) { Time.utc(2026, 9, 7, 1, 46, 34, 95_500) }
    let(:earlier_stamp) { Time.utc(2026, 9, 7, 1, 46, 34, 186_604) }
    let(:later_stamp) { Time.utc(2026, 9, 7, 1, 46, 35, 277_948) }

    # reproduces booking 22664722, v3_bookings partition 0, offsets 22344121-22344127
    context "when an embedded child is newer than the parent" do
      let(:payload) do
        [{
          "id" => 22_664_722,
          "updated_at" => earlier_stamp,
          "links" => { "client" => 1 },
          # earliest_stamp sits first on purpose: it is what pins the max rather than the head
          "payments" => [
            { "id" => 15_414_080, "updated_at" => earliest_stamp },
            { "id" => 15_414_081, "updated_at" => later_stamp }
          ]
        }]
      end

      it "reports the age of the payload and leaves everything else in it alone" do
        expect(apply.first["updated_at"]).to eq later_stamp
        expect(apply.first["payments"].map { |payment| payment["updated_at"] })
          .to eq [earliest_stamp, later_stamp]
        expect(apply.first).to include("id" => 22_664_722, "links" => { "client" => 1 })
      end
    end

    context "when several records share one payload" do
      let(:payload) do
        [
          { "id" => 1, "updated_at" => earlier_stamp, "payments" => [{ "updated_at" => later_stamp }] },
          { "id" => 2, "updated_at" => earliest_stamp, "payments" => [{ "updated_at" => earlier_stamp }] }
        ]
      end

      it "stamps every one of them" do
        expect(apply.map { |record| record["updated_at"] }).to eq [later_stamp, earlier_stamp]
      end
    end

    context "when the parent is newer than every embedded child" do
      let(:payload) do
        [{ "updated_at" => later_stamp, "payments" => [{ "updated_at" => earlier_stamp }] }]
      end

      it "leaves the timestamp alone" do
        expect(apply.first["updated_at"]).to eq later_stamp
      end
    end

    context "when a to-one relationship is embedded" do
      let(:payload) do
        [{ "updated_at" => earlier_stamp, "client" => { "updated_at" => later_stamp } }]
      end

      it "counts it too" do
        expect(apply.first["updated_at"]).to eq later_stamp
      end
    end

    context "when an embedded child's timestamp is not a time" do
      let(:payload) do
        [{ "updated_at" => earlier_stamp, "payments" => [{ "updated_at" => "2026-09-08" }] }]
      end

      it "ignores it rather than comparing a string against a time" do
        expect(apply.first["updated_at"]).to eq earlier_stamp
      end
    end

    context "when the parent's timestamp is not a time" do
      let(:payload) do
        [{ "updated_at" => "2026-09-07", "payments" => [{ "updated_at" => later_stamp }] }]
      end

      it "leaves it alone" do
        expect(apply.first["updated_at"]).to eq "2026-09-07"
      end
    end

    context "when nothing is embedded" do
      let(:payload) { [{ "updated_at" => earlier_stamp, "links" => { "client" => 1 } }] }

      it "leaves the timestamp alone" do
        expect(apply.first["updated_at"]).to eq earlier_stamp
      end
    end

    context "when the payload carries no timestamp" do
      let(:payload) { [{ "id" => 1, "payments" => [{ "updated_at" => later_stamp }] }] }

      it "adds none" do
        expect(apply.first).not_to have_key("updated_at")
      end
    end

    context "when an array element is not a record hash" do
      let(:payload) { [nil, "tombstone", { "updated_at" => earlier_stamp }] }

      it "skips it" do
        expect { apply }.not_to raise_error
      end
    end

    context "when the serializer returned no payload" do
      let(:payload) { nil }

      it "returns it rather than raising" do
        expect(apply).to be_nil
      end
    end

    context "when the payload is not an array of record hashes" do
      let(:payload) { { "updated_at" => earlier_stamp } }

      it "returns it untouched" do
        expect(apply).to eq("updated_at" => earlier_stamp)
        expect(apply).to be payload
      end
    end

    context "when the config flag is off" do
      let(:config) { Dionysus::Producer::Config.new }
      let(:payload) do
        [{ "updated_at" => earlier_stamp, "payments" => [{ "updated_at" => later_stamp }] }]
      end

      it "leaves the payload untouched" do
        expect(apply.first["updated_at"]).to eq earlier_stamp
      end
    end
  end
end
