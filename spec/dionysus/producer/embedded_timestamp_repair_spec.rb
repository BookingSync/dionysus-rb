# frozen_string_literal: true

RSpec.describe Dionysus::Producer::EmbeddedTimestampRepair do
  describe ".call" do
    subject(:call) { described_class.call(records, config, &serialize) }

    let(:config) do
      Dionysus::Producer::Config.new.tap do |config|
        config.touch_records_behind_their_embedded_records = true
      end
    end
    let(:earlier_stamp) { Time.utc(2026, 9, 7, 1, 46, 34, 186_604) }
    let(:later_stamp) { Time.utc(2026, 9, 7, 1, 46, 35, 277_948) }
    let(:latest_stamp) { Time.utc(2026, 9, 7, 1, 47, 5, 95_583) }
    let(:read_at) { Time.utc(2026, 9, 7, 1, 46, 40) }
    let(:record) do
      id = ExampleResource.insert_all(
        [{ account_id: 1, created_at: earlier_stamp, updated_at: earlier_stamp }]
      ).first.fetch("id")
      ExampleResource.find(id)
    end
    let(:records) { [record] }
    let(:serializations) { [] }
    # serializing again after the repair is the whole point, so the block reads the record each time
    let(:serialize) do
      child = later_stamp
      counted = serializations
      lambda do
        counted << :serialized
        [records.map { |r| { "id" => r.id, "updated_at" => r.updated_at, "payments" => [{ "updated_at" => child }] } },
          read_at]
      end
    end
    let(:published_updated_at) { call.first.first["updated_at"] }

    # reproduces booking 22664722, v3_bookings partition 0, offsets 22344122-22344127
    context "when an embedded child is newer than the row" do
      it "corrects the row, serializes again and publishes the corrected timestamp" do
        expect(published_updated_at).to eq later_stamp
        expect(ExampleResource.find(record.id).updated_at).to eq later_stamp
        expect(serializations.size).to eq 2
      end
    end

    context "when the row is already ahead of the payload that was read" do
      before { ExampleResource.where(id: record.id).update_all(updated_at: latest_stamp) }

      it "never moves the row backwards and republishes the row's own value" do
        expect(published_updated_at).to eq latest_stamp
        expect(ExampleResource.find(record.id).updated_at).to eq latest_stamp
      end
    end

    context "when the row is newer than every embedded child" do
      let(:serialize) do
        stamp = earlier_stamp
        counted = serializations
        lambda do
          counted << :serialized
          [[{ "id" => record.id, "updated_at" => later_stamp, "payments" => [{ "updated_at" => stamp }] }], read_at]
        end
      end

      it "writes nothing and does not pay for a second serialization" do
        expect(published_updated_at).to eq later_stamp
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
        expect(serializations.size).to eq 1
      end
    end

    context "when the parent's timestamp is an ISO8601 string and the children are times" do
      let(:serialize) do
        child = later_stamp
        counted = serializations
        lambda do
          counted << :serialized
          payload = records.map do |r|
            { "id" => r.id, "updated_at" => r.updated_at.iso8601(6), "payments" => [{ "updated_at" => child }] }
          end
          [payload, read_at]
        end
      end

      it "parses the string, corrects the row and serializes again" do
        expect(published_updated_at).to eq later_stamp.iso8601(6)
        expect(ExampleResource.find(record.id).updated_at).to eq later_stamp
        expect(serializations.size).to eq 2
      end
    end

    context "when the parent's timestamp is a string that is not a full ISO8601 time" do
      let(:serialize) do
        child = later_stamp
        counted = serializations
        lambda do
          counted << :serialized
          [[{ "id" => record.id, "updated_at" => "2026-09-06",
              "payments" => [{ "updated_at" => child }] }], read_at]
        end
      end

      it "fails closed rather than guessing what the value means" do
        # `call` must run first: asserting on state alone never runs the code under test.
        expect(published_updated_at).to eq "2026-09-06"
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
        expect(serializations.size).to eq 1
      end
    end

    context "when a timestamp string carries no UTC offset" do
      let(:serialize) do
        counted = serializations
        lambda do
          counted << :serialized
          [[{ "id" => record.id, "updated_at" => record.updated_at,
              "payments" => [{ "updated_at" => "2026-09-09T01:46:35" }] }], read_at]
        end
      end

      it "refuses it rather than resolving it in whichever zone the host happens to run" do
        expect(published_updated_at).to eq earlier_stamp
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
        expect(serializations.size).to eq 1
      end
    end

    context "when a timestamp string carries a date that does not exist" do
      let(:serialize) do
        counted = serializations
        lambda do
          counted << :serialized
          [[{ "id" => record.id, "updated_at" => record.updated_at,
              "payments" => [{ "updated_at" => "2026-09-31T01:46:35Z" }] }], read_at]
        end
      end

      it "refuses it rather than accepting the October date it normalizes to" do
        expect(published_updated_at).to eq earlier_stamp
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
        expect(serializations.size).to eq 1
      end
    end

    context "when an embedded child's timestamp is not a time" do
      let(:serialize) do
        lambda {
          [[{ "id" => record.id, "updated_at" => record.updated_at, "payments" => [{ "updated_at" => "2026-09-08" }] }],
            read_at]
        }
      end

      it "ignores it rather than comparing a string against a time" do
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
      end
    end

    context "when nothing is embedded" do
      let(:serialize) do
        -> { [[{ "id" => record.id, "updated_at" => record.updated_at, "links" => { "a" => 1 } }], read_at] }
      end

      it "writes nothing" do
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
      end
    end

    context "when the record is not an ActiveRecord object" do
      let(:record) { double(id: 1, updated_at: earlier_stamp) }

      it "leaves it alone" do
        expect(published_updated_at).to eq earlier_stamp
      end
    end

    context "when the payload is not an array" do
      let(:serialize) { -> { [{ "updated_at" => earlier_stamp }, read_at] } }

      it "returns it untouched" do
        expect(call.first).to eq("updated_at" => earlier_stamp)
      end
    end

    context "when a batch carries several records that all need repair" do
      let(:other_record) do
        id = ExampleResource.insert_all(
          [{ account_id: 2, created_at: earlier_stamp, updated_at: earlier_stamp }]
        ).first.fetch("id")
        ExampleResource.find(id)
      end
      let(:records) { [record, other_record] }

      it "repairs every one of them" do
        call

        expect(ExampleResource.find(record.id).updated_at).to eq later_stamp
        expect(ExampleResource.find(other_record.id).updated_at).to eq later_stamp
      end
    end

    context "when the payload does not line up with the records" do
      let(:other_record) do
        id = ExampleResource.insert_all(
          [{ account_id: 2, created_at: earlier_stamp, updated_at: earlier_stamp }]
        ).first.fetch("id")
        ExampleResource.find(id)
      end
      let(:records) { [record, other_record] }
      let(:serialize) do
        child = later_stamp
        lambda do
          [[{ "id" => record.id, "updated_at" => record.updated_at, "payments" => [{ "updated_at" => child }] }],
            read_at]
        end
      end

      it "repairs nothing rather than pairing a record with another record's children" do
        call

        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
        expect(ExampleResource.find(other_record.id).updated_at).to eq earlier_stamp
      end
    end

    context "when the payload belongs to a different record" do
      let(:serialize) do
        child = later_stamp
        lambda do
          [[{ "id" => record.id + 1_000, "updated_at" => record.updated_at,
              "payments" => [{ "updated_at" => child }] }], read_at]
        end
      end

      it "repairs nothing" do
        call

        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
      end
    end

    context "when the config flag is off" do
      let(:config) { Dionysus::Producer::Config.new }

      it "writes nothing and publishes the stale timestamp" do
        expect(published_updated_at).to eq earlier_stamp
        expect(ExampleResource.find(record.id).updated_at).to eq earlier_stamp
      end
    end

    it "carries the read_at of the attempt it returns" do
      expect(call.last).to eq read_at
    end
  end
end
