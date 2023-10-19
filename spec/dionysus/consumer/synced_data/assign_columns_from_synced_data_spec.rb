# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedData do
  describe "#call" do
    subject(:call) { described_class.new.call(collection, columns) }

    let(:collection) { [record] }
    let(:record) do
      RentalForKarafkaConsumerTest.new(synced_data: synced_data, name: nil, rental_type: "other", account: "account")
    end
    let(:synced_data) do
      { name: "Villas Thalassa", rental_type: "villa" }
    end
    let(:columns) { %w[name rental_type] }

    before do
      allow(record).to receive(:update!).and_call_original
    end

    it "updates collection using update!" do
      expect do
        call
      end.to change { record.name }.from(nil).to("Villas Thalassa")
        .and change { record.rental_type }.from("other").to("villa")
        .and avoid_changing { record.account }

      expect(record).to have_received(:update!)
    end
  end
end
