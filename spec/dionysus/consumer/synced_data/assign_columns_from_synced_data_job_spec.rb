# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedDataJob do
  describe ".enqueue" do
    subject(:enqueue) { described_class.enqueue(model_klass, columns, batch_size: batch_size) }

    let(:model_klass) { Syncable }
    let!(:record) { Syncable.create! }
    let(:columns) { %w[name] }
    let(:batch_size) { 100 }

    before do
      allow(described_class).to receive(:set).and_call_original
    end

    it "enqueues batches" do
      expect(described_class).not_to have_enqueued_sidekiq_job(model_klass.to_s, [record.id], columns)

      enqueue

      expect(described_class).to have_enqueued_sidekiq_job(model_klass.to_s, [record.id], columns)
    end

    it "explicitly sets queue" do
      enqueue

      expect(described_class).to have_received(:set).with(queue: :dionysus)
    end
  end

  describe "#perform" do
    subject(:perform) { described_class.new.perform(model_name, ids, columns) }

    let(:model_name) { "Syncable" }
    let(:ids) { [record.id] }
    let(:columns) { %w[name] }
    let!(:record) { Syncable.create!(synced_data: { "name" => "name" }, name: nil) }

    it "calls Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedData#call" do
      expect do
        perform
      end.to change { record.reload.name }.from(nil).to("name")
    end
  end
end
