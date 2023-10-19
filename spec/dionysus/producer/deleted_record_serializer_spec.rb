# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::DeletedRecordSerializer do
  describe "#as_json" do
    subject(:as_json) do
      described_class.new(record, include: [], context_serializer: context_serializer).as_json
    end

    let(:record) { ExampleResource.new(id: 10) }
    let(:context_serializer) do
      Dionysus::Producer::Serializer.new([record], dependencies: [])
    end

    it { is_expected.to eq("id" => 10, "links" => {}) }
  end
end
