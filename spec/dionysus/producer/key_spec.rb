# frozen_string_literal: true

RSpec.describe Dionysus::Producer::Key do
  describe "#to_key" do
    subject(:to_key) { described_class.new(resource).to_key }

    let(:resource) { ExampleResource.new(id: 1) }

    it { is_expected.to eq "ExampleResource:1" }
  end
end
