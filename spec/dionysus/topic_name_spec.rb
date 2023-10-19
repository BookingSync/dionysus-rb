# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::TopicName do
  describe "#to_s" do
    subject(:to_s) { described_class.new(namespace, name).to_s }

    let(:namespace) { "v3" }
    let(:name) { "rentals" }

    it { is_expected.to eq "v3_rentals" }
  end
end
