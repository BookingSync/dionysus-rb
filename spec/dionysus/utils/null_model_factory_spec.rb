# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullModelFactory do
  describe ".for_model" do
    subject(:for_model) { described_class.for_model("Rental") }

    it { is_expected.to be_nil }
  end
end
