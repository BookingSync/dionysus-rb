# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Suppressor do
  describe "suppression" do
    it "allows to suppress/unsupress producer" do
      expect do
        described_class.suppress!
      end.to change { described_class.suppressed? }.from(false).to(true)

      expect do
        described_class.unsuppress!
      end.to change { described_class.suppressed? }.from(true).to(false)
    end
  end
end
