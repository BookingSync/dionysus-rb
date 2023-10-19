# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullEventBus do
  describe ".publish" do
    subject(:publish) { described_class.publish("name", {}) }

    it "does nothing" do
      expect do
        publish
      end.not_to raise_error
    end
  end
end
