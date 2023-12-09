# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullHermesEventProducer do
  describe ".publish" do
    subject(:publish) { described_class.publish(event) }

    let(:event) { double(:event) }

    it "does nothing" do
      expect { publish }.not_to raise_error
    end
  end
end
