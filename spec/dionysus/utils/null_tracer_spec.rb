# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullTracer do
  describe ".trace" do
    subject(:trace) { described_class.trace(event_name, topic) }

    let(:event_name) { "event_name" }
    let(:topic) { "topic" }

    it { is_expected_block.not_to raise_error }
  end
end
