# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullRetryProvider do
  describe ".retry" do
    subject(:perform_retry) { described_class.retry { puts "retry" } }

    it "yields block" do
      expect do
        perform_retry
      end.not_to raise_error
    end
  end
end
