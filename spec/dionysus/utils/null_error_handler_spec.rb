# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullErrorHandler do
  describe ".capture_exception" do
    subject(:capture_exception) { described_class.capture_exception(error) }

    let(:error) { double(:error) }

    it "does nothing" do
      expect do
        capture_exception
      end.not_to raise_error
    end
  end

  describe ".capture_message" do
    subject(:capture_message) { described_class.capture_message(message) }

    let(:message) { "error message" }

    it "does nothing" do
      expect do
        capture_message
      end.not_to raise_error
    end
  end
end
