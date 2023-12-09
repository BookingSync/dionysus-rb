# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::DefaultMessageFilter do
  let(:filter) { described_class.new(error_handler: error_handler) }
  let(:error_handler) { Dionysus::Utils::NullErrorHandler }

  describe "#ignore_message?" do
    subject(:ignore_message?) { filter.ignore_message?(topic: double, message: double, transformed_data: double) }

    it { is_expected.to be false }
  end

  describe "#notify_about_ignored_message" do
    let(:notify_about_ignored_message) do
      filter.notify_about_ignored_message(topic: topic, message: message, transformed_data: transformed_data)
    end

    let(:topic) { double(:topic) }
    let(:message) { double(:message) }
    let(:transformed_data) { double(:transformed_data) }

    before do
      allow(error_handler).to receive(:capture_message).and_call_original
    end

    it "calls error handler" do
      notify_about_ignored_message

      expect(error_handler).to have_received(:capture_message)
        .with("Ignoring Kafka message. Make sure it's " \
              "processed later (e.g. by directly doing it from console): topic: #{topic}, message: #{message}, " \
              "transformed_data: #{transformed_data}.")
    end
  end
end
