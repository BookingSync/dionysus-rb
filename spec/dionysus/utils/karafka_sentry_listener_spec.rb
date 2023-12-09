# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::KarafkaSentryListener do
  context "when _error event" do
    subject(:broadcast) do
      Karafka.monitor.instrument("error.occurred", caller: self, error: error)
    end

    let(:error) { StandardError.new("lofasz") }

    before do
      allow(Sentry).to receive(:capture_exception).and_call_original
    end

    it "sends an exception to Sentry" do
      broadcast

      expect(Sentry).to have_received(:capture_exception).with(error).at_least(:once)
    end
  end

  context "when other event" do
    subject(:broadcast) do
      Karafka.monitor.instrument("connection.listener.fetch_loop.lofasz", caller: self, error: error)
    end

    let(:error) do
      StandardError.new("lofasz")
    end

    it "blows up with unnown event error" do
      expect do
        broadcast
      end.to raise_error Karafka::Core::Monitoring::Notifications::EventNotRegistered
    end
  end
end
