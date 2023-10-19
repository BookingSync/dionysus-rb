# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::KarafkaDatadogListener do
  describe "error tracking" do
    describe "when _error event" do
      subject(:broadcast) do
        Karafka.monitor.instrument("error.occurred", caller: self, error: error)
      end

      let(:error) { StandardError.new("lofasz") }
      let(:active_span) do
        Class.new do
          def set_error(_error); end
        end.new
      end

      before do
        allow(Datadog::Tracing).to receive(:active_span) { active_span }
        allow(active_span).to receive(:set_error).and_call_original
      end

      it "sends an exception to DataDog" do
        broadcast

        expect(active_span).to have_received(:set_error).with(error).at_least(:once)
      end
    end

    context "when other event" do
      subject(:broadcast) do
        Karafka.monitor.instrument("connection.listener.fetch_loop.lofasz", caller: self, error: error)
      end

      let(:error) { StandardError.new("lofasz") }

      it "blows up with unnown event error" do
        expect do
          broadcast
        end.to raise_error Karafka::Core::Monitoring::Notifications::EventNotRegistered
      end
    end
  end
end
