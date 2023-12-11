# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::DatadogTracer do
  describe "#trace" do
    subject(:trace) { tracer.trace(event_name, topic) { sentinel.call } }

    let(:tracer) { described_class.new }
    let(:event_name) { "event_name" }
    let(:topic) { "v3_rentals" }
    let(:sentinel) do
      Class.new do
        def call
          @called = true
        end

        def called?
          @called == true
        end
      end.new
    end
    let(:message) { double }
    let(:dd_tracer) do
      if Datadog.respond_to?(:tracer)
        Datadog.tracer
      else
        Datadog::Tracing
      end
    end

    before do
      allow(dd_tracer).to receive(:trace)
        .with(event_name, hash_including(service: "dionysus_outbox_worker", span_type: "worker", on_error: anything))
        .and_call_original
    end

    it "uses Datadog tracer" do
      trace

      expect(dd_tracer).to have_received(:trace).with(event_name,
        hash_including(service: "dionysus_outbox_worker", span_type: "worker", on_error: anything))
    end

    it "yields" do
      expect do
        trace
      end.to change { sentinel.called? }.from(false).to(true)
    end
  end
end
