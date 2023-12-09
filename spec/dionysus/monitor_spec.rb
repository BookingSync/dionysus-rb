# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Monitor do
  describe "subscribe/publish" do
    let(:monitor) { described_class.new }
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

    context "when the event exists" do
      subject(:publish) { monitor.publish("outbox_producer.started") }

      before do
        monitor.subscribe("outbox_producer.started") do |_event|
          sentinel.call
        end
      end

      it "allows the event to be subscribed to" do
        expect do
          publish
        end.to change { sentinel.called? }.from(false).to(true)
      end
    end

    context "when the event does not exist" do
      subject(:subscribe) { monitor.subscribe(event_name) }

      let(:event_name) { "outbox_producer.event_with_typo" }

      it { is_expected_block.to raise_error(%r{unknown event: outbox_producer.event_with_typo}) }
    end
  end

  describe "#events" do
    subject(:events) { monitor.events }

    let(:monitor) { described_class.new }
    let(:available_events) do
      %w[
        outbox_producer.started
        outbox_producer.stopped
        outbox_producer.shutting_down
        outbox_producer.error
        outbox_producer.publishing_failed
        outbox_producer.published
        outbox_producer.processing_topic
        outbox_producer.processed_topic
        outbox_producer.lock_exists_for_topic
        outbox_producer.heartbeat
      ]
    end

    it { is_expected.to eq available_events }
  end
end
