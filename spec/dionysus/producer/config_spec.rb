# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Config do
  describe "#registry/registry=" do
    subject(:registry) { config.registry }

    let(:config) { described_class.new }

    before { config.registry = "registry" }

    it { is_expected.to eq "registry" }
  end

  describe "#instrumenter/instrumenter=" do
    subject(:instrumenter) { config.instrumenter }

    let(:config) { described_class.new }

    context "when instrumenter is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullInstrumenter }
    end

    context "when instrumenter is specified" do
      before { config.instrumenter = "instrumenter" }

      it { is_expected.to eq "instrumenter" }
    end
  end

  describe "#event_bus/event_bus=" do
    subject(:event_bus) { config.event_bus }

    let(:config) { described_class.new }

    context "when event bus is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullEventBus }
    end

    context "when event bus is specified" do
      before { config.event_bus = "event_bus" }

      it { is_expected.to eq "event_bus" }
    end
  end

  describe "#outbox_model/outbox_model=" do
    subject(:outbox_model) { config.outbox_model }

    let(:config) { described_class.new }

    context "when outbox model is not specified" do
      it { is_expected.to be_nil }
    end

    context "when outbox model is specified" do
      before { config.outbox_model = "outbox_model" }

      it { is_expected.to eq "outbox_model" }
    end
  end

  describe "#soft_delete_column/soft_delete_column=" do
    subject(:soft_delete_column) { config.soft_delete_column }

    let(:config) { described_class.new }

    context "when soft_delete_column is not specified" do
      it { is_expected.to eq "canceled_at" }
    end

    context "when soft_delete_column is specified" do
      before { config.soft_delete_column = "soft_delete_column" }

      it { is_expected.to eq "soft_delete_column" }
    end
  end

  describe "#default_partition_key/default_partition_key=" do
    subject(:default_partition_key) { config.default_partition_key }

    let(:config) { described_class.new }

    context "when default_partition_key is not specified" do
      it { is_expected.to eq :account_id }
    end

    context "when soft_delete_column is specified" do
      before { config.default_partition_key = "default_partition_key" }

      it { is_expected.to eq "default_partition_key" }
    end
  end

  describe "#database_connection_provider/database_connection_provider=" do
    subject(:registry) { config.database_connection_provider }

    let(:config) { described_class.new }

    before { config.database_connection_provider = "database_connection_provider" }

    it { is_expected.to eq "database_connection_provider" }
  end

  describe "#outbox_worker_sleep_seconds/outbox_worker_sleep_seconds=" do
    subject(:outbox_worker_sleep_seconds) { config.outbox_worker_sleep_seconds }

    let(:config) { described_class.new }

    context "when outbox_worker_sleep_seconds is not specified" do
      it { is_expected.to eq BigDecimal("0.2") }
    end

    context "when outbox_worker_sleep_seconds is specified" do
      before { config.outbox_worker_sleep_seconds = 5 }

      it { is_expected.to eq BigDecimal("5") }
    end
  end

  describe "#transaction_provider/transaction_provider=" do
    subject(:transaction_provider) { config.transaction_provider }

    let(:config) { described_class.new }

    before { config.transaction_provider = "transaction_provider" }

    it { is_expected.to eq "transaction_provider" }
  end

  describe "#lock_client/lock_client=" do
    subject(:lock_client) { config.lock_client }

    let(:config) { described_class.new }

    context "when lock_client is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullLockClient }
    end

    context "when lock_client is specified" do
      before { config.lock_client = "lock_client" }

      it { is_expected.to eq lock_client }
    end
  end

  describe "#lock_expiry_time/lock_expiry_time=" do
    subject(:lock_expiry_time) { config.lock_expiry_time }

    let(:config) { described_class.new }

    context "when lock_expiry_time is not specified" do
      it { is_expected.to eq 10_000 }
    end

    context "when lock_expiry_time is specified" do
      before { config.lock_expiry_time = 5 }

      it { is_expected.to eq 5 }
    end
  end

  describe "#error_handler/error_handler=" do
    subject(:error_handler) { config.error_handler }

    let(:config) { described_class.new }

    context "when error_handler is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullErrorHandler }
    end

    context "when error_handler is specified" do
      before { config.error_handler = "error_handler" }

      it { is_expected.to eq error_handler }
    end
  end

  describe "#outbox_publishing_batch_size/outbox_publishing_batch_size=" do
    subject(:outbox_publishing_batch_size) { config.outbox_publishing_batch_size }

    let(:config) { described_class.new }

    context "when outbox_publishing_batch_size is not specified" do
      it { is_expected.to eq 100 }
    end

    context "when outbox_publishing_batch_size is specified" do
      before { config.outbox_publishing_batch_size = 5 }

      it { is_expected.to eq 5 }
    end
  end

  describe "#transactional_outbox_enabled/transactional_outbox_enabled=" do
    subject(:transactional_outbox_enabled) { config.transactional_outbox_enabled }

    let(:config) { described_class.new }

    context "when transactional_outbox_enabled is not specified" do
      it { is_expected.to be true }
    end

    context "when transactional_outbox_enabled is disabled" do
      before { config.transactional_outbox_enabled = false }

      it { is_expected.to be false }
    end

    context "when transactional_outbox_enabled is enabled" do
      before { config.transactional_outbox_enabled = true }

      it { is_expected.to be true }
    end
  end

  describe "#sidekiq_queue/sidekiq_queue=" do
    subject(:sidekiq_queue) { config.sidekiq_queue }

    let(:config) { described_class.new }

    context "when sidekiq_queue is not specified" do
      it { is_expected.to eq :dionysus }
    end

    context "when sidekiq_queue is specified" do
      before { config.sidekiq_queue = :messaging }

      it { is_expected.to eq :messaging }
    end
  end

  describe "#publisher_service_name/publisher_service_name=" do
    subject(:publisher_service_name) { config.publisher_service_name }

    let(:config) { described_class.new }

    context "when publisher_service_name is not specified" do
      it { is_expected.to eq "dionysus" }
    end

    context "when publisher_service_name is specified" do
      before { config.publisher_service_name = :publisher }

      it { is_expected.to eq :publisher }
    end
  end

  describe "#genesis_consistency_safety_delay/genesis_consistency_safety_delay=" do
    subject(:genesis_consistency_safety_delay) { config.genesis_consistency_safety_delay }

    let(:config) { described_class.new }

    context "when genesis_consistency_safety_delay is not specified" do
      it { is_expected.to eq 60.seconds }
    end

    context "when genesis_consistency_safety_delay is specified" do
      before { config.genesis_consistency_safety_delay = 10.seconds }

      it { is_expected.to eq 10.seconds }
    end
  end

  describe "#hermes_event_producer/hermes_event_producer=" do
    subject(:hermes_event_producer) { config.hermes_event_producer }

    let(:config) { described_class.new }

    context "when hermes_event_producer is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullHermesEventProducer }
    end

    context "when hermes_event_producer is specified" do
      before { config.hermes_event_producer = :hermes_event_producer }

      it { is_expected.to eq :hermes_event_producer }
    end
  end

  describe "#publish_after_commit/publish_after_commit=" do
    subject(:publish_after_commit) { config.publish_after_commit }

    let(:config) { described_class.new }

    context "when publish_after_commit is not specified" do
      it { is_expected.to be false }
    end

    context "when publish_after_commit is specified" do
      before { config.publish_after_commit = true }

      it { is_expected.to be true }
    end
  end

  describe "#outbox_worker_publishing_delay/outbox_worker_publishing_delay=" do
    subject(:outbox_worker_publishing_delay) { config.outbox_worker_publishing_delay }

    let(:config) { described_class.new }

    context "when outbox_worker_publishing_delay is not specified" do
      it { is_expected.to eq BigDecimal("0").seconds }
    end

    context "when outbox_worker_publishing_delay is specified" do
      before { config.outbox_worker_publishing_delay = 3 }

      it { is_expected.to eq BigDecimal("3").seconds }
    end
  end

  describe "#datadog_statsd_client/datadog_statsd_client=" do
    subject(:datadog_statsd_client) { config.datadog_statsd_client }

    let(:config) { described_class.new }

    context "when datadog_statsd_client is not specified" do
      it { is_expected.to be_nil }
    end

    context "when datadog_statsd_client is specified" do
      before { config.datadog_statsd_client = "datadog_statsd_client" }

      it { is_expected.to eq "datadog_statsd_client" }
    end
  end

  describe "#high_priority_sidekiq_queue/high_priority_sidekiq_queue=" do
    subject(:high_priority_sidekiq_queue) { config.high_priority_sidekiq_queue }

    let(:config) { described_class.new }

    context "when high_priority_sidekiq_queue is not specified" do
      it { is_expected.to eq :dionysus_high_priority }
    end

    context "when high_priority_sidekiq_queue is specified" do
      before { config.high_priority_sidekiq_queue = :critical }

      it { is_expected.to eq :critical }
    end
  end

  describe "#observers_inline_maximum_size/observers_inline_maximum_size=" do
    subject(:observers_inline_maximum_size) { config.observers_inline_maximum_size }

    let(:config) { described_class.new }

    context "when observers_inline_maximum_size is not specified" do
      it { is_expected.to eq 1000 }
    end

    context "when observers_inline_maximum_size is specified" do
      before { config.observers_inline_maximum_size = 100 }

      it { is_expected.to eq 100 }
    end
  end

  describe "#remove_consecutive_duplicates_before_publishing/remove_consecutive_duplicates_before_publishing=" do
    subject(:remove_consecutive_duplicates_before_publishing) { config.remove_consecutive_duplicates_before_publishing }

    let(:config) { described_class.new }

    context "when remove_consecutive_duplicates_before_publishing is not specified" do
      it { is_expected.to be false }
    end

    context "when remove_consecutive_duplicates_before_publishing is disabled" do
      before { config.remove_consecutive_duplicates_before_publishing = false }

      it { is_expected.to be false }
    end

    context "when remove_consecutive_duplicates_before_publishing is enabled" do
      before { config.remove_consecutive_duplicates_before_publishing = true }

      it { is_expected.to be true }
    end
  end
end
