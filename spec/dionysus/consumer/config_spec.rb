# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::Config do
  describe "#registry/registry=" do
    subject(:registry) { config.registry }

    let(:config) { described_class.new }

    before { config.registry = "registry" }

    it { is_expected.to eq "registry" }
  end

  describe "#transaction_provider/transaction_provider=" do
    subject(:transaction_provider) { config.transaction_provider }

    let(:config) { described_class.new }

    context "when transaction_provider is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullTransactionProvider }
    end

    context "when transaction_provider is specified" do
      before { config.transaction_provider = "transaction_provider" }

      it { is_expected.to eq "transaction_provider" }
    end
  end

  describe "#model_factory/model_factory=" do
    subject(:model_factory) { config.model_factory }

    let(:config) { described_class.new }

    context "when model_factory is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullModelFactory }
    end

    context "when model_factory is specified" do
      before { config.model_factory = "model_factory" }

      it { is_expected.to eq "model_factory" }
    end
  end

  describe "#processing_mutex_provider/processing_mutex_provider=" do
    subject(:processing_mutex_provider) { config.processing_mutex_provider }

    let(:config) { described_class.new }

    context "when processing_mutex_provider is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullMutexProvider }
    end

    context "when processing_mutex_provider is specified" do
      before { config.processing_mutex_provider = "processing_mutex_provider" }

      it { is_expected.to eq "processing_mutex_provider" }
    end
  end

  describe "#processing_mutex_method_name/processing_mutex_method_name=" do
    subject(:processing_mutex_method_name) { config.processing_mutex_method_name }

    let(:config) { described_class.new }

    context "when processing_mutex_method_name is not specified" do
      it { is_expected.to eq :with_lock }
    end

    context "when processing_mutex_method_name is specified" do
      before { config.processing_mutex_method_name = "processing_mutex_method_name" }

      it { is_expected.to eq "processing_mutex_method_name" }
    end
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

  describe "#soft_delete_strategy/soft_delete_strategy=" do
    subject(:soft_delete_strategy) { config.soft_delete_strategy }

    let(:config) { described_class.new }

    context "when soft_delete_strategy is not specified" do
      it { is_expected.to eq :cancel }
    end

    context "when soft_delete_strategy is specified" do
      before { config.soft_delete_strategy = "soft_delete_strategy" }

      it { is_expected.to eq "soft_delete_strategy" }
    end
  end

  describe "#soft_deleted_at_timestamp_attribute/soft_deleted_at_timestamp_attribute=" do
    subject(:soft_deleted_at_timestamp_attribute) { config.soft_deleted_at_timestamp_attribute }

    let(:config) { described_class.new }

    context "when soft_deleted_at_timestamp_attribute is not specified" do
      it { is_expected.to eq :synced_canceled_at }
    end

    context "when soft_deleted_at_timestamp_attribute is specified" do
      before { config.soft_deleted_at_timestamp_attribute = "soft_deleted_at_timestamp_attribute" }

      it { is_expected.to eq "soft_deleted_at_timestamp_attribute" }
    end
  end

  describe "#synced_created_at_timestamp_attribute/synced_created_at_timestamp_attribute=" do
    subject(:synced_created_at_timestamp_attribute) { config.synced_created_at_timestamp_attribute }

    let(:config) { described_class.new }

    context "when synced_created_at_timestamp_attribute is not specified" do
      it { is_expected.to eq :synced_created_at }
    end

    context "when synced_created_at_timestamp_attribute is specified" do
      before { config.synced_created_at_timestamp_attribute = "synced_created_at_timestamp_attribute" }

      it { is_expected.to eq "synced_created_at_timestamp_attribute" }
    end
  end

  describe "#synced_updated_at_timestamp_attribute/synced_updated_at_timestamp_attribute=" do
    subject(:synced_updated_at_timestamp_attribute) { config.synced_updated_at_timestamp_attribute }

    let(:config) { described_class.new }

    context "when synced_updated_at_timestamp_attribute is not specified" do
      it { is_expected.to eq :synced_updated_at }
    end

    context "when synced_updated_at_timestamp_attribute is specified" do
      before { config.synced_updated_at_timestamp_attribute = "synced_updated_at_timestamp_attribute" }

      it { is_expected.to eq "synced_updated_at_timestamp_attribute" }
    end
  end

  describe "#synced_id_attribute/synced_id_attribute=" do
    subject(:synced_id_attribute) { config.synced_id_attribute }

    let(:config) { described_class.new }

    context "when synced_id_attribute is not specified" do
      it { is_expected.to eq :synced_id }
    end

    context "when synced_id_attribute is specified" do
      before { config.synced_id_attribute = "synced_id_attribute" }

      it { is_expected.to eq "synced_id_attribute" }
    end
  end

  describe "#synced_data_attribute/synced_data_attribute=" do
    subject(:synced_data_attribute) { config.synced_data_attribute }

    let(:config) { described_class.new }

    context "when synced_data_attribute is not specified" do
      it { is_expected.to eq :synced_data }
    end

    context "when synced_data_attribute is specified" do
      before { config.synced_data_attribute = "synced_data_attribute" }

      it { is_expected.to eq "synced_data_attribute" }
    end
  end

  describe "#consumer_base_class/consumer_base_class=" do
    subject(:consumer_base_class) { config.consumer_base_class }

    let(:config) { described_class.new }

    context "when consumer_base_class is not specified" do
      it { is_expected.to eq Karafka::BaseConsumer }
    end

    context "when consumer_base_class is specified" do
      before { config.consumer_base_class = "consumer_base_class" }

      it { is_expected.to eq "consumer_base_class" }
    end
  end

  describe "#retry_provider/retry_provider=" do
    subject(:retry_provider) { config.retry_provider }

    let(:config) { described_class.new }

    context "when retry_provider is not specified" do
      it { is_expected.to eq Dionysus::Utils::NullRetryProvider }
    end

    context "when retry_provider is specified" do
      before { config.retry_provider = "retry_provider" }

      it { is_expected.to eq "retry_provider" }
    end
  end

  describe "attributes mapping" do
    subject(:add_attributes_mapping_for_model) do
      config.add_attributes_mapping_for_model(model_name) do
        {
          local_name: :remote_name
        }
      end
    end

    let(:config) { described_class.new }
    let(:model_name) { double(:model_name, to_s: "Rental") }

    it "allows to store attributes mapping and retrieve them" do
      expect do
        add_attributes_mapping_for_model
      end.to change { config.attributes_mapping_for_models }.from({}).to({ "Rental" => { local_name: :remote_name } })
        .and change { config.attributes_mapping_for_model(model_name) }.from({}).to(local_name: :remote_name)
    end
  end

  describe "#resolve_synced_data_hash_proc/resolve_synced_data_hash_proc=" do
    subject(:resolve_synced_data_hash) { config.resolve_synced_data_hash_proc.call(record) }

    let(:record) { RentalForKarafkaConsumerTest.new(synced_data: synced_data) }
    let(:synced_data) do
      {
        "name" => "Villas Thalassa"
      }
    end
    let(:config) { described_class.new }

    context "when resolve_synced_data_hash_proc is not specified" do
      it { is_expected.to eq(synced_data) }
    end

    context "when resolve_synced_data_hash_proc is specified" do
      before do
        config.resolve_synced_data_hash_proc = ->(_record) { { "name" => "other name" } }
      end

      it { is_expected.to eq({ "name" => "other name" }) }
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

  describe "#message_filter/message_filter=" do
    subject(:message_filter) { config.message_filter }

    let(:config) { described_class.new }

    context "when message_filter is not specified" do
      it { is_expected.to be_a Dionysus::Utils::DefaultMessageFilter }
    end

    context "when message_filter is specified" do
      before { config.message_filter = "message_filter" }

      it { is_expected.to eq "message_filter" }
    end
  end
end
