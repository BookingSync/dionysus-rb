# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus do
  describe ".initialize_application!/.karafka_application" do
    subject(:initialize_application!) { described_class.initialize_application!(**initialization_options) }

    let(:dummy_logger) { double }
    let(:karafka_application) { described_class.karafka_application }
    let(:initialization_options) { base_initialization_options.merge(extra_initialization_options) }
    let(:base_initialization_options) do
      {
        environment: "test",
        seed_brokers: ["kafka://localhost:9092"],
        client_id: "dionysus_test",
        logger: dummy_logger
      }
    end
    let(:extra_initialization_options) do
      {}
    end

    before do
      allow(Karafka::Setup::Config).to receive(:setup_components).and_call_original

      Dionysus::Consumer.declare do
        namespace :v3 do
          topic :rentals
        end

        namespace :v4 do
          topic :rentals, mapper: "mapper", sidekiq: true, worker: "Karafka::BaseWorker"
          topic :bookings do
            dead_letter_queue(topic: "dead_messages", max_retries: 2)
          end
        end
      end

      ENV["KARAFKA_ENV"] = nil
    end

    it "evaluates routing and creates proper consumer group" do
      initialize_application!

      expect(karafka_application.consumer_groups).to be_present
      expect(karafka_application.consumer_groups.last.name).to eq "dionysus_consumer_group_for_dionysus_test"
      expect(karafka_application.consumer_groups.last.topics.count).to eq 3
      expect(karafka_application.consumer_groups.last.topics[0].name).to eq "v3_rentals"
      expect(karafka_application.consumer_groups.last.topics[0].dead_letter_queue.topic).to be_nil
      expect(karafka_application.consumer_groups.last.topics[1].name).to eq "v4_rentals"
      expect(karafka_application.consumer_groups.last.topics[1].dead_letter_queue.topic).to be_nil
      expect(karafka_application.consumer_groups.last.topics[2].name).to eq "v4_bookings"
      expect(karafka_application.consumer_groups.last.topics[2].dead_letter_queue.topic).to eq "dead_messages"
    end

    context "when :consumer_group_prefix is specified" do
      let(:extra_initialization_options) do
        {
          consumer_group_prefix: "prometheus_consumer_group_for"
        }
      end

      it "creates proper consumer group" do
        initialize_application!

        expect(karafka_application.consumer_groups).to be_present
        expect(karafka_application.consumer_groups.last.name).to eq "prometheus_consumer_group_for_dionysus_test"
      end
    end

    describe "without routes evaluation" do
      let(:extra_initialization_options) do
        {
          draw_routing: false
        }
      end

      it "sets config on KarafkaApp" do
        expect do
          initialize_application!
        end.to change { ENV.fetch("KARAFKA_ENV", nil) }.from(nil).to("test")

        expect(karafka_application.config.kafka[:"bootstrap.servers"]).to eq("kafka://localhost:9092")
        expect(karafka_application.config.kafka[:"client.id"]).to eq("dionysus_test")
        expect(karafka_application.config.client_id).to eq "dionysus_test"
        expect(karafka_application.config.logger).to eq dummy_logger
      end

      it "sets KarafkaApp constant" do
        expect do
          KarafkaApp
        end.to raise_error NameError

        initialize_application!

        expect(KarafkaApp).to be_present
        expect(karafka_application).to eq KarafkaApp
      end

      context "when block is given" do
        subject(:initialize_application!) do
          described_class.initialize_application!(**initialization_options) do |config|
            config.kafka[:"session.timeout.ms"] = 1230
          end
        end

        it "allows to customize all other options" do
          initialize_application!

          expect(KarafkaApp.config.kafka[:"session.timeout.ms"]).to eq 1230
        end
      end
    end
  end

  describe ".logger" do
    subject(:logger) { described_class.logger }

    let(:initialize_application!) do
      described_class.initialize_application!(
        environment: "test",
        seed_brokers: ["kafka://localhost:9092"],
        client_id: "dionysus_test",
        logger: dummy_logger
      )
    end
    let(:dummy_logger) { double }

    context "when Karafka Application is initialized" do
      before do
        initialize_application!
      end

      it "returns passed logger in the config" do
        expect(logger).to eq dummy_logger
      end
    end

    context "when Karafka Application is not initialized" do
      it "returns instance of Logger" do
        expect(logger).to be_a(Logger)
      end
    end
  end

  describe ".health_check" do
    subject(:set_health_check) { described_class.health_check = health_check }

    let(:health_check) { Dionysus::Checks::HealthCheck.new }
    let(:healthcheck_storage) do
      FileBasedHealthcheck.new(directory: directory, filename: key, time_threshold: 120)
    end
    let(:directory) { "/tmp" }
    let(:key) { "__karafka_app_running__hostname" }
    let(:publish_app_initialized) { Karafka.monitor.instrument("app.initialized") }
    let(:publish_app_stopped) { Karafka.monitor.instrument("app.stopped") }
    let(:publish_statistics_emitted) { Karafka.monitor.instrument("statistics.emitted") }
    let(:publish_consumer_consumed) { Karafka.monitor.instrument("consumer.consumed") }

    around do |example|
      original_hostname = ENV.fetch("HOSTNAME", nil)
      ENV["HOSTNAME"] = "hostname"

      example.run

      ENV["HOSTNAME"] = original_hostname
    end

    after do
      healthcheck_storage.remove
    end

    it "gets set by a setter" do
      expect do
        set_health_check
      end.to change { described_class.health_check }.from(nil).to(health_check)
    end

    it "subscribes to 'app.initialized' to register a heartbeat" do
      expect do
        set_health_check
        publish_app_initialized
      end.to change { healthcheck_storage.running? }.from(false).to(true)
    end

    it "subscribes to 'statistics.emitted' to register a heartbeat" do
      expect do
        set_health_check
        publish_statistics_emitted
      end.to change { healthcheck_storage.running? }.from(false).to(true)
    end

    it "subscribes to 'consumer.consumed' to register a heartbeat" do
      expect do
        set_health_check
        publish_consumer_consumed
      end.to change { healthcheck_storage.running? }.from(false).to(true)
    end

    it "subscribes to 'app.stopped' to remove the storage for the healthcheck" do
      set_health_check
      publish_app_initialized

      expect do
        publish_app_stopped
      end.to change { healthcheck_storage.running? }.from(true).to(false)
    end
  end

  describe ".inject_routing!/.consumer_registry" do
    subject(:inject_routing!) { described_class.inject_routing!(registry) }

    let(:registry) { double(:registry) }

    it "sets consumer registry" do
      expect do
        inject_routing!
      end.to change { described_class.consumer_registry }.from(nil).to(registry)
    end
  end

  describe ".monitor" do
    subject(:monitor) { described_class.monitor }

    it { is_expected.to be_a(Dionysus::Monitor) }
  end

  describe ".outbox_worker_health_check" do
    subject(:monitor) { described_class.outbox_worker_health_check }

    it { is_expected.to be_a(Dionysus::Producer::Outbox::HealthCheck) }
  end

  describe ".enable_outbox_worker_healthcheck" do
    subject(:enable_outbox_worker_healthcheck) { described_class.enable_outbox_worker_healthcheck }

    before do
      allow(described_class.monitor).to receive(:subscribe).and_call_original
    end

    it "subscribes to outbox worker events" do
      enable_outbox_worker_healthcheck

      expect(described_class.monitor).to have_received(:subscribe).with("outbox_producer.started")
      expect(described_class.monitor).to have_received(:subscribe).with("outbox_producer.stopped")
      expect(described_class.monitor).to have_received(:subscribe).with("outbox_producer.heartbeat")
    end
  end
end
