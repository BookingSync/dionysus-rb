# frozen_string_literal: true

RSpec.describe Dionysus::Checks::HealthCheck do
  let(:healthcheck_storage) do
    FileBasedHealthcheck.new(directory: directory, filename: key, time_threshold: 120)
  end
  let(:key) { "__karafka_app_running__hostname" }
  let(:directory) { "/tmp" }

  around do |example|
    original_hostname = ENV.fetch("HOSTNAME", nil)
    ENV["HOSTNAME"] = "hostname"

    example.run

    ENV["HOSTNAME"] = original_hostname
  end

  after do
    healthcheck_storage.remove
  end

  describe ".check" do
    subject(:check) { health_check.check }

    let(:health_check) { described_class }

    context "when the heartbeat has not been registered" do
      let(:expected_result) { "[Dionysus healthcheck failed]" }

      it { is_expected.to eq expected_result }
    end

    context "when the heartbeat has been registered" do
      let(:health_check) { described_class }

      before do
        healthcheck_storage.touch
      end

      it { is_expected.to eq "" }
    end
  end

  describe "#app_initialized!" do
    subject(:app_initialized!) { health_check.app_initialized! }

    let(:health_check) { described_class.new }

    it "registers a heartbeat" do
      expect do
        app_initialized!
      end.to change { healthcheck_storage.running? }.from(false).to(true)
    end
  end

  describe "#register_heartbeat" do
    subject(:register_heartbeat) { health_check.register_heartbeat }

    let(:health_check) { described_class.new }

    it "registers a heartbeat" do
      expect do
        register_heartbeat
      end.to change { healthcheck_storage.running? }.from(false).to(true)
    end
  end

  describe "#app_stopped!" do
    subject(:app_stopped!) { health_check.app_stopped! }

    let(:health_check) { described_class.new }

    before do
      health_check.app_initialized!
    end

    it "removes the registry for hearbeats" do
      expect do
        app_stopped!
      end.to change { healthcheck_storage.running? }.from(true).to(false)
    end
  end
end
