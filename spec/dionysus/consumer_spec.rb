# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer do
  describe ".configuration/.configure" do
    subject(:configuration) { described_class.configuration }

    before do
      Dionysus::Producer.configure do |config|
        config.registry = Dionysus::Consumer::Registry.new
      end
    end

    it { is_expected.to be_a(Dionysus::Consumer::Config) }
  end

  describe ".registry" do
    subject(:registry) { described_class.registry }

    before do
      described_class.configure do |config|
        config.registry = Dionysus::Consumer::Registry.new
      end
    end

    it { is_expected.to be_a Dionysus::Consumer::Registry }
  end

  describe ".declare" do
    subject(:declare) do
      described_class.declare do
        namespace :v3 do
          deserializer Struct.new(:name).new("deserializer")

          topic :rentals
        end

        namespace :v4 do
          deserializer Struct.new(:name).new("deserializer_v4")

          topic :rentals, mapper: "mapper"
          topic :bookings
        end
      end
    end

    let(:registry) { described_class.registry }

    it "assigns Registry to config" do
      declare

      expect(registry).to be_instance_of(Dionysus::Consumer::Registry)
    end

    it "injects routing" do
      expect do
        declare
      end.to change {
               Dionysus.consumer_registry
             }.from(nil).to(instance_of(Dionysus::Consumer::Registry))
    end

    it "generates consuming routing with producers and stores declaration in Registry" do
      declare

      expect(registry.registrations.keys).to eq %i[v3 v4]

      expect(registry.registrations[:v3].topics.count).to eq 1
      expect(registry.registrations[:v3].deserializer_klass.name).to eq "deserializer"
      expect(registry.registrations[:v3].consumers.count).to eq 1
      expect(registry.registrations[:v3].consumers).to eq [Dionysus::V3RentalConsumer]
      expect(registry.registrations[:v3].topics.first.name).to eq :rentals
      expect(registry.registrations[:v3].topics.first.consumer).to eq Dionysus::V3RentalConsumer
      expect(registry.registrations[:v3].topics.first.deserializer_klass.name).to eq "deserializer"
      expect(registry.registrations[:v3].topics.first.options).to eq({})

      expect(registry.registrations[:v4].topics.count).to eq 2
      expect(registry.registrations[:v4].deserializer_klass.name).to eq "deserializer_v4"
      expect(registry.registrations[:v4].consumers.count).to eq 2
      expect(registry.registrations[:v4].consumers).to eq [Dionysus::V4RentalConsumer,
        Dionysus::V4BookingConsumer]
      expect(registry.registrations[:v4].topics.first.name).to eq :rentals
      expect(registry.registrations[:v4].topics.first.consumer).to eq Dionysus::V4RentalConsumer
      expect(registry.registrations[:v4].topics.first.deserializer_klass.name).to eq "deserializer_v4"
      expect(registry.registrations[:v4].topics.first.options).to eq(mapper: "mapper")
      expect(registry.registrations[:v4].topics.last.name).to eq :bookings
      expect(registry.registrations[:v4].topics.last.consumer).to eq Dionysus::V4BookingConsumer
      expect(registry.registrations[:v4].topics.last.deserializer_klass.name).to eq "deserializer_v4"
      expect(registry.registrations[:v4].topics.last.options).to eq({})
    end
  end

  describe ".reset!" do
    subject(:reset!) { described_class.reset! }

    before do
      described_class.declare do
        namespace :v3 do
          topic :rentals
        end
      end
    end

    it "resets config and undefines constants for Consumers" do
      expect(Dionysus::V3RentalConsumer).to be_present

      expect do
        reset!
      end.to change { described_class.configuration.registry }.to(nil)
        .and change { Dionysus.consumer_registry }.to(nil)

      expect do
        Dionysus::V3RentalConsumer
      end.to raise_error NameError
    end
  end
end
