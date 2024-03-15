# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Integration scenario" do
  let(:rental) { double(:rental, id: 1, name: "Villa Saganaki", model_name: double(name: "Rental"), class: "Rental") }

  let(:karafka_server) { Karafka::Server }
  let(:timeout) { 10 }
  let(:event_bus) do
    Class.new do
      attr_reader :messages

      def initialize
        @messages = []
      end

      def reset
        @messages = []
      end

      def publish(_message, payload)
        @messages << { message: payload }
      end
    end.new
  end
  let(:transaction_provider) { ActiveRecord::Base }
  let(:database_connection_provider) { ActiveRecord::Base }
  let(:outbox_model) { DionysusOutbox }

  before do
    Dionysus::Producer.declare do
      namespace :v102 do
        serializer(Class.new do
          def self.serialize(records, dependencies:)
            records.map { |record| { id: record.id, name: record.name } }
          end
        end)

        topic :rentals do
          publish "Rental"
          publish ExamplePublishableResource
        end
      end
    end

    Dionysus::Consumer.declare do
      namespace :v102 do
        topic :rentals
      end
    end

    Dionysus::Consumer.configure do |config|
      config.transaction_provider = transaction_provider
      config.model_factory = ModelFactoryForKarafkaConsumerTest.new
      config.event_bus = event_bus
    end

    Dionysus::Producer.configure do |config|
      config.transaction_provider = transaction_provider
      config.database_connection_provider = database_connection_provider
      config.outbox_model = outbox_model
    end

    Dionysus.initialize_application!(
      environment: "development",
      seed_brokers: ["localhost:9092"],
      client_id: client_id,
      logger: Logger.new($stdout)
    ) do |config|
      config.kafka[:"statistics.interval.ms"] = 100
      config.kafka[:"heartbeat.interval.ms"] = 1_000
      config.kafka[:"queue.buffering.max.ms"] = 5
      config.pause_timeout = 1
      config.pause_max_timeout = 1
      config.pause_with_exponential_backoff = false
      config.max_wait_time = 500
      config.shutdown_timeout = 30_000
      config.initial_offset = "latest"
    end
    begin
      Karafka::Admin.create_topic("v102_rentals", 1, 1)
    rescue => e
      puts e
    end
  end

  around do |example|
    ensure_real_karafka_producer
    Karafka.producer.config.deliver = true
    Karafka.producer.config.kafka[:"client.id"] = client_id

    example.run

    Karafka.producer.config.deliver = false
    Karafka.producer.config.kafka[:"client.id"] = "waterdrop"
  end

  describe "publishing/consuming a normal message and a tombstone message" do
    subject(:publish) do
      lambda do
        Dionysus::V102RentalResponder.call([["rental_created", [rental]]], key: "Rental:1",
          partition_key: "1")
        Dionysus::V102RentalResponder.call(nil, key: "Rental:1", partition_key: "1")
      end
    end

    let(:client_id) { "publishing_consuming_#{SecureRandom.hex(4)}" }
    let(:transaction_provider) do
      Class.new do
        attr_reader :counter

        def initialize
          @counter = 0
        end

        def transaction
          @counter += 1
          yield
        end

        def reset_counter
          @counter = 0
        end

        def connection_pool
          self
        end

        def with_connection
          yield
        end
      end.new
    end

    it "publishes/consumes the payload that was sent using Dionysus" do
      expect do
        Thread.new do
          counter = 0
          while DBForKarafkaConsumerTest.rentals.empty?
            sleep 1.0
            counter += 1
            puts "increasing counter to #{counter}"
            transaction_provider.reset_counter
            event_bus.reset
            publish.call
            if counter >= timeout
              Karafka::Server.stop
              raise "Timeout for Kafka consuming!"
            end
          end
          Karafka::Server.stop
        end
        puts "running karafka server..."
        Karafka::Server.run
        sleep 1.0
      end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0)
        .and change {
               event_bus.messages.count
             }.from(0).to(2) # only for rental_created, tombstone is not considered here
        .and change { transaction_provider.counter }.from(0).to(2)
    end
  end

  describe "producing from outbox", :freeze_time do
    let(:client_id) { "producing_from_outbox_#{SecureRandom.hex(4)}" }
    let!(:resource) { ExamplePublishableResource.create }
    let(:outbox_record) do
      DionysusOutbox.find_by(resource_class: resource.class.model_name.to_s, resource_id: resource.id)
    end

    let(:start_outbox_worker) do
      Thread.new do
        Karafka.producer.client if Karafka.producer.status.closed?
        Dionysus::Producer.start_outbox_worker(threads_number: 1)
        sleep 1.0
        Process.kill("TERM", 0)
      end
    end

    it "publishes things using outbox worker" do
      expect do
        start_outbox_worker
        sleep 1.0
      end.to change { outbox_record.reload.published_at }.from(nil).to(Time.current)
    end
  end
end
