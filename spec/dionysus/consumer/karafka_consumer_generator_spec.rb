# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::KarafkaConsumerGenerator do
  describe "#generate" do
    let(:topic) do
      Dionysus::Consumer::Registry::Registration::Topic.new(:v8, "rentals", nil)
    end
    let(:config) do
      Dionysus::Consumer::Config.new.tap do |current_config|
        model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
        current_config.model_factory = model_factory_for_karafka_consumer_test.new
        current_config.transaction_provider = transaction_provider
        current_config.processing_mutex_provider = processing_mutex_provider
        current_config.processing_mutex_method_name = :lock_with
      end
    end
    let(:transaction_provider) do
      Class.new do
        def transaction
          yield
        end

        def connection_pool
          self
        end

        def with_connection
          yield
        end
      end.new
    end
    let(:processing_mutex_provider) do
      Class.new do
        attr_reader :last_lock_name

        def initialize
          @last_lock_name = nil
        end

        def lock_with(lock_name)
          @last_lock_name = lock_name
          yield
        end
      end.new
    end

    describe "consumer class" do
      subject(:generate) { described_class.new.generate(config, topic) }

      it "generates a consumer class with Karafka::BaseConsumer as a base class" do
        expect do
          Dionysus::V8RentalConsumer
        end.to raise_error NameError

        consumer_klass = generate

        expect(Dionysus::V8RentalConsumer).to be_present
        expect(consumer_klass).to eq Dionysus::V8RentalConsumer
        expect(consumer_klass.superclass).to eq Karafka::BaseConsumer
      end

      context "when consumer class is specified on consumer config level" do
        let(:custom_base_class_for_config) { Class.new(Karafka::BaseConsumer) }

        let(:config) do
          Dionysus::Consumer::Config.new.tap do |current_config|
            model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
            current_config.model_factory = model_factory_for_karafka_consumer_test.new
            current_config.transaction_provider = transaction_provider
            current_config.processing_mutex_provider = processing_mutex_provider
            current_config.processing_mutex_method_name = :lock_with
            current_config.consumer_base_class = custom_base_class_for_config
          end
        end

        it "generates a consumer class with that class as a base class" do
          consumer_klass = generate

          expect(consumer_klass.superclass).to eq custom_base_class_for_config
        end
      end

      context "when consumer class is specified on consumer config level and topic level" do
        let(:topic) do
          Dionysus::Consumer::Registry::Registration::Topic.new(:v8, "rentals", nil,
            consumer_base_class: custom_base_class_for_topic)
        end
        let(:custom_base_class_for_config) { Class.new(Karafka::BaseConsumer) }
        let(:custom_base_class_for_topic) { Class.new(Karafka::BaseConsumer) }

        let(:config) do
          Dionysus::Consumer::Config.new.tap do |current_config|
            model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
            current_config.model_factory = model_factory_for_karafka_consumer_test.new
            current_config.transaction_provider = transaction_provider
            current_config.processing_mutex_provider = processing_mutex_provider
            current_config.processing_mutex_method_name = :lock_with
            current_config.consumer_base_class = custom_base_class_for_config
          end
        end

        it "generates a consumer class with class specified on a topic level as a base class" do
          consumer_klass = generate

          expect(consumer_klass.superclass).to eq custom_base_class_for_topic
        end
      end
    end

    describe "consuming" do
      subject(:consume) { consumer.consume }

      let(:consumer_klass) { described_class.new.generate(config, topic) }
      let(:consumer) { consumer_klass.new }
      let(:params_batch) do
        Karafka::Messages::Messages.new([Karafka::Messages::Message.new(message_payload, metadata)],
          double(:batch_metadata))
      end
      let(:metadata) do
        ::Karafka::Messages::Metadata.new.tap do |metadata|
          metadata["deserializer"] = deserializer
        end
      end
      let(:deserializer) { ->(kafka_params) { kafka_params.raw_payload } }

      before do
        # forgive me for what I'm about to do
        # but figuring out the setup how consumer receives messages
        # and potentially adjusting it with each new version release is just not worth it
        allow(consumer).to receive(:messages) { params_batch }
      end

      describe "complex scenario" do
        let(:message_payload) do
          {
            "message" => [event_1, event_2, event_3, event_4, event_5]
          }
        end
        let(:event_1) do
          {
            "event" => "rental_created",
            "model_name" => "Rental",
            "data" => [
              {
                "links" => {},
                "id" => 1,
                "name" => "Villa Saganaki"
              },
              {
                "links" => {},
                "id" => 2,
                "name" => "Villas Thalassa"
              }
            ]
          }
        end
        let(:event_2) do
          {
            "event" => "booking_updated",
            "model_name" => "Booking",
            "data" => [
              {
                "id" => 3,
                "client_fullname" => "Rich Piana"
              }
            ]
          }
        end
        let(:event_3) do
          {
            "event" => "client_destroyed",
            "model_name" => "Client",
            "data" => [
              {
                "id" => 1,
                "links" => {}
              }
            ]
          }
        end
        let(:event_4) do
          {
            "event" => "rentals_fee_created",
            "model_name" => "RentalsFee",
            "data" => {
              "id" => 1
            }
          }
        end
        let(:event_5) do
          {
            "event" => "rental_completed",
            "model_name" => "Rental",
            "data" => {
              "id" => 1
            }
          }
        end

        before do
          DBForKarafkaConsumerTest.clients << ClientForKarafkaConsumerTest.new(synced_id: 1)
          DBForKarafkaConsumerTest.rentals << RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          DBForKarafkaConsumerTest.bookings << BookingForKarafkaConsumerTest.new(synced_id: 11)
        end

        it "handles complex scenarios with multiple events and each event possibly containing multiple records" do
          expect do
            consume
          end.to change { DBForKarafkaConsumerTest.rentals.count }.from(1).to(2)
            .and change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
            .and avoid_changing { DBForKarafkaConsumerTest.clients.count }

          rentals = DBForKarafkaConsumerTest.rentals
          expect(rentals.first.synced_id).to eq 1
          expect(rentals.first.name).to eq "Villa Saganaki"
          expect(rentals.first.saved).to be true
          expect(rentals.first.destroyed).to be false
          expect(rentals.last.synced_id).to eq 2
          expect(rentals.last.name).to eq "Villas Thalassa"
          expect(rentals.last.destroyed).to be false

          bookings = DBForKarafkaConsumerTest.bookings
          expect(bookings.first.synced_id).to eq 11
          expect(bookings.first.saved).to be false
          expect(bookings.first.destroyed).to be false
          expect(bookings.last.synced_id).to eq 3
          expect(bookings.last.saved).to be true
          expect(bookings.last.destroyed).to be false

          clients = DBForKarafkaConsumerTest.clients
          expect(clients.first.synced_id).to eq 1
          expect(clients.first.saved).to be false
          expect(clients.first.destroyed).to be true
        end
      end

      describe "custom config" do
        context "when modified timestamps" do
          let(:config) do
            Dionysus::Consumer::Config.new.tap do |current_config|
              model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
              current_config.model_factory = model_factory_for_karafka_consumer_test.new
              current_config.transaction_provider = transaction_provider
              current_config.processing_mutex_provider = processing_mutex_provider
              current_config.processing_mutex_method_name = :lock_with
              current_config.synced_created_at_timestamp_attribute = :bookingsync_created_at
              current_config.synced_updated_at_timestamp_attribute = :bookingsync_updated_at
              current_config.soft_deleted_at_timestamp_attribute = :bookingsync_canceled_at
              current_config.synced_data_attribute = :bookingsync_data
            end
          end
          let(:rental) do
            RentalWithModifiedTimestampsForKarafkaConsumerTest.new(synced_id: 1)
          end
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "links" => {},
              "id" => 1,
              "name" => "Villa Saganaki",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:updated_at) { 102 }
          let(:created_at) { 101 }

          before do
            DBForKarafkaConsumerTest.rentals << rental
          end

          it "updates the record's timestamps" do
            expect do
              consume
            end.to change { rental.bookingsync_created_at }.from(nil).to(101)
              .and change { rental.bookingsync_updated_at }.from(nil).to(102)
          end

          it "stores synced data under specified attribute" do
            expect do
              consume
            end.to change { rental.bookingsync_data }.from(nil).to(
              "name" => "Villa Saganaki",
              "synced_created_at" => 101,
              "synced_id" => 1,
              "synced_updated_at" => 102
            )
          end

          context "when cancelation" do
            let(:synced_updated_at) { 10 }
            let(:synced_created_at) { 10 }
            let(:event_1) do
              {
                "event" => "rental_destroyed",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            context "when canceled_at is in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at,
                  "canceled_at" => 123
                }
              end

              context "when model implements soft_deleted_at_timestamp_attribute" do
                let(:rental) do
                  RentalWithModifiedTimestampsForKarafkaConsumerTest.new(synced_id: 1,
                    bookingsync_created_at: synced_created_at, bookingsync_updated_at: synced_updated_at)
                end

                it "cancels the record via assigning timestamp" do
                  expect do
                    consume
                  end.to change { rental.bookingsync_canceled_at }.to(123)
                end
              end

              context "when model does not implement soft_deleted_at_timestamp_attribute" do
                let(:rental) do
                  RentalWithModifiedTimestampsWithoutCanceledAtForKarafkaConsumerTest.new(synced_id: 1,
                    bookingsync_created_at: synced_created_at, bookingsync_updated_at: synced_updated_at)
                end

                it "cancels the record via calling cancel" do
                  expect do
                    consume
                  end.to change { rental.canceled? }.from(false).to(true)
                end
              end
            end
          end
        end

        context "when message filter is supposed to ignore messages" do
          let(:config) do
            Dionysus::Consumer::Config.new.tap do |current_config|
              model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
              current_config.model_factory = model_factory_for_karafka_consumer_test.new
              current_config.transaction_provider = transaction_provider
              current_config.synced_created_at_timestamp_attribute = :bookingsync_created_at
              current_config.synced_updated_at_timestamp_attribute = :bookingsync_updated_at
              current_config.soft_deleted_at_timestamp_attribute = :bookingsync_canceled_at
              current_config.synced_data_attribute = :bookingsync_data
              current_config.message_filter = custom_message_filter
            end
          end
          let(:custom_message_filter) do
            Class.new(Dionysus::Utils::DefaultMessageFilter) do
              attr_reader :topic, :message, :transformed_data

              def ignore_message?(topic:, message:, transformed_data:)
                @topic = topic
                @message = message
                @transformed_data = transformed_data

                transformed_data.first.attributes.fetch("synced_id") == 1
              end
            end.new(error_handler: error_handler)
          end
          let(:error_handler) { Dionysus::Utils::NullErrorHandler }
          let(:rental_1) do
            RentalWithModifiedTimestampsForKarafkaConsumerTest.new(synced_id: 1)
          end
          let(:rental_2) do
            RentalWithModifiedTimestampsForKarafkaConsumerTest.new(synced_id: 2)
          end
          let(:message_payload) do
            {
              "message" => [event_1, event_2]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_1]
            }
          end
          let(:event_2) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_2]
            }
          end
          let(:data_1) do
            {
              "links" => {},
              "id" => 1,
              "name" => "Villa Saganaki",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:data_2) do
            {
              "links" => {},
              "id" => 2,
              "name" => "Villa Thalassa",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:updated_at) { 102 }
          let(:created_at) { 101 }

          before do
            DBForKarafkaConsumerTest.rentals << rental_1
            DBForKarafkaConsumerTest.rentals << rental_2
            allow(error_handler).to receive(:capture_message).and_call_original
          end

          it "does not process the message to be ignored and processes the rest" do
            expect do
              consume
            end.to avoid_changing { rental_1.bookingsync_created_at }
              .and avoid_changing { rental_1.bookingsync_updated_at }
              .and change { rental_2.bookingsync_created_at }
              .and change { rental_2.bookingsync_updated_at }
          end

          it "notifies about ignored message" do
            consume

            expect(error_handler).to have_received(:capture_message).with(%r{Ignoring Kafka message})
          end
        end

        context "when multiple filters are specified" do
          let(:error_handler) { Dionysus::Utils::NullErrorHandler }
          let(:rental_1) do
            RentalWithModifiedTimestampsForKarafkaConsumerTest.new(synced_id: 1)
          end
          let(:rental_2) do
            RentalWithModifiedTimestampsForKarafkaConsumerTest.new(synced_id: 2)
          end
          let(:message_payload) do
            {
              "message" => [event_1, event_2]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_1]
            }
          end
          let(:event_2) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_2]
            }
          end
          let(:data_1) do
            {
              "links" => {},
              "id" => 1,
              "name" => "Villa Saganaki",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:data_2) do
            {
              "links" => {},
              "id" => 2,
              "name" => "Villa Thalassa",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:updated_at) { 102 }
          let(:created_at) { 101 }

          before do
            DBForKarafkaConsumerTest.rentals << rental_1
            DBForKarafkaConsumerTest.rentals << rental_2
            allow(error_handler).to receive(:capture_message).and_call_original
            allow(custom_message_filter_1).to receive(:notify_about_ignored_message).and_call_original
            allow(custom_message_filter_2).to receive(:notify_about_ignored_message).and_call_original
          end

          context "when none of the filters are supposed to ignore the message" do
            let(:config) do
              Dionysus::Consumer::Config.new.tap do |current_config|
                model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
                current_config.model_factory = model_factory_for_karafka_consumer_test.new
                current_config.transaction_provider = transaction_provider
                current_config.synced_created_at_timestamp_attribute = :bookingsync_created_at
                current_config.synced_updated_at_timestamp_attribute = :bookingsync_updated_at
                current_config.soft_deleted_at_timestamp_attribute = :bookingsync_canceled_at
                current_config.synced_data_attribute = :bookingsync_data
                current_config.message_filters = [custom_message_filter_1, custom_message_filter_2]
              end
            end
            let(:custom_message_filter_1) do
              Class.new(Dionysus::Utils::DefaultMessageFilter) do
                attr_reader :topic, :message, :transformed_data

                def ignore_message?(topic:, message:, transformed_data:)
                  @topic = topic
                  @message = message
                  @transformed_data = transformed_data

                  false
                end
              end.new(error_handler: error_handler)
            end
            let(:custom_message_filter_2) do
              Class.new(Dionysus::Utils::DefaultMessageFilter) do
                attr_reader :topic, :message, :transformed_data

                def ignore_message?(topic:, message:, transformed_data:)
                  @topic = topic
                  @message = message
                  @transformed_data = transformed_data

                  false
                end
              end.new(error_handler: error_handler)
            end

            it "does processes the messages" do
              expect do
                consume
              end.to change { rental_1.bookingsync_created_at }
                .and change { rental_1.bookingsync_updated_at }
                .and change { rental_2.bookingsync_created_at }
                .and change { rental_2.bookingsync_updated_at }
            end

            it "does not notify about ignored message" do
              consume

              expect(error_handler).not_to have_received(:capture_message).with(%r{Ignoring Kafka message})
              expect(custom_message_filter_1).not_to have_received(:notify_about_ignored_message)
              expect(custom_message_filter_2).not_to have_received(:notify_about_ignored_message)
            end
          end

          context "when all of the filters are supposed to ignore messages" do
            let(:config) do
              Dionysus::Consumer::Config.new.tap do |current_config|
                model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
                current_config.model_factory = model_factory_for_karafka_consumer_test.new
                current_config.transaction_provider = transaction_provider
                current_config.synced_created_at_timestamp_attribute = :bookingsync_created_at
                current_config.synced_updated_at_timestamp_attribute = :bookingsync_updated_at
                current_config.soft_deleted_at_timestamp_attribute = :bookingsync_canceled_at
                current_config.synced_data_attribute = :bookingsync_data
                current_config.message_filters = [custom_message_filter_1, custom_message_filter_2]
              end
            end
            let(:custom_message_filter_1) do
              Class.new(Dionysus::Utils::DefaultMessageFilter) do
                attr_reader :topic, :message, :transformed_data

                def ignore_message?(topic:, message:, transformed_data:)
                  @topic = topic
                  @message = message
                  @transformed_data = transformed_data

                  transformed_data.first.attributes.fetch("synced_id") == 1
                end
              end.new(error_handler: error_handler)
            end
            let(:custom_message_filter_2) do
              Class.new(Dionysus::Utils::DefaultMessageFilter) do
                attr_reader :topic, :message, :transformed_data

                def ignore_message?(topic:, message:, transformed_data:)
                  @topic = topic
                  @message = message
                  @transformed_data = transformed_data

                  transformed_data.first.attributes.fetch("synced_id") == 1
                end
              end.new(error_handler: error_handler)
            end

            it "does not process the message to be ignored and processes the rest" do
              expect do
                consume
              end.to avoid_changing { rental_1.bookingsync_created_at }
                .and avoid_changing { rental_1.bookingsync_updated_at }
                .and change { rental_2.bookingsync_created_at }
                .and change { rental_2.bookingsync_updated_at }
            end

            it "notifies about ignored message" do
              consume

              expect(error_handler).to have_received(:capture_message).with(%r{Ignoring Kafka message}).once
              expect(custom_message_filter_1).to have_received(:notify_about_ignored_message)
              expect(custom_message_filter_2).not_to have_received(:notify_about_ignored_message)
            end
          end

          context "when only one of the filters is supposed to ignore messages" do
            let(:config) do
              Dionysus::Consumer::Config.new.tap do |current_config|
                model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
                current_config.model_factory = model_factory_for_karafka_consumer_test.new
                current_config.transaction_provider = transaction_provider
                current_config.synced_created_at_timestamp_attribute = :bookingsync_created_at
                current_config.synced_updated_at_timestamp_attribute = :bookingsync_updated_at
                current_config.soft_deleted_at_timestamp_attribute = :bookingsync_canceled_at
                current_config.synced_data_attribute = :bookingsync_data
                current_config.message_filters = [custom_message_filter_1, custom_message_filter_2]
              end
            end
            let(:custom_message_filter_1) do
              Class.new(Dionysus::Utils::DefaultMessageFilter) do
                attr_reader :topic, :message, :transformed_data

                def ignore_message?(topic:, message:, transformed_data:)
                  @topic = topic
                  @message = message
                  @transformed_data = transformed_data

                  transformed_data.first.attributes.fetch("synced_id") == 1
                end
              end.new(error_handler: error_handler)
            end
            let(:custom_message_filter_2) do
              Class.new(Dionysus::Utils::DefaultMessageFilter) do
                attr_reader :topic, :message, :transformed_data

                def ignore_message?(topic:, message:, transformed_data:)
                  @topic = topic
                  @message = message
                  @transformed_data = transformed_data

                  false
                end
              end.new(error_handler: error_handler)
            end

            it "does not process the message to be ignored and processes the rest" do
              expect do
                consume
              end.to avoid_changing { rental_1.bookingsync_created_at }
                .and avoid_changing { rental_1.bookingsync_updated_at }
                .and change { rental_2.bookingsync_created_at }
                .and change { rental_2.bookingsync_updated_at }
            end

            it "notifies about ignored message" do
              consume

              expect(error_handler).to have_received(:capture_message).with(%r{Ignoring Kafka message}).once
              expect(custom_message_filter_1).to have_received(:notify_about_ignored_message)
              expect(custom_message_filter_2).not_to have_received(:notify_about_ignored_message)
            end
          end
        end
      end

      describe "_created event" do
        context "when the record already exists" do
          context "when the local record has synced_updated_at attribute" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name", synced_updated_at: synced_updated_at,
                synced_created_at: synced_created_at)
            end
            let(:synced_updated_at) { 10 }
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:updated_at) { 101 }
            let(:created_at) { 101 }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            context "when updated_at attribute is present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at
                }
              end

              context "when the updated_at from payload is before the synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at - 1 }

                it "does not do anything" do
                  expect do
                    consume
                  end.not_to change { rental.name }
                end
              end

              context "when the updated_at from payload is equal to the synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when the updated_at from payload is greater than synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at + 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.synced_updated_at }.to(updated_at)
                    .and change { rental.saved }.from(false).to(true)
                end
              end
            end

            context "when updated_at attribute is not present in the payload" do
              context "when created_at is present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "created_at" => created_at
                  }
                end

                context "when the created_at from payload is before the synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at - 1 }

                  it "does not do anything" do
                    expect do
                      consume
                    end.not_to change { rental.name }
                  end
                end

                context "when the created_at from payload is equal to the synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when the created_at from payload is greater than synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at + 1 }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end
              end

              context "when created at is not present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki"
                  }
                end

                context "when record's synced_updated_at is present but not synced_created_at" do
                  let(:synced_created_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when record's synced_created_at is present but not synced_updated_at" do
                  let(:synced_updated_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when neither synced_updated_at, nor synced_created_at are present" do
                  let(:synced_updated_at) { nil }
                  let(:synced_created_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end
              end
            end
          end

          context "when the local record does not have synced_updated_at attribute" do
            let(:rental) do
              RentalWithoutSyncedUpdatedAtForKarafkaConsumerTest.new(synced_id: 1, name: "old name",
                synced_created_at: synced_created_at)
            end
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:updated_at) { 101 }
            let(:created_at) { 101 }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            context "when updated_at attribute is present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at
                }
              end

              context "when the updated_at from payload is before the synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at - 1 }

                it "does not do anything" do
                  expect do
                    consume
                  end.not_to change { rental.name }
                end
              end

              context "when the updated_at from payload is equal to the synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.synced_created_at }.to(created_at)
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when the updated_at from payload is greater than synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at + 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end
            end

            context "when updated_at attribute is not present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "created_at" => created_at
                }
              end

              context "when created_at is present in the payload" do
                context "when the created_at from payload is before the synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at - 1 }

                  it "does not do anything" do
                    expect do
                      consume
                    end.not_to change { rental.name }
                  end
                end

                context "when the created_at from payload is equal to the synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when the created_at from payload is greater than synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at + 1 }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.synced_created_at }.to(created_at)
                      .and change { rental.saved }.from(false).to(true)
                  end
                end
              end

              context "when synced_created_at is not present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki"
                  }
                end

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when record's synced_created_at is present" do
                let(:synced_created_at) { 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when record's synced_created_at is not present" do
                let(:synced_created_at) { nil }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end
            end
          end

          describe "potential restoration" do
            context "when the model responds to synced_canceled_at" do
              let(:rental) do
                RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name", synced_canceled_at: 101)
              end
              let(:message_payload) do
                {
                  "message" => [event_1]
                }
              end
              let(:event_1) do
                {
                  "event" => "rental_created",
                  "model_name" => "Rental",
                  "data" => [data]
                }
              end

              before do
                DBForKarafkaConsumerTest.rentals << rental
              end

              context "when the record was canceled by having synced_canceled_at date set" do
                context "when attributes have canceled_at set as not nil" do
                  let(:data) do
                    {
                      "id" => 1,
                      "name" => "Villa Saganaki updated",
                      "canceled_at" => 121
                    }
                  end

                  it "justs syncs the attributes, including canceled_at" do
                    expect do
                      consume
                    end.to change { rental.synced_canceled_at }.from(101).to(121)
                      .and change { rental.name }.to("Villa Saganaki updated")
                  end
                end

                context "when attributes does not have canceled_at set" do
                  let(:data) do
                    {
                      "id" => 1,
                      "name" => "Villa Saganaki updated"
                    }
                  end

                  it "nullifies synced_canceled_at and syncs other attributes" do
                    expect do
                      consume
                    end.to change { rental.synced_canceled_at }.from(101).to(nil)
                      .and change { rental.name }.to("Villa Saganaki updated")
                  end

                  context "when attributes has canceled_at set as nil" do
                    let(:data) do
                      {
                        "id" => 1,
                        "name" => "Villa Saganaki updated",
                        "canceled_at" => nil
                      }
                    end

                    it "nullifies synced_canceled_at and syncs other attributes" do
                      expect do
                        consume
                      end.to change { rental.synced_canceled_at }.from(101).to(nil)
                    end
                  end
                end
              end
            end
          end

          describe "relationships" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:booking) do
              BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
            end
            let(:account) do
              AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental
              DBForKarafkaConsumerTest.bookings << booking
              DBForKarafkaConsumerTest.accounts << account
            end

            context "when the record does not have any associated records" do
              let(:data) do
                {
                  "links" => {
                    "tax" => 201,
                    "bookings" => [101, 102],
                    "account" => 300
                  },
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "non_relationship" => [{ "id" => 1 }],
                  "bookings" => [
                    {
                      "id" => 101,
                      "start_at" => 1
                    },
                    {
                      "id" => 102,
                      "start_at" => 22
                    }
                  ],
                  "tax" => {
                    "id" => 201,
                    "name" => "VAT"
                  },
                  "account" => {
                    "id" => 300,
                    "name" => "#WhateverItTakes"
                  }
                }
              end

              it "creates or updates the relationship records from the payload without blowing up or relationships" do
                expect do
                  consume
                end.to change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
                  .and change { DBForKarafkaConsumerTest.taxes.count }.from(0).to(1)
                  .and change { booking.start_at }.from(2).to(22)
                  .and change { account.name }.from("Discipline Equals Freedom").to("#WhateverItTakes")

                bookings = DBForKarafkaConsumerTest.bookings

                expect(bookings.first.synced_id).to eq 102
                expect(bookings.first.start_at).to eq 22
                expect(bookings.last.synced_id).to eq 101
                expect(bookings.last.start_at).to eq 1

                taxes = DBForKarafkaConsumerTest.taxes
                expect(taxes.first.synced_id).to eq 201
                expect(taxes.first.name).to eq "VAT"
              end

              it "tells model to resolve linking associations" do
                expect do
                  consume
                end.to change { rental.to_many_associations_resolutions }.from({}).to("bookings" => [101, 102])
                  .and change { rental.to_one_associations_resolutions }.from({}).to("tax" => 201, "account" => 300)
              end

              context "when there is a relationship that is not defined on model" do
                let(:data) do
                  {
                    "links" => {
                      "fees" => [121]
                    },
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "fee" => [{
                      "id" => 121
                    }]
                  }
                end

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end
        end

        context "when the record does not exist" do
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "links" => {},
              "id" => 1,
              "name" => "Villa Saganaki",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:updated_at) { 101 }
          let(:created_at) { 111 }

          it "creates the record" do
            expect do
              consume
            end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)

            rentals = DBForKarafkaConsumerTest.rentals
            expect(rentals.first.name).to eq "Villa Saganaki"
            expect(rentals.first.synced_updated_at).to eq 101
            expect(rentals.first.synced_created_at).to eq 111
          end

          describe "relationships" do
            let(:booking) do
              BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
            end
            let(:account) do
              AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.bookings << booking
              DBForKarafkaConsumerTest.accounts << account
            end

            context "when the record does not have any associated records" do
              let(:data) do
                {
                  "links" => {
                    "tax" => 201,
                    "bookings" => [101, 102],
                    "account" => 300
                  },
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "non_relationship" => [{ "id" => 1 }],
                  "bookings" => [
                    {
                      "id" => 101,
                      "start_at" => 1
                    },
                    {
                      "id" => 102,
                      "start_at" => 22
                    }
                  ],
                  "tax" => {
                    "id" => 201,
                    "name" => "VAT"
                  },
                  "account" => {
                    "id" => 300,
                    "name" => "#WhateverItTakes"
                  }
                }
              end
              let(:rental) { DBForKarafkaConsumerTest.rentals.last }

              it "creates or updates the relationship records from the payload without blowing up or relationships" do
                expect do
                  consume
                end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)
                  .and change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
                  .and change { DBForKarafkaConsumerTest.taxes.count }.from(0).to(1)
                  .and change { booking.start_at }.from(2).to(22)
                  .and change { account.name }.from("Discipline Equals Freedom").to("#WhateverItTakes")

                bookings = DBForKarafkaConsumerTest.bookings

                expect(bookings.first.synced_id).to eq 102
                expect(bookings.first.start_at).to eq 22
                expect(bookings.last.synced_id).to eq 101
                expect(bookings.last.start_at).to eq 1

                taxes = DBForKarafkaConsumerTest.taxes
                expect(taxes.first.synced_id).to eq 201
                expect(taxes.first.name).to eq "VAT"
              end

              it "tells model to resolve linking associations" do
                consume

                expect(rental.to_many_associations_resolutions).to eq("bookings" => [101, 102])
                expect(rental.to_one_associations_resolutions).to eq("tax" => 201, "account" => 300)
              end

              context "when there is a relationship that is not defined on model" do
                let(:data) do
                  {
                    "links" => {
                      "fees" => [121]
                    },
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "fee" => [{ "id" => 121 }]
                  }
                end

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end
        end
      end

      describe "_updated event" do
        context "when the record already exists" do
          context "when the local record has synced_updated_at attribute" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name", synced_updated_at: synced_updated_at,
                synced_created_at: synced_created_at)
            end
            let(:synced_updated_at) { 10 }
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_updated",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:updated_at) { 101 }
            let(:created_at) { 101 }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            context "when updated_at attribute is present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at
                }
              end

              context "when the updated_at from payload is before the synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at - 1 }

                it "does not do anything" do
                  expect do
                    consume
                  end.not_to change { rental.name }
                end
              end

              context "when the updated_at from payload is equal to the synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when the updated_at from payload is greater than synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at + 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.synced_updated_at }.to(updated_at)
                    .and change { rental.saved }.from(false).to(true)
                end
              end
            end

            context "when updated_at attribute is not present in the payload" do
              context "when created_at is present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "created_at" => created_at
                  }
                end

                context "when the created_at from payload is before the synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at - 1 }

                  it "does not do anything" do
                    expect do
                      consume
                    end.not_to change { rental.name }
                  end
                end

                context "when the created_at from payload is equal to the synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when the created_at from payload is greater than synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at + 1 }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end
              end

              context "when created at is not present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki"
                  }
                end

                context "when record's synced_updated_at is present but not synced_created_at" do
                  let(:synced_created_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when record's synced_created_at is present but not synced_updated_at" do
                  let(:synced_updated_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when neither synced_updated_at, nor synced_created_at are present" do
                  let(:synced_updated_at) { nil }
                  let(:synced_created_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end
              end
            end
          end

          context "when the local record does not have synced_updated_at attribute" do
            let(:rental) do
              RentalWithoutSyncedUpdatedAtForKarafkaConsumerTest.new(synced_id: 1, name: "old name",
                synced_created_at: synced_created_at)
            end
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_updated",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:updated_at) { 101 }
            let(:created_at) { 101 }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            context "when updated_at attribute is present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at
                }
              end

              context "when the updated_at from payload is before the synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at - 1 }

                it "does not do anything" do
                  expect do
                    consume
                  end.not_to change { rental.name }
                end
              end

              context "when the updated_at from payload is equal to the synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.synced_created_at }.to(created_at)
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when the updated_at from payload is greater than synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at + 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end
            end

            context "when updated_at attribute is not present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "created_at" => created_at
                }
              end

              context "when created_at is present in the payload" do
                context "when the created_at from payload is before the synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at - 1 }

                  it "does not do anything" do
                    expect do
                      consume
                    end.not_to change { rental.name }
                  end
                end

                context "when the created_at from payload is equal to the synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end
                end

                context "when the created_at from payload is greater than synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at + 1 }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.synced_created_at }.to(created_at)
                      .and change { rental.saved }.from(false).to(true)
                  end
                end
              end

              context "when synced_created_at is not present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki"
                  }
                end

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when record's synced_created_at is present" do
                let(:synced_created_at) { 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when record's synced_created_at is not present" do
                let(:synced_created_at) { nil }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end
            end
          end

          describe "potential restoration" do
            context "when the model responds to synced_canceled_at" do
              let(:rental) do
                RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name", synced_canceled_at: 101)
              end
              let(:message_payload) do
                {
                  "message" => [event_1]
                }
              end
              let(:event_1) do
                {
                  "event" => "rental_updated",
                  "model_name" => "Rental",
                  "data" => [data]
                }
              end

              before do
                DBForKarafkaConsumerTest.rentals << rental
              end

              context "when the record was canceled by having synced_canceled_at date set" do
                context "when attributes have canceled_at set" do
                  let(:data) do
                    {
                      "id" => 1,
                      "name" => "Villa Saganaki",
                      "canceled_at" => 121
                    }
                  end

                  it "justs syncs that attribute" do
                    expect do
                      consume
                    end.to change { rental.synced_canceled_at }.from(101).to(121)
                  end
                end

                context "when attributes does not have canceled_at set" do
                  let(:data) do
                    {
                      "id" => 1,
                      "name" => "Villa Saganaki"
                    }
                  end

                  it "nullifies synced_canceled_at" do
                    expect do
                      consume
                    end.to change { rental.synced_canceled_at }.from(101).to(nil)
                  end
                end
              end
            end
          end

          describe "relationships" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:booking) do
              BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
            end
            let(:account) do
              AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_updated",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental
              DBForKarafkaConsumerTest.bookings << booking
              DBForKarafkaConsumerTest.accounts << account
            end

            context "when the record does not have any associated records" do
              let(:data) do
                {
                  "links" => {
                    "tax" => 201,
                    "bookings" => [101, 102],
                    "account" => 300
                  },
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "non_relationship" => [{ "id" => 1 }],
                  "bookings" => [
                    {
                      "id" => 101,
                      "start_at" => 1
                    },
                    {
                      "id" => 102,
                      "start_at" => 22
                    }
                  ],
                  "tax" => {
                    "id" => 201,
                    "name" => "VAT"
                  },
                  "account" => {
                    "id" => 300,
                    "name" => "#WhateverItTakes"
                  }
                }
              end

              it "creates or updates the relationship records from the payload without blowing up or relationships" do
                expect do
                  consume
                end.to change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
                  .and change { DBForKarafkaConsumerTest.taxes.count }.from(0).to(1)
                  .and change { booking.start_at }.from(2).to(22)
                  .and change { account.name }.from("Discipline Equals Freedom").to("#WhateverItTakes")

                bookings = DBForKarafkaConsumerTest.bookings

                expect(bookings.first.synced_id).to eq 102
                expect(bookings.first.start_at).to eq 22
                expect(bookings.last.synced_id).to eq 101
                expect(bookings.last.start_at).to eq 1

                taxes = DBForKarafkaConsumerTest.taxes
                expect(taxes.first.synced_id).to eq 201
                expect(taxes.first.name).to eq "VAT"
              end

              it "tells model to resolve linking associations" do
                expect do
                  consume
                end.to change { rental.to_many_associations_resolutions }.from({}).to("bookings" => [101, 102])
                  .and change { rental.to_one_associations_resolutions }.from({}).to("tax" => 201, "account" => 300)
              end

              context "when there is a relationship that is not defined on model" do
                let(:data) do
                  {
                    "links" => {
                      "fees" => [121]
                    },
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "fee" => [{ "id" => 121 }]
                  }
                end

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end
        end

        context "when the record does not exist" do
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_updated",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "links" => {},
              "id" => 1,
              "name" => "Villa Saganaki",
              "updated_at" => updated_at,
              "created_at" => created_at
            }
          end
          let(:updated_at) { 101 }
          let(:created_at) { 111 }

          it "creates the record" do
            expect do
              consume
            end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)

            rentals = DBForKarafkaConsumerTest.rentals
            expect(rentals.first.name).to eq "Villa Saganaki"
            expect(rentals.first.synced_updated_at).to eq 101
            expect(rentals.first.synced_created_at).to eq 111
          end

          describe "relationships" do
            let(:booking) do
              BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
            end
            let(:account) do
              AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_updated",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.bookings << booking
              DBForKarafkaConsumerTest.accounts << account
            end

            context "when the record does not have any associated records" do
              let(:data) do
                {
                  "links" => {
                    "tax" => 201,
                    "bookings" => [101, 102],
                    "account" => 300
                  },
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "non_relationship" => [{ "id" => 1 }],
                  "bookings" => [
                    {
                      "id" => 101,
                      "start_at" => 1
                    },
                    {
                      "id" => 102,
                      "start_at" => 22
                    }
                  ],
                  "tax" => {
                    "id" => 201,
                    "name" => "VAT"
                  },
                  "account" => {
                    "id" => 300,
                    "name" => "#WhateverItTakes"
                  }
                }
              end
              let(:rental) { DBForKarafkaConsumerTest.rentals.last }

              it "creates or updates the relationship records from the payload without blowing up or relationships" do
                expect do
                  consume
                end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)
                  .and change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
                  .and change { DBForKarafkaConsumerTest.taxes.count }.from(0).to(1)
                  .and change { booking.start_at }.from(2).to(22)
                  .and change { account.name }.from("Discipline Equals Freedom").to("#WhateverItTakes")

                bookings = DBForKarafkaConsumerTest.bookings

                expect(bookings.first.synced_id).to eq 102
                expect(bookings.first.start_at).to eq 22
                expect(bookings.last.synced_id).to eq 101
                expect(bookings.last.start_at).to eq 1

                taxes = DBForKarafkaConsumerTest.taxes
                expect(taxes.first.synced_id).to eq 201
                expect(taxes.first.name).to eq "VAT"
              end

              it "tells model to resolve linking associations" do
                consume

                expect(rental.to_many_associations_resolutions).to eq("bookings" => [101, 102])
                expect(rental.to_one_associations_resolutions).to eq("tax" => 201, "account" => 300)
              end

              context "when there is a relationship that is not defined on model" do
                let(:data) do
                  {
                    "links" => {
                      "fees" => [121]
                    },
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "fee" => [{ "id" => 121 }]
                  }
                end

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end
        end
      end

      describe "_destroyed event" do
        context "when the record already exists" do
          context "when the local record has synced_updated_at attribute" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name", synced_updated_at: synced_updated_at,
                synced_created_at: synced_created_at)
            end
            let(:synced_updated_at) { 10 }
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_destroyed",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:updated_at) { 101 }
            let(:created_at) { 101 }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            context "when updated_at attribute is present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at,
                  "canceled_at" => 123
                }
              end

              context "when the updated_at from payload is before the synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at - 1 }

                it "does not update attributes" do
                  expect do
                    consume
                  end.not_to change { rental.name }
                end

                it "does not cancel the record as the cancelation might be out of order and
                restoration might have come before" do
                  expect do
                    consume
                  end.not_to change { rental.canceled }
                end
              end

              context "when the updated_at from payload is equal to the synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end

                it "cancels the record not by calling :cancel but by assigning synced_canceled_at" do
                  expect do
                    consume
                  end.to change { rental.synced_canceled_at }.to(123)
                end
              end

              context "when the updated_at from payload is greater than synced_updated_at locally stored" do
                let(:updated_at) { rental.synced_updated_at + 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.synced_updated_at }.to(updated_at)
                    .and change { rental.saved }.from(false).to(true)
                end

                it "cancels the record not by calling :cancel but by assigning synced_canceled_at" do
                  expect do
                    consume
                  end.to change { rental.synced_canceled_at }.to(123)
                end
              end
            end

            context "when updated_at attribute is not present in the payload" do
              context "when created_at is present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "created_at" => created_at,
                    "canceled_at" => 123
                  }
                end

                context "when the created_at from payload is before the synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at - 1 }

                  it "does not update attributes" do
                    expect do
                      consume
                    end.not_to change { rental.name }
                  end

                  it "does not cancel the record as the cancelation might be out of order and
                  restoration might have come before" do
                    expect do
                      consume
                    end.not_to change { rental.canceled }
                  end
                end

                context "when the created_at from payload is equal to the synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels the record not by calling :cancel but by assigning synced_canceled_at" do
                    expect do
                      consume
                    end.to change { rental.synced_canceled_at }.to(123)
                  end
                end

                context "when the created_at from payload is greater than synced_updated_at locally stored" do
                  let(:created_at) { rental.synced_updated_at + 1 }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels the record not by calling :cancel but by assigning synced_canceled_at" do
                    expect do
                      consume
                    end.to change { rental.synced_canceled_at }.to(123)
                  end
                end
              end

              context "when created at is not present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki"
                  }
                end

                context "when record's synced_updated_at is present but not synced_created_at" do
                  let(:synced_created_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels the record" do
                    expect do
                      consume
                    end.to change { rental.canceled }.to(true)
                  end
                end

                context "when record's synced_created_at is present but not synced_updated_at" do
                  let(:synced_updated_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels the record" do
                    expect do
                      consume
                    end.to change { rental.canceled }.to(true)
                  end
                end

                context "when neither synced_updated_at, nor synced_created_at are present" do
                  let(:synced_updated_at) { nil }
                  let(:synced_created_at) { nil }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels the record" do
                    expect do
                      consume
                    end.to change { rental.canceled }.to(true)
                  end
                end
              end
            end
          end

          context "when the local record does not have synced_updated_at attribute" do
            let(:rental) do
              RentalWithoutSyncedUpdatedAtForKarafkaConsumerTest.new(synced_id: 1, name: "old name",
                synced_created_at: synced_created_at)
            end
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_destroyed",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:updated_at) { 101 }
            let(:created_at) { 101 }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            context "when updated_at attribute is present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "updated_at" => updated_at,
                  "created_at" => created_at,
                  "canceled_at" => 123
                }
              end

              context "when the updated_at from payload is before the synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at - 1 }

                it "does not update attributes" do
                  expect do
                    consume
                  end.not_to change { rental.name }
                end

                it "does not cancel/destroy the record as the cancelation might be out of order and
                restoration might have come before" do
                  expect do
                    consume
                  end.to avoid_changing { rental.canceled }
                    .and avoid_changing { rental.destroyed }
                end
              end

              context "when the updated_at from payload is equal to the synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.synced_created_at }.to(created_at)
                    .and change { rental.saved }.from(false).to(true)
                end

                it "cancels record" do
                  expect do
                    consume
                  end.to change { rental.canceled }.to(true)
                end
              end

              context "when the updated_at from payload is greater than synced_created_at locally stored" do
                let(:updated_at) { rental.synced_created_at + 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end

                it "cancels record" do
                  expect do
                    consume
                  end.to change { rental.canceled }.to(true)
                end
              end
            end

            context "when updated_at attribute is not present in the payload" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "created_at" => created_at,
                  "canceled_at" => 123
                }
              end

              context "when created_at is present in the payload" do
                context "when the created_at from payload is before the synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at - 1 }

                  it "does not update attributes" do
                    expect do
                      consume
                    end.not_to change { rental.name }
                  end

                  it "does not cancel the record as the cancelation might be out of order and
                  restoration might have come before" do
                    expect do
                      consume
                    end.not_to change { rental.canceled }
                  end
                end

                context "when the created_at from payload is equal to the synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels record" do
                    expect do
                      consume
                    end.to change { rental.canceled }.to(true)
                  end
                end

                context "when the created_at from payload is greater than synced_created_at locally stored" do
                  let(:created_at) { rental.synced_created_at + 1 }

                  it "updates the record" do
                    expect do
                      consume
                    end.to change { rental.name }.from("old name").to("Villa Saganaki")
                      .and change { rental.synced_created_at }.to(created_at)
                      .and change { rental.saved }.from(false).to(true)
                  end

                  it "cancels record" do
                    expect do
                      consume
                    end.to change { rental.canceled }.to(true)
                  end
                end
              end

              context "when synced_created_at is not present in the payload" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki"
                  }
                end

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end
              end

              context "when record's synced_created_at is present" do
                let(:synced_created_at) { 1 }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end

                it "cancels the record" do
                  expect do
                    consume
                  end.to change { rental.canceled }.to(true)
                end
              end

              context "when record's synced_created_at is not present" do
                let(:synced_created_at) { nil }

                it "updates the record" do
                  expect do
                    consume
                  end.to change { rental.name }.from("old name").to("Villa Saganaki")
                    .and change { rental.saved }.from(false).to(true)
                end

                it "cancels the record" do
                  expect do
                    consume
                  end.to change { rental.canceled }.to(true)
                end
              end
            end
          end

          context "when the local record is not soft-deletable and is supposed to be destroyed" do
            let(:fee) do
              FeeForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "fee_destroyed",
                "model_name" => "Fee",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.fees << fee
            end

            context "when even canceled_at is present in the params" do
              let(:data) do
                {
                  "links" => {},
                  "id" => 1,
                  "name" => "Fee Saganaki",
                  "canceled_at" => 101
                }
              end

              it "destroys record but does not save it" do
                expect do
                  consume
                end.to change { fee.destroyed }.from(false).to(true)
                  .and avoid_changing { fee.saved }
              end
            end
          end

          describe "relationships" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:booking) do
              BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
            end
            let(:account) do
              AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_destroyed",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental
              DBForKarafkaConsumerTest.bookings << booking
              DBForKarafkaConsumerTest.accounts << account
            end

            context "when the has some associated records" do
              let(:data) do
                {
                  "links" => {
                    "tax" => 201,
                    "bookings" => [101, 102],
                    "account" => 300
                  },
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "non_relationship" => [{ "id" => 1 }],
                  "bookings" => [
                    {
                      "id" => 101,
                      "start_at" => 1
                    },
                    {
                      "id" => 102,
                      "start_at" => 22
                    }
                  ],
                  "tax" => {
                    "id" => 201,
                    "name" => "VAT"
                  },
                  "account" => {
                    "id" => 300,
                    "name" => "#WhateverItTakes"
                  }
                }
              end

              it "creates or updates the relationship records from the payload without blowing up or relationships,
              it does not cancel these relationships" do
                expect do
                  consume
                end.to change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
                  .and change { DBForKarafkaConsumerTest.taxes.count }.from(0).to(1)
                  .and change { booking.start_at }.from(2).to(22)
                  .and change { account.name }.from("Discipline Equals Freedom").to("#WhateverItTakes")
                  .and change { rental.canceled }.to(true)

                bookings = DBForKarafkaConsumerTest.bookings

                expect(bookings.first.synced_id).to eq 102
                expect(bookings.first.start_at).to eq 22
                expect(bookings.first.destroyed).to be false
                expect(bookings.last.synced_id).to eq 101
                expect(bookings.last.start_at).to eq 1
                expect(bookings.last.destroyed).to be false

                taxes = DBForKarafkaConsumerTest.taxes
                expect(taxes.first.synced_id).to eq 201
                expect(taxes.first.name).to eq "VAT"
                expect(taxes.first.destroyed).to be false
              end

              it "tells model to resolve linking associations" do
                expect do
                  consume
                end.to change { rental.to_many_associations_resolutions }.from({}).to("bookings" => [101, 102])
                  .and change { rental.to_one_associations_resolutions }.from({}).to("tax" => 201, "account" => 300)
              end

              context "when there is a relationship that is not defined on model" do
                let(:data) do
                  {
                    "links" => {
                      "fees" => [121]
                    },
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "fee" => [{ "id" => 121 }]
                  }
                end

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end

          describe "destroying records when there are only IDs in the payload" do
            let(:rental_1) { RentalForKarafkaConsumerTest.new(synced_id: 1) }
            let(:rental_2) { RentalForKarafkaConsumerTest.new(synced_id: 2) }
            let(:synced_updated_at) { 10 }
            let(:synced_created_at) { 10 }
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_destroyed",
                "model_name" => "Rental",
                "data" => data
              }
            end
            let(:data) do
              [
                { "id" => 1 },
                { "id" => 2 }
              ]
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental_1
              DBForKarafkaConsumerTest.rentals << rental_2
            end

            it "destroyed these records" do
              expect do
                consume
              end.to change { rental_1.canceled }.from(false).to(true)
                .and change { rental_2.canceled }.from(false).to(true)
            end
          end
        end

        context "when the record does not exist" do
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:updated_at) { 101 }
          let(:created_at) { 111 }

          describe "canceling - general scenario applicable for other cases, yet, it's easier to test
          the details of scenario here" do
            context "when model is cancelable" do
              let(:event_1) do
                {
                  "event" => "rental_destroyed",
                  "model_name" => "Rental",
                  "data" => [data]
                }
              end

              context "when canceled_at is present in the params" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "updated_at" => updated_at,
                    "created_at" => created_at,
                    "canceled_at" => 101
                  }
                end

                it "creates the record" do
                  expect do
                    consume
                  end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)

                  rentals = DBForKarafkaConsumerTest.rentals
                  expect(rentals.first.name).to eq "Villa Saganaki"
                  expect(rentals.first.synced_canceled_at).to eq 101
                  expect(rentals.first.saved).to be true
                end

                it "neither cancels it explicitly, nor destroys it as the cancelation is performed
                via setting synced_canceled_at" do
                  consume

                  rental = DBForKarafkaConsumerTest.rentals.first
                  expect(rental.canceled).to be false
                  expect(rental.destroyed).to be false
                end
              end

              context "when canceled_at is not present in the params" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "updated_at" => updated_at,
                    "created_at" => created_at
                  }
                end

                it "creates the record" do
                  expect do
                    consume
                  end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)

                  rentals = DBForKarafkaConsumerTest.rentals
                  expect(rentals.first.name).to eq "Villa Saganaki"
                  expect(rentals.first.synced_updated_at).to eq 101
                  expect(rentals.first.synced_created_at).to eq 111
                  expect(rentals.first.saved).to be true
                end

                it "makes this record canceled and not deleted" do
                  consume

                  rental = DBForKarafkaConsumerTest.rentals.first
                  expect(rental.canceled).to be true
                  expect(rental.destroyed).to be false
                end
              end
            end

            context "when model is not cancelable" do
              let(:event_1) do
                {
                  "event" => "fee_destroyed",
                  "model_name" => "Fee",
                  "data" => [data]
                }
              end

              context "when canceled_at is present in the params" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Fee Saganaki",
                    "updated_at" => updated_at,
                    "created_at" => created_at,
                    "canceled_at" => 101
                  }
                end

                it "does not create the record in DB by saving it" do
                  expect do
                    consume
                  end.to change { DBForKarafkaConsumerTest.fees.count }.from(0).to(1)

                  fees = DBForKarafkaConsumerTest.fees
                  expect(fees.first.name).to eq "Fee Saganaki"
                  expect(fees.first.saved).to be false
                end

                it "destroys record" do
                  consume

                  fee = DBForKarafkaConsumerTest.fees.first
                  expect(fee.destroyed).to be true
                end
              end

              context "when canceled_at is not present in the params" do
                let(:data) do
                  {
                    "links" => {},
                    "id" => 1,
                    "name" => "Fee Saganaki",
                    "updated_at" => updated_at,
                    "created_at" => created_at
                  }
                end

                it "does not create the record in DB by saving it" do
                  expect do
                    consume
                  end.to change { DBForKarafkaConsumerTest.fees.count }.from(0).to(1)

                  fees = DBForKarafkaConsumerTest.fees
                  expect(fees.first.name).to eq "Fee Saganaki"
                  expect(fees.first.saved).to be false
                end

                it "destroys record" do
                  consume

                  fee = DBForKarafkaConsumerTest.fees.first
                  expect(fee.destroyed).to be true
                end
              end
            end
          end

          describe "relationships" do
            let(:booking) do
              BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
            end
            let(:account) do
              AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_destroyed",
                "model_name" => "Rental",
                "data" => [data]
              }
            end

            before do
              DBForKarafkaConsumerTest.bookings << booking
              DBForKarafkaConsumerTest.accounts << account
            end

            context "when the record does not have any associated records" do
              let(:data) do
                {
                  "links" => {
                    "tax" => 201,
                    "bookings" => [101, 102],
                    "account" => 300
                  },
                  "id" => 1,
                  "name" => "Villa Saganaki",
                  "non_relationship" => [{ "id" => 1 }],
                  "bookings" => [
                    {
                      "id" => 101,
                      "start_at" => 1
                    },
                    {
                      "id" => 102,
                      "start_at" => 22
                    }
                  ],
                  "tax" => {
                    "id" => 201,
                    "name" => "VAT"
                  },
                  "account" => {
                    "id" => 300,
                    "name" => "#WhateverItTakes"
                  }
                }
              end
              let(:rental) { DBForKarafkaConsumerTest.rentals.last }

              it "creates or updates the relationship records from the payload without blowing up or relationships" do
                expect do
                  consume
                end.to change { DBForKarafkaConsumerTest.rentals.count }.from(0).to(1)
                  .and change { DBForKarafkaConsumerTest.bookings.count }.from(1).to(2)
                  .and change { DBForKarafkaConsumerTest.taxes.count }.from(0).to(1)
                  .and change { booking.start_at }.from(2).to(22)
                  .and change { account.name }.from("Discipline Equals Freedom").to("#WhateverItTakes")

                bookings = DBForKarafkaConsumerTest.bookings

                expect(bookings.first.synced_id).to eq 102
                expect(bookings.first.start_at).to eq 22
                expect(bookings.last.synced_id).to eq 101
                expect(bookings.last.start_at).to eq 1

                taxes = DBForKarafkaConsumerTest.taxes
                expect(taxes.first.synced_id).to eq 201
                expect(taxes.first.name).to eq "VAT"
              end

              it "tells model to resolve linking associations" do
                consume

                expect(rental.to_many_associations_resolutions).to eq("bookings" => [101, 102])
                expect(rental.to_one_associations_resolutions).to eq("tax" => 201, "account" => 300)
              end

              context "when there is a relationship that is not defined on model" do
                let(:data) do
                  {
                    "links" => {
                      "fees" => [121]
                    },
                    "id" => 1,
                    "name" => "Villa Saganaki",
                    "fee" => [{ "id" => 121 }]
                  }
                end

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end
        end
      end

      describe "unknown event" do
        let(:message_payload) do
          {
            "message" => [event_1]
          }
        end
        let(:event_1) do
          {
            "event" => "rental_completed",
            "model_name" => "Rental",
            "data" => [
              {
                "id" => 2
              }
            ]
          }
        end

        it "does not blow up" do
          expect do
            consume
          end.not_to raise_error
        end
      end

      describe "synced_id being blank" do
        let(:message_payload) do
          {
            "message" => [event_1]
          }
        end
        let(:event_1) do
          {
            "event" => "rental_created",
            "model_name" => "Rental",
            "data" => [
              {}
            ]
          }
        end
        let(:logger) do
          Logger.new($stdout)
        end

        before do
          allow(Dionysus).to receive(:logger).and_return(logger)
          allow(logger).to receive(:error).and_call_original
        end

        it "does not blow up and does not save anything" do
          expect do
            consume
          end.not_to change { DBForKarafkaConsumerTest.rentals.count }
        end

        it "logs the fact that there was a problem" do
          consume

          expect(logger).to have_received(:error).with(%r{synced_id nil})
        end
      end

      describe "known event" do
        describe "relationships edgecases" do
          context "when processing payload when known to-one relationship is not sideloaded" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:data) do
              {
                "links" => {
                  "account" => 123
                },
                "id" => 1,
                "name" => "Villa Saganaki"
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            it "does not blow up as it doesn't event process relationship for this scenario" do
              expect do
                consume
              end.not_to change { rental.to_one_associations_resolutions }
            end
          end

          context "when processing payload when known to-many relationship is not sideloaded" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:data) do
              {
                "links" => {
                  "bookings" => [123]
                },
                "id" => 1,
                "name" => "Villa Saganaki"
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            it "does not blow up as it doesn't event process relationship for this scenario" do
              expect do
                consume
              end.not_to change { rental.to_many_associations_resolutions }
            end
          end

          context "when processing payload when known to-many relationship is sideloaded and is empty" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:data) do
              {
                "links" => {
                  "bookings" => [123]
                },
                "id" => 1,
                "name" => "Villa Saganaki",
                "bookings" => []
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            it "processes this relationship as a legit scenario where the association is empty" do
              expect do
                consume
              end.to change { rental.to_many_associations_resolutions }
            end
          end
        end

        describe "processing mutex" do
          context "when :key is specified in the payload" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:data) do
              {
                "links" => {
                  "bookings" => [123]
                },
                "id" => 1,
                "name" => "Villa Saganaki",
                "bookings" => []
              }
            end

            before do
              DBForKarafkaConsumerTest.rentals << rental

              metadata["key"] = "Rental:1"
            end

            it "uses that key for the lock" do
              expect do
                consume
              end.to change { processing_mutex_provider.last_lock_name }.from(nil).to("Dionysus-Rental:1")
            end
          end

          context "when :key is not specified in the payload" do
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => "rental_created",
                "model_name" => "Rental",
                "data" => [data]
              }
            end
            let(:data) do
              {
                "links" => {
                  "bookings" => [123]
                },
                "id" => 1,
                "name" => "Villa Saganaki",
                "bookings" => []
              }
            end
            let(:uuid) { "16816fdd-286b-4216-bac4-cd29d64fbfae" }

            before do
              DBForKarafkaConsumerTest.rentals << rental
              allow(SecureRandom).to receive(:uuid) { uuid }
            end

            it "generates some random key using SecureRandom.uuid and uses that as a lock name" do
              expect do
                consume
              end.to change {
                       processing_mutex_provider.last_lock_name
                     }.from(nil).to("Dionysus-16816fdd-286b-4216-bac4-cd29d64fbfae")
            end
          end
        end

        describe "instrumentation" do
          let(:rental) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:booking) do
            BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
          end
          let(:account) do
            AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
          end
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_updated",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "links" => {
                "tax" => 201,
                "bookings" => [101, 102],
                "account" => 300
              },
              "id" => 1,
              "name" => "Villa Saganaki",
              "non_relationship" => [{ "id" => 1 }],
              "bookings" => [
                {
                  "id" => 101,
                  "start_at" => 1
                },
                {
                  "id" => 102,
                  "start_at" => 22
                }
              ],
              "tax" => {
                "id" => 201,
                "name" => "VAT"
              },
              "account" => {
                "id" => 300,
                "name" => "#WhateverItTakes"
              }
            }
          end

          before do
            DBForKarafkaConsumerTest.rentals << rental
            DBForKarafkaConsumerTest.bookings << booking
            DBForKarafkaConsumerTest.accounts << account
          end

          context "when instrumenter is specified" do
            let(:instrumenter) do
              Class.new do
                attr_reader :registrations

                def initialize
                  @registrations = Hash.new { |hash, key| hash[key] = [] }
                end

                def instrument(name, payload = {})
                  registrations[name] << payload
                  yield
                end
              end.new
            end
            let(:expected_registrations) do
              {
                "dionysus.consume.v8_rentals" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.deserialize" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.persist" => [
                  { event_name: "rental_updated", model_name: "Rental" }
                ],
                "dionysus.consume.v8_rentals.batch_number_1.persist.restore_with_dionysus" => [{}, {}, {}, {}, {}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.save" => [{}, {}, {}, {}, {}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_many_relationships" => [{}, {}, {},
                  {}, {}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_many_relationship.bookings" => [
                  { event_name: "rental_updated", parent_model_record: "Rental", relationship_name: "bookings" }
                ],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_many_relationship.bookings.persist" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationships" => [{}, {}, {}, {},
                  {}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_many_relationship.bookings.resolve_to_many_association" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationship.tax" => [
                  { event_name: "rental_updated", parent_model_record: "Rental", relationship_name: "tax" }
                ],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationship.tax.persist" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationship.tax.resolve_to_one_association" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationship.account" => [
                  { event_name: "rental_updated", parent_model_record: "Rental", relationship_name: "account" }
                ],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationship.account.persist" => [{}],
                "dionysus.consume.v8_rentals.batch_number_1.persist.persist_to_one_relationship.account.resolve_to_one_association" => [{}]
              }
            end

            before do
              config.instrumenter = instrumenter
            end

            it "uses that instrumenter for instrumentation" do
              expect do
                consume
              end.to change { instrumenter.registrations }.from({}).to(expected_registrations)
            end
          end

          context "when instrumenter is not specified" do
            it "does not blow up" do
              expect do
                consume
              end.not_to raise_error
            end
          end
        end

        describe "publishing events via EventBus" do
          let(:rental) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:booking) do
            BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
          end
          let(:account) do
            AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
          end
          let(:message_payload) do
            {
              "message" => [event_1, event_2]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_updated",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:event_2) do
            {
              "event" => "rental_destroyed",
              "model_name" => "Rental",
              "data" => [{ "id" => 10_101 }]
            }
          end
          let(:data) do
            {
              "links" => {
                "tax" => 201,
                "bookings" => [101, 102],
                "account" => 300
              },
              "id" => 1,
              "name" => "Villa Saganaki",
              "non_relationship" => [{ "id" => 1 }],
              "bookings" => [
                {
                  "id" => 101,
                  "start_at" => 1
                },
                {
                  "id" => 102,
                  "start_at" => 22
                }
              ],
              "tax" => {
                "id" => 201,
                "name" => "VAT"
              },
              "account" => {
                "id" => 300,
                "name" => "#WhateverItTakes"
              }
            }
          end

          before do
            DBForKarafkaConsumerTest.rentals << rental
            DBForKarafkaConsumerTest.bookings << booking
            DBForKarafkaConsumerTest.accounts << account
          end

          context "when event bus is specified" do
            let(:event_bus) do
              Class.new do
                attr_reader :events

                def initialize
                  @events = []
                end

                def publish(event_name, payload = {})
                  events << [event_name, payload]
                end
              end.new
            end
            let(:expected_events) do
              [
                [
                  "dionysus.consume",
                  {
                    topic_name: "v8_rentals",
                    event_name: "rental_updated",
                    model_name: "Rental",
                    transformed_data: [
                      attributes: {
                        "synced_tax_id" => 201,
                        "synced_booking_ids" => [101, 102],
                        "synced_account_id" => 300,
                        "synced_id" => 1,
                        "name" => "Villa Saganaki",
                        "non_relationship" => [
                          { "id" => 1 }
                        ]
                      },
                      has_many: [
                        [
                          "bookings",
                          [
                            {
                              attributes: {
                                "synced_id" => 101,
                                "start_at" => 1
                              },
                              has_many: [],
                              has_one: []
                            },
                            {
                              attributes: {
                                "synced_id" => 102,
                                "start_at" => 22
                              },
                              has_many: [],
                              has_one: []
                            }
                          ]
                        ]
                      ],
                      has_one: [
                        [
                          "tax",
                          {
                            attributes: {
                              "synced_id" => 201,
                              "name" => "VAT"
                            },
                            has_many: [],
                            has_one: []
                          }
                        ],
                        [
                          "account",
                          {
                            attributes:
                              {
                                "synced_id" => 300,
                                "name" => "#WhateverItTakes"
                              },
                            has_many: [],
                            has_one: []
                          }
                        ]
                      ]
                    ],
                    local_changes: {
                      ["Rental", 1] => { "name" => ["old name", "Villa Saganaki"] },
                      ["bookings", 101] => { "start_at" => [nil, 1] },
                      ["bookings", 102] => { "start_at" => [2, 22] },
                      ["tax", 201] => { "name" => [nil, "VAT"] },
                      ["account", 300] => { "name" => ["Discipline Equals Freedom", "#WhateverItTakes"] }
                    }
                  }
                ],
                [
                  "dionysus.consume",
                  {
                    topic_name: "v8_rentals",
                    event_name: "rental_destroyed",
                    model_name: "Rental",
                    transformed_data: [
                      attributes: {
                        "synced_id" => 10_101
                      },
                      has_many: [],
                      has_one: []
                    ],
                    local_changes: {}
                  }
                ]
              ]
            end

            before do
              config.event_bus = event_bus
            end

            it "uses that event bus for publishing events" do
              expect do
                consume
              end.to change { event_bus.events }.from([]).to(expected_events)
            end
          end

          context "when event bus is not specified" do
            it "does not blow up" do
              expect do
                consume
              end.not_to raise_error
            end
          end
        end

        describe "mapping" do
          let(:rental) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "links" => {
                "bookings" => [123]
              },
              "id" => 1,
              "name" => "Villa Saganaki",
              "type" => "villa"
            }
          end

          before do
            DBForKarafkaConsumerTest.rentals << rental
            config.add_attributes_mapping_for_model("Rental") do
              {
                rental_type: :type
              }
            end
          end

          it "uses provided mapping to figure out how to map remote attribute to local attribute" do
            expect do
              consume
            end.to change { rental.name }.to("Villa Saganaki")
              .and change { rental.rental_type }.to("villa")
          end
        end

        describe "importing" do
          context "when topic has configured import: true" do
            let(:topic) do
              Dionysus::Consumer::Registry::Registration::Topic.new(:v8, "rentals", nil, import: true)
            end
            let(:rental) do
              RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
            end
            let(:message_payload) do
              {
                "message" => [event_1]
              }
            end
            let(:event_1) do
              {
                "event" => event_name,
                "model_name" => model_name,
                "data" => [data]
              }
            end
            let(:data) do
              {
                "links" => {
                  "bookings" => [123]
                },
                "id" => 1,
                "name" => "Villa Saganaki"
              }
            end
            let(:model_name) { "Rental" }

            before do
              DBForKarafkaConsumerTest.rentals << rental
            end

            after do
              ModelRepositoryForKarafkaConsumerTest.dionysus_import(nil)
            end

            context "when the event is _created one" do
              let(:event_name) { "rental_created" }

              context "when model is known" do
                it "performs import bypassing the rest of the flow with relationships etc." do
                  expect do
                    consume
                  end.to change { ModelRepositoryForKarafkaConsumerTest.dionysus_import_data }.from(nil)
                    .and avoid_changing { rental.saved }
                    .and avoid_changing { rental.name }

                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_import_data.count).to eq(1)
                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_import_data.first.attributes).to eq(
                    "name" => "Villa Saganaki",
                    "synced_id" => 1,
                    "synced_booking_ids" => [123]
                  )
                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_import_data.first.has_many).to eq([[
                    "bookings", nil
                  ]])
                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_import_data.first.has_one).to eq([])
                end
              end

              context "when model is not known" do
                let(:model_name) { "Unknown" }

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end

            context "when the event is _updated one" do
              let(:event_name) { "rental_updated" }

              it "doesn't change anything and the normal flow is performed" do
                expect do
                  consume
                end.to change { rental.name }.to("Villa Saganaki")
                  .and avoid_changing { ModelRepositoryForKarafkaConsumerTest.dionysus_import_data }.from(nil)
              end
            end

            context "when the event is _destroyed one" do
              let(:event_name) { "rental_destroyed" }

              context "when model is known" do
                it "performs import bypassing the rest of the flow with relationships etc." do
                  expect do
                    consume
                  end.to change { ModelRepositoryForKarafkaConsumerTest.dionysus_destroy_data }.from(nil)
                    .and avoid_changing { rental.saved }
                    .and avoid_changing { rental.name }

                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_destroy_data.count).to eq(1)
                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_destroy_data.first.attributes).to eq(
                    "name" => "Villa Saganaki",
                    "synced_id" => 1,
                    "synced_booking_ids" => [123]
                  )
                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_destroy_data.first.has_many).to eq([[
                    "bookings", nil
                  ]])
                  expect(ModelRepositoryForKarafkaConsumerTest.dionysus_destroy_data.first.has_one).to eq([])
                end
              end

              context "when model is not known" do
                let(:model_name) { "Unknown" }

                it "does not blow up" do
                  expect do
                    consume
                  end.not_to raise_error
                end
              end
            end
          end
        end

        describe "assigning synced data" do
          let(:rental) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:booking) do
            BookingForKarafkaConsumerTest.new(synced_id: 102, start_at: 2)
          end
          let(:account) do
            AccountForKarafkaConsumerTest.new(synced_id: 300, name: "Discipline Equals Freedom")
          end
          let(:message_payload) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_updated",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "links" => {
                "tax" => 201,
                "bookings" => [101, 102],
                "account" => 300
              },
              "id" => 1,
              "name" => "Villa Saganaki",
              "non_relationship" => [{ "id" => 1 }],
              "bookings" => [
                {
                  "id" => 101,
                  "start_at" => 1
                },
                {
                  "id" => 102,
                  "start_at" => 22
                }
              ],
              "tax" => {
                "id" => 201,
                "name" => "VAT"
              },
              "account" => {
                "id" => 300,
                "name" => "#WhateverItTakes"
              }
            }
          end

          let(:expected_rental_synced_data) do
            {
              "name" => "Villa Saganaki",
              "non_relationship" => [{ "id" => 1 }],
              "synced_account_id" => 300,
              "synced_id" => 1,
              "synced_tax_id" => 201,
              "synced_booking_ids" => [101, 102]
            }
          end

          before do
            DBForKarafkaConsumerTest.rentals << rental
            DBForKarafkaConsumerTest.bookings << booking
            DBForKarafkaConsumerTest.accounts << account
          end

          it "assigns synced data to the models" do
            expect do
              consume
            end.to change { rental.synced_data }.from(nil).to(expected_rental_synced_data)
              .and change { booking.synced_data }.from(nil).to("start_at" => 22, "synced_id" => 102)
              .and change { account.synced_data }.from(nil).to("name" => "#WhateverItTakes", "synced_id" => 300)

            expect(DBForKarafkaConsumerTest.bookings.last.synced_data).to eq("start_at" => 1, "synced_id" => 101)
            expect(DBForKarafkaConsumerTest.taxes.last.synced_data).to eq("name" => "VAT", "synced_id" => 201)
          end
        end

        describe "processing tombstones" do
          let(:message_payload) do
            {
              "payload" => nil
            }
          end

          it "returns early when it encounters a tombstone" do
            expect do
              consume
            end.not_to raise_error
          end
        end

        describe "with custom retry provider" do
          let(:config) do
            Dionysus::Consumer::Config.new.tap do |current_config|
              model_factory_for_karafka_consumer_test = ModelFactoryForKarafkaConsumerTest
              current_config.model_factory = model_factory_for_karafka_consumer_test.new
              current_config.transaction_provider = transaction_provider
              current_config.retry_provider = retry_provider
            end
          end
          let(:retry_provider) do
            Class.new do
              attr_reader :retried

              def initialize
                @retried = false
              end

              def retry
                @retried = true
                yield
              end
            end.new
          end
          let(:rental) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:message_payload) do
            {
              "message" => [event_1],
              "key" => "Rental:1"
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data]
            }
          end
          let(:data) do
            {
              "id" => 1,
              "name" => "Villa Saganaki"
            }
          end

          before do
            DBForKarafkaConsumerTest.rentals << rental
          end

          it "uses custom retry provider" do
            expect do
              consume
            end.to change { retry_provider.retried }.from(false).to(true)
          end
        end

        describe "concurrency" do
          let(:topic) do
            Dionysus::Consumer::Registry::Registration::Topic.new(:v8, "rentals", nil, concurrency: true)
          end
          let(:params_batch) do
            [
              Karafka::Messages::Message.new(message_payload_1, metadata_1),
              Karafka::Messages::Message.new(message_payload_2, metadata_2),
              Karafka::Messages::Message.new(message_payload_3, metadata_3)
            ]
          end
          let(:rental_1) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:message_payload_1) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_1]
            }
          end
          let(:data_1) do
            {
              "links" => {
                "bookings" => [123]
              },
              "id" => 1,
              "name" => "Villa Saganaki",
              "bookings" => []
            }
          end
          let(:metadata_1) do
            ::Karafka::Messages::Metadata.new.tap do |metadata|
              metadata["deserializer"] = deserializer
              metadata["key"] = "Rental:1"
            end
          end
          let(:message_payload_2) do
            {
              "message" => [event_2]
            }
          end
          let(:event_2) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_2]
            }
          end
          let(:data_2) do
            {
              "links" => {
                "bookings" => [321]
              },
              "id" => 2,
              "name" => "Other Villa",
              "bookings" => []
            }
          end
          let(:metadata_2) do
            ::Karafka::Messages::Metadata.new.tap do |metadata|
              metadata["deserializer"] = deserializer
              metadata["key"] = "Rental:2"
            end
          end
          let(:message_payload_3) do
            {
              "message" => [event_3]
            }
          end
          let(:event_3) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_3]
            }
          end
          let(:data_3) do
            {
              "links" => {
                "bookings" => [12]
              },
              "id" => 3,
              "name" => "Yet Another Villa",
              "bookings" => []
            }
          end
          let(:metadata_3) do
            ::Karafka::Messages::Metadata.new.tap do |metadata|
              metadata["deserializer"] = deserializer
              metadata["key"] = "Rental:3"
            end
          end
          let(:event_bus) do
            Class.new do
              attr_reader :events

              def initialize
                @events = []
              end

              def publish(event_name, payload = {})
                events << [event_name, payload]
              end
            end.new
          end

          before do
            DBForKarafkaConsumerTest.reset!
            DBForKarafkaConsumerTest.rentals << rental_1
            config.event_bus = event_bus
          end

          describe "on success" do
            let(:name) do
              consume
            end

            it "handles processing correctly" do
              expect do
                name
              end.to change { DBForKarafkaConsumerTest.rentals.count }.from(1).to(3)
                .and change { rental_1.name }.from("old name").to("Villa Saganaki")
            end

            it "applies concurrency setting" do
              expect_any_instance_of(Dionysus::Consumer::WorkersGroup).to receive(:work)
                .and_call_original

              consume
            end

            it "publishes events correctly in the end" do
              expect do
                consume
              end.to change { event_bus.events.count }.from(0).to(3)
            end
          end

          describe "on error" do
            before do
              allow(processing_mutex_provider).to receive(:lock_with).with("Dionysus-Rental:1").and_call_original
              allow(processing_mutex_provider).to receive(:lock_with).with("Dionysus-Rental:2") { raise "whoops" }
              allow(processing_mutex_provider).to receive(:lock_with).with("Dionysus-Rental:3").and_call_original
            end

            it "does not silently fail, it raises error when it happens" do
              expect do
                consume
              end.to raise_error(%r{whoops})
            end

            it "still processes things from other threads that don't blow up", retry: 3 do
              expect do
                consume
              rescue
                nil
              end.to change { DBForKarafkaConsumerTest.rentals.count }.from(1).to(2)
                .and change { rental_1.name }.from("old name").to("Villa Saganaki")
            end

            it "does not publish any events, despite a partial success" do
              expect do
                consume
              rescue
                nil
              end.not_to change { event_bus.events.count }
            end
          end
        end

        describe "params_batch_transformation" do
          let(:topic) do
            Dionysus::Consumer::Registry::Registration::Topic.new(:v8, "rentals", nil,
              params_batch_transformation: params_batch_transformation)
          end
          let(:params_batch_transformation) do
            lambda do |params_batch|
              payload = {
                "message" => params_batch.to_a.flat_map { |batch| batch.payload["message"] }
              }
              [
                double(payload: payload, metadata: double(key: SecureRandom.uuid))
              ]
            end
          end
          let(:params_batch) do
            [
              Karafka::Messages::Message.new(message_payload_1, metadata_1),
              Karafka::Messages::Message.new(message_payload_2, metadata_2),
              Karafka::Messages::Message.new(message_payload_3, metadata_3)
            ]
          end
          let(:rental_1) do
            RentalForKarafkaConsumerTest.new(synced_id: 1, name: "old name")
          end
          let(:message_payload_1) do
            {
              "message" => [event_1]
            }
          end
          let(:event_1) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_1]
            }
          end
          let(:data_1) do
            {
              "links" => {
                "bookings" => [123]
              },
              "id" => 1,
              "name" => "Villa Saganaki",
              "bookings" => []
            }
          end
          let(:metadata_1) do
            ::Karafka::Messages::Metadata.new.tap do |metadata|
              metadata["deserializer"] = deserializer
              metadata["key"] = "Rental:1"
            end
          end
          let(:message_payload_2) do
            {
              "message" => [event_2]
            }
          end
          let(:event_2) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_2]
            }
          end
          let(:data_2) do
            {
              "links" => {
                "bookings" => [321]
              },
              "id" => 2,
              "name" => "Other Villa",
              "bookings" => []
            }
          end
          let(:metadata_2) do
            ::Karafka::Messages::Metadata.new.tap do |metadata|
              metadata["deserializer"] = deserializer
              metadata["key"] = "Rental:2"
            end
          end
          let(:message_payload_3) do
            {
              "message" => [event_3]
            }
          end
          let(:event_3) do
            {
              "event" => "rental_created",
              "model_name" => "Rental",
              "data" => [data_3]
            }
          end
          let(:data_3) do
            {
              "links" => {
                "bookings" => [12]
              },
              "id" => 3,
              "name" => "Yet Another Villa",
              "bookings" => []
            }
          end
          let(:metadata_3) do
            ::Karafka::Messages::Metadata.new.tap do |metadata|
              metadata["deserializer"] = deserializer
              metadata["key"] = "Rental:3"
            end
          end
          let(:event_bus) do
            Class.new do
              attr_reader :events

              def initialize
                @events = []
              end

              def publish(event_name, payload = {})
                events << [event_name, payload]
              end
            end.new
          end

          before do
            allow(consumer).to receive(:process_batch).exactly(1).and_call_original
            DBForKarafkaConsumerTest.rentals << rental_1
            config.event_bus = event_bus
          end

          it "handles processing correctly" do
            expect do
              consume
            end.to change { DBForKarafkaConsumerTest.rentals.count }.from(1).to(3)
              .and change { rental_1.name }.from("old name").to("Villa Saganaki")
          end

          it "calls ParamsBatchProcessor only once as params have been merged" do
            consume

            expect(consumer).to have_received(:process_batch).exactly(1)
          end

          it "publishes events correctly in the end" do
            expect do
              consume
            end.to change { event_bus.events.count }.from(0).to(3)
          end
        end
      end
    end
  end
end
