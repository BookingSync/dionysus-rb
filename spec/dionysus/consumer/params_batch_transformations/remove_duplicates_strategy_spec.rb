# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy do
  describe ".call" do
    subject(:call) { described_class.new.call(params_batch) }

    let(:params_batch) { Karafka::Messages::Messages.new(messages_array, metadata) }
    let(:messages_array) { [message_1, message_2, message_3, message_4, message_5, message_6] }

    describe "typical case" do
      let(:messages_array) { [message_1, message_2, message_3, message_4, message_5, message_6] }

      let(:message_1) { double(:batch_1, payload: payload_1, offset: 1) }
      let(:message_2) { double(:batch_2, payload: payload_2, offset: 2) }
      let(:message_3) { double(:batch_3, payload: payload_3, offset: 3) }
      let(:message_4) { double(:batch_4, payload: payload_4, offset: 4) }
      let(:message_5) { double(:batch_5, payload: payload_5, offset: 5) }
      let(:message_6) { double(:batch_6, payload: payload_6, offset: 6) }
      let(:metadata) { double(:metadata) }
      let(:payload_1) do
        {
          "message" => [
            {
              "event" => "booking_created",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Rich",
                  "updated_at" => "2023-07-13T16:01:41.0Z"
                }
              ]
            }
          ]
        }
      end
      let(:payload_2) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Rich",
                  "updated_at" => "2023-07-13T16:01:41.5Z"
                }
              ]
            }
          ]
        }
      end
      let(:payload_3) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 17,
                  "client_name" => "Rich",
                  "updated_at" => "2023-07-13T16:01:41.6Z"
                }
              ]
            }
          ]
        }
      end
      let(:payload_4) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Updated",
                  "updated_at" => "2023-07-13T16:01:41.7Z"
                }
              ]
            }
          ]
        }
      end
      let(:payload_5) do
        {
          "message" => [
            {
              "event" => "booking_canceled",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16
                }
              ]
            }
          ]
        }
      end
      let(:payload_6) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Updated",
                  "updated_at" => "2023-07-13T16:01:41.2Z"
                }
              ]
            }
          ]
        }
      end

      it "returns Karafka::Messages::Messages" do
        expect(call).to be_a(Karafka::Messages::Messages)
      end

      it "removes duplicates by keeping the last published message and preserving metadata" do
        expect(call.to_a).to eq([message_1, message_6, message_3, message_5])
        expect(call.metadata).to eq(metadata)
      end
    end

    describe "anomaly - message with 2 events" do
      let(:messages_array) { [message_1, message_2, message_3, message_4, message_5] }

      let(:message_1) { double(:batch_1, payload: payload_1, offset: 1) }
      let(:message_2) { double(:batch_2, payload: payload_2, offset: 2) }
      let(:message_3) { double(:batch_3, payload: payload_3, offset: 3) }
      let(:message_4) { double(:batch_4, payload: payload_4, offset: 4) }
      let(:message_5) { double(:batch_5, payload: payload_5, offset: 5) }
      let(:metadata) { double(:metadata) }
      let(:payload_1) do
        {
          "message" => [
            {
              "event" => "booking_created",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Rich"
                }
              ]
            }
          ]
        }
      end
      let(:payload_2) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Rich"
                }
              ]
            }
          ]
        }
      end
      let(:payload_3) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 17,
                  "client_name" => "Rich"
                }
              ]
            },
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 18,
                  "client_name" => "Rich"
                }
              ]
            }
          ]
        }
      end
      let(:payload_4) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Updated"
                }
              ]
            }
          ]
        }
      end
      let(:payload_5) do
        {
          "message" => [
            {
              "event" => "booking_canceled",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16
                }
              ]
            }
          ]
        }
      end

      it "returns Karafka::Messages::Messages" do
        expect(call).to be_a(Karafka::Messages::Messages)
      end

      it "does not remove duplicates" do
        expect(call).to eq params_batch
      end
    end

    describe "anomaly - message with an event with 2 data items" do
      let(:messages_array) { [message_1, message_2, message_3, message_4, message_5] }

      let(:message_1) { double(:batch_1, payload: payload_1, offset: 1) }
      let(:message_2) { double(:batch_2, payload: payload_2, offset: 2) }
      let(:message_3) { double(:batch_3, payload: payload_3, offset: 3) }
      let(:message_4) { double(:batch_4, payload: payload_4, offset: 4) }
      let(:message_5) { double(:batch_5, payload: payload_5, offset: 5) }
      let(:metadata) { double(:metadata) }
      let(:payload_1) do
        {
          "message" => [
            {
              "event" => "booking_created",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Rich"
                }
              ]
            }
          ]
        }
      end
      let(:payload_2) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Rich"
                }
              ]
            }
          ]
        }
      end
      let(:payload_3) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 17,
                  "client_name" => "Rich"
                },
                {
                  "id" => 18,
                  "client_name" => "Rich"
                }
              ]
            }
          ]
        }
      end
      let(:payload_4) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "client_name" => "Updated"
                }
              ]
            }
          ]
        }
      end
      let(:payload_5) do
        {
          "message" => [
            {
              "event" => "booking_canceled",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16
                }
              ]
            }
          ]
        }
      end

      it "returns Karafka::Messages::Messages" do
        expect(call).to be_a(Karafka::Messages::Messages)
      end

      it "does not remove duplicates" do
        expect(call).to eq params_batch
      end
    end

    describe "when the last published message carries an older updated_at" do
      # reproduces booking 22578292, partition 6, offsets 17288919-17288920
      let(:messages_array) { [message_1, message_2] }

      let(:message_1) { double(:batch_1, payload: payload_1, offset: 1) }
      let(:message_2) { double(:batch_2, payload: payload_2, offset: 2) }
      let(:metadata) { double(:metadata) }

      let(:payload_1) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "paid_amount" => "0.0",
                  "updated_at" => "2026-08-17T13:39:05.982700Z"
                }
              ]
            }
          ]
        }
      end
      let(:payload_2) do
        {
          "message" => [
            {
              "event" => "booking_updated",
              "model_name" => "Booking",
              "data" => [
                {
                  "id" => 16,
                  "paid_amount" => "57.89",
                  "updated_at" => "2026-08-17T13:39:05.953694Z"
                }
              ]
            }
          ]
        }
      end

      it "keeps the last published message, not the one with the highest updated_at" do
        expect(call.to_a).to eq([message_2])
      end
    end
  end
end
