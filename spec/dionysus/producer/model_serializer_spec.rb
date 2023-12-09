# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::ModelSerializer do
  describe "#as_json" do
    subject(:as_json) { serializer.as_json }

    let(:serializer) { example_serializer.new(record, include: include, context_serializer: context_serializer) }
    let(:example_serializer) do
      Class.new(Dionysus::Producer::ModelSerializer) do
        attributes :id, :name

        has_one :account

        has_many :bookings
      end
    end
    let(:account_serializer) do
      Class.new(Dionysus::Producer::ModelSerializer) do
        attributes :id
      end
    end
    let(:booking_serializer) do
      Class.new(Dionysus::Producer::ModelSerializer) do
        attributes :id
      end
    end
    let(:context_serializer) do
      Class.new(Dionysus::Producer::Serializer) do
        class << self
          attr_reader :account_serializer
        end

        class << self
          attr_reader :booking_serializer
        end

        def infer_serializer
          return self.class.account_serializer if records.first.id == "2_from_account"

          self.class.booking_serializer
        end
      end.tap do |current_serializer|
        current_serializer.instance_variable_set(:@account_serializer, account_serializer)
        current_serializer.instance_variable_set(:@booking_serializer, booking_serializer)
      end
    end
    let(:record) do
      Class.new do
        def id
          1
        end

        def persisted?
          true
        end

        def name
          "name"
        end

        def account_id
          2
        end

        def account
          Class.new do
            def id
              "2_from_account"
            end

            def persisted?
              true
            end
          end.new
        end

        def booking_ids
          [3]
        end

        def bookings
          [Class.new do
            def id
              "3_from_booking"
            end

            def persisted?
              true
            end
          end.new]
        end
      end.new
    end

    context "when :include is an empty array" do
      let(:include) do
        []
      end
      let(:expected_result) do
        {
          "id" => 1,
          "name" => "name",
          "links" => {
            "account" => 2,
            "bookings" => [3]
          }
        }
      end

      it { is_expected.to eq expected_result }
    end

    context "when :include is not an empty array" do
      let(:include) { %i[account bookings] }
      let(:expected_result) do
        {
          "id" => 1,
          "name" => "name",
          "links" => {
            "account" => 2,
            "bookings" => [3]
          },
          "account" => {
            "id" => "2_from_account",
            "links" => {}
          },
          "bookings" => [
            {
              "id" => "3_from_booking",
              "links" => {}
            }
          ]
        }
      end

      it { is_expected.to eq expected_result }
    end
  end
end
