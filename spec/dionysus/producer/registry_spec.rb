# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Registry do
  describe "#namespace/#registrations" do
    subject(:registrations) { registry.registrations }

    let(:registry) { described_class.new }

    before do
      registry.namespace :v3 do
        serializer Struct.new(:name).new("serializer")

        topic :rentals, partition_key: :account_id do
          publish "Rental"
          publish "RentalsTax"
        end
      end

      registry.namespace :v4 do
        serializer Struct.new(:name).new("serializer_v4")

        topic :bookings, genesis_replica: true do
          publish "Booking", with: ["BookingsFee"]
        end

        topic :accounts do
          publish "Account", observe: [
            {
              model: ExampleResource,
              attributes: %i[created_at],
              association_name: :example_publishable_resource
            }
          ]
        end
      end
    end

    it "performs the actual registration that is available under registrations attribute
    and generates Karafka Responders" do
      expect(registrations.keys).to eq %i[v3 v4]

      expect(registrations[:v3].topics.count).to eq 1
      expect(registrations[:v3].producers.count).to eq 1
      expect(registrations[:v3].producers.first).to eq Dionysus::V3RentalResponder
      expect(registrations[:v3].namespace).to eq :v3
      expect(registrations[:v3].serializer_klass.name).to eq "serializer"
      expect(registrations[:v3].topics.first.name).to eq :rentals
      expect(registrations[:v3].topics.first.namespace).to eq :v3
      expect(registrations[:v3].topics.first.to_s).to eq "v3_rentals"
      expect(registrations[:v3].topics.first.genesis_to_s).to be_nil
      expect(registrations[:v3].topics.first.producer).to eq Dionysus::V3RentalResponder
      expect(registrations[:v3].topics.first.serializer_klass.name).to eq "serializer"
      expect(registrations[:v3].topics.first.options).to eq(partition_key: :account_id)
      expect(registrations[:v3].topics.first.genesis_replica?).to be false
      expect(registrations[:v3].topics.first.partition_key).to eq :account_id
      expect(registrations[:v3].topics.first.models.count).to eq 2
      expect(registrations[:v3].topics.first.models.first.model_klass).to eq "Rental"
      expect(registrations[:v3].topics.first.models.first.options).to eq({})
      expect(registrations[:v3].topics.first.models.last.model_klass).to eq "RentalsTax"
      expect(registrations[:v3].topics.first.models.last.options).to eq({})
      expect(registrations[:v3].topics.first.publishes_model?("Rental")).to be true
      expect(registrations[:v3].topics.first.publishes_model?("RentalsTax")).to be true
      expect(registrations[:v3].topics.first.publishes_model?("Booking")).to be false

      expect(registrations[:v4].topics.count).to eq 2
      expect(registrations[:v4].producers.count).to eq 2
      expect(registrations[:v4].producers.first).to eq Dionysus::V4BookingResponder
      expect(registrations[:v4].producers.last).to eq Dionysus::V4AccountResponder
      expect(registrations[:v4].namespace).to eq :v4
      expect(registrations[:v4].serializer_klass.name).to eq "serializer_v4"
      expect(registrations[:v4].topics.first.name).to eq :bookings
      expect(registrations[:v4].topics.first.namespace).to eq :v4
      expect(registrations[:v4].topics.first.to_s).to eq "v4_bookings"
      expect(registrations[:v4].topics.first.genesis_to_s).to eq "v4_bookings_genesis"
      expect(registrations[:v4].topics.first.producer).to eq Dionysus::V4BookingResponder
      expect(registrations[:v4].topics.first.serializer_klass.name).to eq "serializer_v4"
      expect(registrations[:v4].topics.first.options).to eq(genesis_replica: true)
      expect(registrations[:v4].topics.first.genesis_replica?).to be true
      expect(registrations[:v4].topics.first.partition_key).to be_nil
      expect(registrations[:v4].topics.first.models.count).to eq 1
      expect(registrations[:v4].topics.first.models.first.model_klass).to eq "Booking"
      expect(registrations[:v4].topics.first.models.first.options).to eq(with: ["BookingsFee"])
      expect(registrations[:v4].topics.first.publishes_model?("Booking")).to be true
      expect(registrations[:v4].topics.first.publishes_model?("BookingsFee")).to be false
      expect(registrations[:v4].topics.first.publishes_model?("Rental")).to be false
      expect(registrations[:v4].topics.last.name).to eq :accounts
      expect(registrations[:v4].topics.last.namespace).to eq :v4
      expect(registrations[:v4].topics.last.to_s).to eq "v4_accounts"
      expect(registrations[:v4].topics.last.genesis_to_s).to be_nil
      expect(registrations[:v4].topics.last.producer).to eq Dionysus::V4AccountResponder
      expect(registrations[:v4].topics.last.serializer_klass.name).to eq "serializer_v4"
      expect(registrations[:v4].topics.last.options).to eq({})
      expect(registrations[:v4].topics.last.genesis_replica?).to be false
      expect(registrations[:v4].topics.last.partition_key).to be_nil
      expect(registrations[:v4].topics.last.models.count).to eq 1
      expect(registrations[:v4].topics.last.models.first.model_klass).to eq "Account"
      expect(registrations[:v4].topics.last.models.first.options).to eq(observe: [{
        association_name: :example_publishable_resource, attributes: [:created_at], model: ExampleResource
      }])
      expect(registrations[:v4].topics.last.publishes_model?("Account")).to be true
      expect(registrations[:v4].topics.last.publishes_model?("Booking")).to be false
      expect(registrations[:v4].topics.last.publishes_model?("Rental")).to be false
      expect(registrations[:v4].topics.last.models.first.observes?(ExampleResource, { "id" => [nil, 1] })).to be false
      expect(registrations[:v4].topics.last.models.first.observes?(ExampleResource,
        { "created_at" => [nil, Time.current] })).to be true
      expect(registrations[:v4].topics.last.models.first.association_name_for_observable(ExampleResource,
        { "created_at" => [nil, Time.current] })).to eq :example_publishable_resource
    end
  end

  describe "model registration options validations" do
    let(:registry) { described_class.new }

    describe "observers" do
      context "when :model argument is missing" do
        subject(:declare_schema) do
          registry.namespace :v4 do
            serializer Struct.new(:name).new("serializer_v4")

            topic :accounts do
              publish "Account", observe: [
                {
                  attributes: %i[created_at],
                  association_name: :example_publishable_resource
                }
              ]
            end
          end
        end

        it { is_expected_block.to raise_error KeyError, "key not found: :model" }
      end

      context "when :attributes argument is missing" do
        subject(:declare_schema) do
          registry.namespace :v4 do
            serializer Struct.new(:name).new("serializer_v4")

            topic :accounts do
              publish "Account", observe: [
                {
                  model: ExampleResource,
                  association_name: :example_publishable_resource
                }
              ]
            end
          end
        end

        it { is_expected_block.to raise_error KeyError, "key not found: :attributes" }
      end

      context "when :association_name argument is missing" do
        subject(:declare_schema) do
          registry.namespace :v4 do
            serializer Struct.new(:name).new("serializer_v4")

            topic :accounts do
              publish "Account", observe: [
                {
                  model: ExampleResource,
                  attributes: %i[created_at]
                }
              ]
            end
          end
        end

        it { is_expected_block.to raise_error KeyError, "key not found: :association_name" }
      end

      context "when :association_name is something that the model does not respond to" do
        context "when using symbol" do
          subject(:declare_schema) do
            registry.namespace :v4 do
              serializer Struct.new(:name).new("serializer_v4")

              topic :accounts do
                publish "Account", observe: [
                  {
                    model: ExampleResource,
                    attributes: %i[id],
                    association_name: :account
                  }
                ]
              end
            end
          end

          it {
            is_expected_block.to raise_error ArgumentError,
              "association name :account does not exist on model ExampleResource"
          }
        end

        context "when using string" do
          subject(:declare_schema) do
            registry.namespace :v4 do
              serializer Struct.new(:name).new("serializer_v4")

              topic :accounts do
                publish "Account", observe: [
                  {
                    model: ExampleResource,
                    attributes: %i[id],
                    association_name: "some.chaining.to.get.account"
                  }
                ]
              end
            end
          end

          it { is_expected_block.not_to raise_error }
        end
      end

      context "when everything is fine" do
        subject(:declare_schema) do
          registry.namespace :v4 do
            serializer Struct.new(:name).new("serializer_v4")

            topic :accounts do
              publish "Account", observe: [
                {
                  model: ExampleResource,
                  attributes: %i[id],
                  association_name: :example_publishable_resource
                }
              ]
            end
          end
        end

        it { is_expected_block.not_to raise_error }
      end
    end
  end
end
