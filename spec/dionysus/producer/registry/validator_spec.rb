# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Registry::Validator do
  describe "#validate_columns" do
    subject(:validate_columns) { described_class.new.validate_columns }

    describe "when there is some invalid column that does not exist" do
      before do
        Dionysus::Producer.declare do
          namespace :v4 do
            serializer Struct.new(:name).new("serializer_v4")

            topic :accounts do
              publish "Account", observe: [
                {
                  model: ExampleResource,
                  attributes: %i[name created_at],
                  association_name: :example_publishable_resource
                }
              ]
              publish "Rental"
            end
          end
        end
      end

      it {
        is_expected_block.to raise_error ArgumentError,
          "some attributes [:name, :created_at] do not exist on model ExampleResource"
      }
    end

    describe "when everything is valid" do
      before do
        Dionysus::Producer.declare do
          namespace :v4 do
            serializer Struct.new(:name).new("serializer_v4")

            topic :accounts do
              publish "Account", observe: [
                {
                  model: ExampleResource,
                  attributes: %i[created_at],
                  association_name: :example_publishable_resource
                }
              ]
              publish "Rental"
            end
          end
        end
      end

      it { is_expected_block.not_to raise_error }
    end
  end
end
