# frozen_string_literal: true

RSpec.describe Dionysus::Producer::PartitionKey do
  describe "#to_key" do
    subject(:to_key) { described_class.new(resource, config: config).to_key(responder: responder) }

    let(:resource) { ExampleResource.new(id: 1, account_id: 2) }
    let(:config) do
      Dionysus::Producer::Config.new.tap { |conf| conf.default_partition_key = default_partition_key }
    end
    let(:default_partition_key) { nil }
    let(:responder) { double(:responder, partition_key: responder_partition_key) }
    let(:responder_partition_key) { nil }

    context "when custom partition key is defined" do
      context "when it's a lambda" do
        context "when the result is nil" do
          let(:responder_partition_key) do
            ->(_resource) {}
          end

          it { is_expected.to be_nil }
        end

        context "when the result is not nil" do
          let(:responder_partition_key) do
            ->(resource) { resource.id * 10 }
          end

          it "calls the lambda with the resource" do
            expect(to_key).to eq "10"
          end
        end
      end

      context "when it's a symbol" do
        context "when resource responds to that partition key" do
          let(:responder_partition_key) { :id }

          it { is_expected.to eq "1" }
        end

        context "when resource does not respond to that partition key" do
          let(:responder_partition_key) { :booking_id }

          it { is_expected.to be_nil }
        end
      end
    end

    context "when custom partition key is not defined" do
      context "when it's a lambda" do
        context "when the result is nil" do
          let(:default_partition_key) do
            ->(_resource) {}
          end

          it { is_expected.to be_nil }
        end

        context "when the result is not nil" do
          let(:default_partition_key) do
            ->(resource) { (resource.id * 10) + 1 }
          end

          it "calls the lambda with the resource" do
            expect(to_key).to eq "11"
          end
        end
      end

      context "when it's not a lambda" do
        context "when resource responds to the default partition key" do
          let(:default_partition_key) { :account_id }

          it { is_expected.to eq "2" }
        end

        context "when resource does not respond to the default partition key" do
          let(:default_partition_key) { :rental_id }

          it { is_expected.to be_nil }
        end
      end
    end
  end
end
