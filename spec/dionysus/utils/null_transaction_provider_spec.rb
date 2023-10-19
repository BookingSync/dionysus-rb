# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullTransactionProvider do
  describe ".transaction" do
    subject(:transaction) { described_class.transaction { puts "retry" } }

    it "yields block" do
      expect do
        transaction
      end.not_to raise_error
    end
  end

  describe ".connection_pool" do
    subject(:connection_pool) { transaction_provider.connection_pool }

    let(:transaction_provider) { described_class }

    it { is_expected.to eq transaction_provider }
  end

  describe ".with_connection" do
    subject(:with_connection) { described_class.with_connection { puts "retry" } }

    it "yields block" do
      expect do
        with_connection
      end.not_to raise_error
    end
  end
end
