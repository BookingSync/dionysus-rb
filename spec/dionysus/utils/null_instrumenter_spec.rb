# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullInstrumenter do
  describe ".instrument" do
    subject(:instrument) do
      described_class.instrument("name", {}) do
        sentinel.call
      end
    end

    let(:sentinel) do
      Class.new do
        attr_reader :called

        def initialize
          @called = false
        end

        def call
          @called = true
        end
      end.new
    end

    it "requires block and passing name and optionally payload" do
      expect do
        instrument
      end.to change { sentinel.called }.from(false).to(true)
    end
  end
end
