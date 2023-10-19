# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Utils::NullMutexProvider do
  describe ".with_lock" do
    subject(:with_lock) do
      described_class.with_lock("name") do
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

    it "requires block and passing name and it yields the block" do
      expect do
        with_lock
      end.to change { sentinel.called }.from(false).to(true)
    end
  end
end
