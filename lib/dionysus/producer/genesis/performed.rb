# frozen_string_literal: true

class Dionysus::Producer::Genesis::Performed < Hermes::BaseEvent
  attribute :model, Dry.Types::Strict::String
  attribute :service, Dry.Types::Strict::String
  attribute :topic, Dry.Types::Strict::String
  attribute :start_at, Dry.Types::Nominal::DateTime
  attribute :end_at, Dry.Types::Nominal::DateTime
end
