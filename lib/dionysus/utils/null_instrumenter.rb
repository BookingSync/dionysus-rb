# frozen_string_literal: true

class Dionysus::Utils::NullInstrumenter
  def self.instrument(_name, _payload = {})
    yield
  end

  def self.increment(_name, _options = {}); end
end
