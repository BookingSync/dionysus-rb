# frozen_string_literal: true

class Dionysus::Utils::NullTracer
  def self.trace(_event_name, _topic); end
end
