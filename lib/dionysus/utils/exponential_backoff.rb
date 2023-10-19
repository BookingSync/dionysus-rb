# frozen_string_literal: true

class Dionysus::Utils::ExponentialBackoff
  def self.backoff_for(multiplier, count)
    (multiplier * (2**count))
  end
end
