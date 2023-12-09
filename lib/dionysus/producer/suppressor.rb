# frozen_string_literal: true

class Dionysus::Producer::Suppressor
  SUPPRESSION_KEY = :dionysus_producer_suppressed
  private_constant :SUPPRESSION_KEY

  def self.suppressed?
    Thread.current[SUPPRESSION_KEY] == true
  end

  def self.suppress!
    Thread.current[SUPPRESSION_KEY] = true
  end

  def self.unsuppress!
    Thread.current[SUPPRESSION_KEY] = nil
  end
end
