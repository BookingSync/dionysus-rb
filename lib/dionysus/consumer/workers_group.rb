# frozen_string_literal: true

class Dionysus::Consumer::WorkersGroup
  attr_reader :workers
  private :workers

  def initialize
    @workers = []
  end

  def <<(worker)
    workers << worker
  end

  def work
    workers.map(&:join)
  end
end
