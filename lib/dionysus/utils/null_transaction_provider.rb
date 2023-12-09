# frozen_string_literal: true

class Dionysus::Utils::NullTransactionProvider
  def self.transaction
    yield
  end

  def self.connection_pool
    self
  end

  def self.with_connection
    yield
  end
end
