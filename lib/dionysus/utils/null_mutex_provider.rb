# frozen_string_literal: true

class Dionysus::Utils::NullMutexProvider
  def self.with_lock(_name)
    yield
  end
end
