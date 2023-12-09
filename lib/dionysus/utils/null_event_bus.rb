# frozen_string_literal: true

class Dionysus::Utils::NullEventBus
  def self.publish(_name, _payload); end
end
