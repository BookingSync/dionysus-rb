# frozen_string_literal: true

class Dionysus::Utils::NullErrorHandler
  def self.capture_exception(_error); end
  def self.capture_message(_error); end
end
