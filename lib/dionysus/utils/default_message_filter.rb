# frozen_string_literal: true

class Dionysus::Utils::DefaultMessageFilter
  attr_reader :error_handler
  private     :error_handler

  def initialize(error_handler:)
    @error_handler = error_handler
  end

  def ignore_message?(topic:, message:, transformed_data:)
    false
  end

  def notify_about_ignored_message(topic:, message:, transformed_data:)
    error_handler.capture_message(error_message(topic, message, transformed_data))
  end

  private

  def error_message(topic, message, transformed_data)
    "Ignoring Kafka message. Make sure it's processed later (e.g. by directly doing it from console): " \
      "topic: #{topic}, message: #{message}, transformed_data: #{transformed_data}."
  end
end
