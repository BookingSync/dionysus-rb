# frozen_string_literal: true

class Dionysus::Checks::HealthCheck
  KEY_PREFIX = "__karafka_app_running__"
  TMP_DIR = "/tmp"
  KARAFKA_TIMEOUT = "KARAFKA_HEALTH_CHECK_TIMEOUT"
  private_constant :KEY_PREFIX, :TMP_DIR, :KARAFKA_TIMEOUT

  def self.check(hostname: ENV.fetch("HOSTNAME", nil), expiry_time_in_seconds: ENV.fetch(KARAFKA_TIMEOUT, 600))
    new(hostname: hostname, expiry_time_in_seconds: expiry_time_in_seconds).check
  end

  attr_reader :hostname, :expiry_time_in_seconds

  def initialize(hostname: ENV.fetch("HOSTNAME", nil), expiry_time_in_seconds: ENV.fetch(KARAFKA_TIMEOUT, 600))
    @hostname = hostname
    @expiry_time_in_seconds = expiry_time_in_seconds
  end

  def check
    if healthcheck_storage.running?
      ""
    else
      "[Dionysus healthcheck failed]"
    end
  end

  def app_initialized!
    register_heartbeat
  end

  def register_heartbeat
    healthcheck_storage.touch
  end

  def app_stopped!
    healthcheck_storage.remove
  end

  private

  def healthcheck_storage
    @healthcheck_storage ||= FileBasedHealthcheck.new(directory: TMP_DIR, filename: key,
      time_threshold: expiry_time_in_seconds)
  end

  def key
    "#{KEY_PREFIX}#{hostname}"
  end
end
