# frozen_string_literal: true

class Dionysus::Utils::NullLockClient
  def self.lock(resource_key, expiration_time)
    payload = {
      validity: expiration_time,
      resource: resource_key,
      value: "null_lock_client_lock"
    }

    yield payload
  end
end
