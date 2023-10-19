# frozen_string_literal: true

class Dionysus::Producer::Config
  attr_accessor :registry, :outbox_model, :database_connection_provider, :transaction_provider,
    :datadog_statsd_client

  attr_writer :instrumenter, :event_bus, :soft_delete_column, :default_partition_key, :outbox_worker_sleep_seconds,
    :lock_client, :lock_expiry_time, :error_handler, :outbox_publishing_batch_size,
    :transactional_outbox_enabled, :sidekiq_queue, :publisher_service_name,
    :genesis_consistency_safety_delay, :hermes_event_producer, :publish_after_commit, :outbox_worker_publishing_delay,
    :high_priority_sidekiq_queue, :observers_inline_maximum_size, :remove_consecutive_duplicates_before_publishing

  def self.default_sidekiq_queue
    :dionysus
  end

  def self.high_priority_sidekiq_queue
    :dionysus_high_priority
  end

  def instrumenter
    @instrumenter || Dionysus::Utils::NullInstrumenter
  end

  def event_bus
    @event_bus || Dionysus::Utils::NullEventBus
  end

  def soft_delete_column
    @soft_delete_column || "canceled_at"
  end

  def default_partition_key
    @default_partition_key || :account_id
  end

  def outbox_worker_sleep_seconds
    return BigDecimal("0.2") if @outbox_worker_sleep_seconds.nil?

    @outbox_worker_sleep_seconds.to_d
  end

  def lock_client
    @lock_client || Dionysus::Utils::NullLockClient
  end

  def lock_expiry_time
    @lock_expiry_time || 10_000
  end

  def error_handler
    @error_handler || Dionysus::Utils::NullErrorHandler
  end

  def outbox_publishing_batch_size
    @outbox_publishing_batch_size || 100
  end

  def transactional_outbox_enabled
    return @transactional_outbox_enabled if defined?(@transactional_outbox_enabled)

    true
  end

  def sidekiq_queue
    @sidekiq_queue || self.class.default_sidekiq_queue
  end

  def publisher_service_name
    @publisher_service_name || Karafka.producer.id
  end

  def genesis_consistency_safety_delay
    @genesis_consistency_safety_delay || 60.seconds
  end

  def hermes_event_producer
    @hermes_event_producer || Hermes::EventProducer
  end

  def publish_after_commit
    return @publish_after_commit if defined?(@publish_after_commit)

    false
  end

  def outbox_worker_publishing_delay
    (@outbox_worker_publishing_delay || 0).to_d.seconds
  end

  def high_priority_sidekiq_queue
    @high_priority_sidekiq_queue || self.class.high_priority_sidekiq_queue
  end

  def observers_inline_maximum_size
    @observers_inline_maximum_size || 1000
  end

  def remove_consecutive_duplicates_before_publishing
    return @remove_consecutive_duplicates_before_publishing if defined?(@remove_consecutive_duplicates_before_publishing)

    false
  end
end
