# frozen_string_literal: true

class Dionysus::Producer::Config
  attr_accessor :registry, :outbox_model, :database_connection_provider, :transaction_provider,
    :datadog_statsd_client

  attr_writer :instrumenter, :event_bus, :soft_delete_column, :default_partition_key, :outbox_worker_sleep_seconds,
    :lock_client, :lock_expiry_time, :error_handler, :outbox_publishing_batch_size,
    :transactional_outbox_enabled, :sidekiq_queue, :publisher_service_name,
    :genesis_consistency_safety_delay, :hermes_event_producer, :publish_after_commit, :outbox_worker_publishing_delay,
    :high_priority_sidekiq_queue, :observers_inline_maximum_size, :remove_consecutive_duplicates_before_publishing,
    :include_serialized_at_in_payload, :publish_with_uncached_reads, :publish_consistent_snapshots,
    :max_snapshot_attempts, :republish_deduplicated_records, :republish_deduplicated_records_delay

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
    @hermes_event_producer || Dionysus::Utils::NullHermesEventProducer
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

  # Off by default so it can be switched on per producer, and switched back off in one env change
  # if the extra reads ever cost more than they are worth.
  # How many times a payload may be re-serialized before the last attempt is published as it stands.
  # This is the expensive half of publish_consistent_snapshots: a record written faster than it
  # serializes fails the check on every attempt, so it pays the full serialization cost this many
  # times over and still publishes a payload that may be torn. Lower it to bound that cost; 1
  # disables re-serialization while leaving the check (and its metric) in place.
  def max_snapshot_attempts
    @max_snapshot_attempts || Dionysus::Producer::MAX_SNAPSHOT_ATTEMPTS
  end

  def publish_with_uncached_reads
    return @publish_with_uncached_reads if defined?(@publish_with_uncached_reads)

    false
  end

  # Re-serialize when a record moved while its payload was being built, so the payload describes a
  # single moment. Off by default; the extra read costs one indexed column per message, and the
  # re-serialization only happens on the records that were actually contended.
  def publish_consistent_snapshots
    return @publish_consistent_snapshots if defined?(@publish_consistent_snapshots)

    false
  end

  # Off by default so consumers, which fall back to the offset when the field is absent, can be
  # rolled out first.
  def include_serialized_at_in_payload
    return @include_serialized_at_in_payload if defined?(@include_serialized_at_in_payload)

    false
  end

  def remove_consecutive_duplicates_before_publishing
    return @remove_consecutive_duplicates_before_publishing if defined?(@remove_consecutive_duplicates_before_publishing)

    false
  end

  # Publish the survivor of a collapsed run of duplicates once more, after a delay, so a payload
  # that may have been serialized across a write is followed by one read after the writes settled.
  # Off by default; does nothing unless remove_consecutive_duplicates_before_publishing is also on.
  def republish_deduplicated_records
    return @republish_deduplicated_records if defined?(@republish_deduplicated_records)

    false
  end

  # Has to outlast the burst of writes that produced the duplicates, or the republish is serialized
  # inside the same contended window it exists to escape.
  def republish_deduplicated_records_delay
    (@republish_deduplicated_records_delay || 30).to_d.seconds
  end
end
