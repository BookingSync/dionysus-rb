# frozen_string_literal: true

require "logger"
require "hermes"
require "sidekiq"
require "sidekiq-cron"
require "karafka"
require "dry-monitor"
require "concurrent/array"
require "file-based-healthcheck"
require "dionysus/version"
require "dionysus/topic_name"
require "dionysus/producer"
require "dionysus/producer/base_responder"
require "dionysus/producer/config"
require "dionysus/producer/genesis"
require "dionysus/producer/genesis/performed"
require "dionysus/producer/genesis/streamer"
require "dionysus/producer/genesis/streamer/base_job"
require "dionysus/producer/genesis/streamer/standard_job"
require "dionysus/producer/genesis/stream_job"
require "dionysus/producer/karafka_responder_generator"
require "dionysus/producer/key"
require "dionysus/producer/model_serializer"
require "dionysus/producer/deleted_record_serializer"
require "dionysus/producer/outbox"
require "dionysus/producer/outbox/active_record_publishable"
require "dionysus/producer/outbox/datadog_latency_reporter"
require "dionysus/producer/outbox/datadog_latency_reporter_job"
require "dionysus/producer/outbox/datadog_latency_reporter_scheduler"
require "dionysus/producer/outbox/datadog_tracer"
require "dionysus/producer/outbox/duplicates_filter"
require "dionysus/producer/outbox/event_name"
require "dionysus/producer/outbox/health_check"
require "dionysus/producer/outbox/latency_tracker"
require "dionysus/producer/outbox/model"
require "dionysus/producer/outbox/producer"
require "dionysus/producer/outbox/publishable"
require "dionysus/producer/outbox/publisher"
require "dionysus/producer/outbox/records_processor"
require "dionysus/producer/outbox/runner"
require "dionysus/producer/outbox/tombstone_publisher"
require "dionysus/producer/partition_key"
require "dionysus/producer/registry"
require "dionysus/producer/registry/validator"
require "dionysus/producer/serializer"
require "dionysus/producer/suppressor"
require "dionysus/consumer"
require "dionysus/consumer/config"
require "dionysus/consumer/karafka_consumer_generator"
require "dionysus/consumer/registry"
require "dionysus/consumer/deserializer"
require "dionysus/consumer/workers_group"
require "dionysus/consumer/dionysus_event"
require "dionysus/consumer/synced_data"
require "dionysus/consumer/synced_data/assign_columns_from_synced_data"
require "dionysus/consumer/synced_data/assign_columns_from_synced_data_job"
require "dionysus/consumer/synchronizable_model"
require "dionysus/consumer/persistor"
require "dionysus/consumer/params_batch_processor"
require "dionysus/consumer/params_batch_transformations"
require "dionysus/consumer/params_batch_transformations/remove_duplicates_strategy"
require "dionysus/consumer/batch_events_publisher"
require "dionysus/monitor"
require "dionysus/utils"
require "dionysus/utils/default_message_filter"
require "dionysus/utils/exponential_backoff"
require "dionysus/utils/null_instrumenter"
require "dionysus/utils/null_error_handler"
require "dionysus/utils/null_event_bus"
require "dionysus/utils/null_hermes_event_producer"
require "dionysus/utils/null_model_factory"
require "dionysus/utils/null_mutex_provider"
require "dionysus/utils/null_retry_provider"
require "dionysus/utils/null_transaction_provider"
require "dionysus/utils/null_lock_client"
require "dionysus/utils/null_tracer"
require "dionysus/utils/sidekiq_batched_job_distributor"
require "dionysus/checks"
require "dionysus/railtie" if defined?(Rails)
require "sigurd"
require "securerandom"

module Dionysus
  def self.initialize_application!(environment:, seed_brokers:, client_id:, logger:, draw_routing: true)
    ENV["KARAFKA_ENV"] = environment

    karafka_app = Class.new(Karafka::App) do
      setup do |config|
        config.kafka = {
          "bootstrap.servers": seed_brokers.join(","),
          "client.id": client_id
        }
        config.client_id = client_id
        config.logger = logger
        yield config if block_given?
      end
    end

    Object.const_set(:KarafkaApp, karafka_app)
    self.karafka_application = karafka_app
    evaluate_routing if consumer_registry.present? && draw_routing
  end

  def self.karafka_application=(karafka_app)
    @karafka_application = karafka_app
  end

  def self.karafka_application
    @karafka_application
  end

  def self.health_check=(health_check)
    @health_check = health_check

    Karafka.monitor.subscribe("app.initialized") do |_event|
      health_check = Dionysus.health_check

      health_check&.app_initialized!
    end

    Karafka.monitor.subscribe("statistics.emitted") do |_event|
      health_check = Dionysus.health_check

      health_check&.register_heartbeat
    end

    Karafka.monitor.subscribe("consumer.consumed") do |_event|
      health_check = Dionysus.health_check

      health_check&.register_heartbeat
    end

    Karafka.monitor.subscribe("app.stopped") do |_event|
      health_check = Dionysus.health_check

      health_check&.app_stopped!
    end
  end

  def self.health_check
    @health_check
  end

  def self.outbox_worker_health_check
    @outbox_worker_health_check ||= Dionysus::Producer::Outbox::HealthCheck.new
  end

  def self.enable_outbox_worker_healthcheck
    monitor.subscribe("outbox_producer.started") { outbox_worker_health_check.register_heartbeat }
    monitor.subscribe("outbox_producer.stopped") { outbox_worker_health_check.worker_stopped }
    monitor.subscribe("outbox_producer.heartbeat") { outbox_worker_health_check.register_heartbeat }
  end

  def self.logger
    if karafka_application
      karafka_application.config.logger
    else
      Logger.new($stdout)
    end
  end

  def self.consumer_registry
    @consumer_registry
  end

  def self.inject_routing!(registry)
    @consumer_registry = registry
  end

  def self.monitor
    @monitor ||= Dionysus::Monitor.new
  end

  def self.evaluate_routing
    consumer_group_name = "prometheus_consumer_group_for_#{karafka_application.config.client_id}"
    karafka_application.instance_exec(consumer_registry) do |registry|
      consumer_groups.draw do
        consumer_group consumer_group_name do
          registry.registrations.each do |_, registration|
            registration.topics.each do |topic|
              topic topic.to_s do
                consumer topic.consumer
                instance_eval(&topic.extensions_block) if topic.extensions_block
              end
            end
          end
        end
      end
    end
  end
  private_class_method :evaluate_routing
end
