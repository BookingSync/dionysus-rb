# frozen_string_literal: true

require "logger"
require "sidekiq"
require "sidekiq-cron"
require "karafka"
require "dry-monitor"
require "concurrent/array"
require "file-based-healthcheck"
require "sigurd"
require "securerandom"
require "zeitwerk"

module Dionysus
  CONSUMER_GROUP_PREFIX = "dionysus_consumer_group_for"

  def self.loader
    @loader ||= Zeitwerk::Loader.for_gem.tap do |loader|
      loader.ignore("#{__dir__}/dionysus-rb.rb")
    end
  end

  def self.initialize_application!(environment:, seed_brokers:, client_id:, logger:, draw_routing: true, consumer_group_prefix: CONSUMER_GROUP_PREFIX)
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
    evaluate_routing(consumer_group_prefix: consumer_group_prefix) if consumer_registry.present? && draw_routing
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

  def self.evaluate_routing(consumer_group_prefix:)
    consumer_group_name = "#{consumer_group_prefix}_#{karafka_application.config.client_id}"
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

Dionysus.loader.setup
