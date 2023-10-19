# frozen_string_literal: true

class Dionysus::Producer::Outbox::Runner
  attr_reader :logger, :id, :config
  private     :logger, :config

  def initialize(config: Dionysus::Producer.configuration,
    logger: Dionysus.logger)
    @id = SecureRandom.uuid
    @logger = logger
    logger.push_tags("Dionysus::Producer::Outbox::Runners #{id}") if logger.respond_to?(:push_tags)
    @config = config
  end

  def start
    log("started")
    instrument("outbox_producer.started")
    @should_stop = false
    ensure_database_connection!
    loop do
      if @should_stop
        instrument("outbox_producer.shutting_down")
        log("shutting down")
        break
      end
      process_topics
      instrument("outbox_producer.heartbeat")
      sleep outbox_worker_sleep_seconds
    end
  rescue => e
    error_handler.capture_exception(e)
    log("error: #{e} #{e.message}")
    instrument("outbox_producer.error", error: e, error_message: e.message)
    raise e
  end

  def stop
    log("Outbox worker stopping")
    instrument("outbox_producer.stopped")
    @should_stop = true
  end

  private

  delegate :outbox_worker_sleep_seconds, :database_connection_provider, :outbox_model,
    :lock_client, :lock_expiry_time, :error_handler,
    :outbox_publishing_batch_size, to: :config

  delegate :pending_topics, to: :outbox_model
  delegate :lock, to: :lock_client

  def process_topics
    pending_topics.each do |topic|
      instrument("outbox_producer.processing_topic", topic: topic) do
        tracer.trace("outbox_producer", topic) do
          lock(lock_name(topic), lock_expiry_time) do |locked|
            if locked
              producer.call(topic, batch_size: outbox_publishing_batch_size) do |record|
                if record.failed?
                  instrument("outbox_producer.publishing_failed", outbox_record: record)
                  error("failed to publish #{record.inspect}")
                  error_handler.capture_exception(record.error)
                else
                  debug("published #{record.inspect}")
                  instrument("outbox_producer.published", outbox_record: record)
                end
              end
              instrument("outbox_producer.processed_topic", topic: topic)
            else
              debug("lock exists for #{topic} topic")
              instrument("outbox_producer.lock_exists_for_topic", topic: topic)
            end
          end
        end
      end
    end
  end

  def ensure_database_connection!
    database_connection_provider.connection.reconnect!
  end

  def log(message)
    logger.info("#{log_prefix} #{message}")
  end

  def debug(message)
    logger.debug("#{log_prefix} #{message}")
  end

  def error(message)
    logger.error("#{log_prefix} #{message}")
  end

  def lock_name(topic)
    "dionysus_#{topic}_lock"
  end

  def log_prefix
    "[Dionysus] Outbox worker"
  end

  def instrument(*args, **kwargs)
    Dionysus.monitor.instrument(*args, **kwargs) do
      yield if block_given?
    end
  end

  def tracer
    @tracer ||= if Object.const_defined?(:Datadog)
      Dionysus::Producer::Outbox::DatadogTracer.new
    else
      Dionysus::Utils::NullTracer
    end
  end

  def producer
    @producer ||= Dionysus::Producer::Outbox::Producer.new
  end
end
