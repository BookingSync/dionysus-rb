# frozen_string_literal: true

class Dionysus::Consumer::Config
  attr_accessor :registry
  attr_writer :transaction_provider, :model_factory, :processing_mutex_provider, :processing_mutex_method_name,
    :instrumenter, :event_bus, :soft_delete_strategy, :soft_deleted_at_timestamp_attribute,
    :synced_created_at_timestamp_attribute, :synced_updated_at_timestamp_attribute, :synced_id_attribute,
    :synced_data_attribute, :consumer_base_class, :retry_provider, :resolve_synced_data_hash_proc, :sidekiq_queue,
    :message_filters

  def self.default_sidekiq_queue
    :dionysus
  end

  def transaction_provider
    @transaction_provider || Dionysus::Utils::NullTransactionProvider
  end

  def model_factory
    @model_factory || Dionysus::Utils::NullModelFactory
  end

  def processing_mutex_provider
    @processing_mutex_provider || Dionysus::Utils::NullMutexProvider
  end

  def processing_mutex_method_name
    @processing_mutex_method_name || :with_lock
  end

  def instrumenter
    @instrumenter || Dionysus::Utils::NullInstrumenter
  end

  def event_bus
    @event_bus || Dionysus::Utils::NullEventBus
  end

  def soft_delete_strategy
    @soft_delete_strategy || :cancel
  end

  def soft_deleted_at_timestamp_attribute
    @soft_deleted_at_timestamp_attribute || :synced_canceled_at
  end

  def synced_created_at_timestamp_attribute
    @synced_created_at_timestamp_attribute || :synced_created_at
  end

  def synced_updated_at_timestamp_attribute
    @synced_updated_at_timestamp_attribute || :synced_updated_at
  end

  def synced_id_attribute
    @synced_id_attribute || :synced_id
  end

  def synced_data_attribute
    @synced_data_attribute || :synced_data
  end

  def consumer_base_class
    @consumer_base_class || Karafka::BaseConsumer
  end

  def retry_provider
    @retry_provider || Dionysus::Utils::NullRetryProvider
  end

  def add_attributes_mapping_for_model(model_name)
    attributes_mapping_for_models[model_name.to_s] = yield
  end

  def attributes_mapping_for_models
    @attributes_mapping_for_models ||= {}
  end

  def attributes_mapping_for_model(model_name)
    attributes_mapping_for_models.fetch(model_name.to_s, {})
  end

  def resolve_synced_data_hash_proc
    @resolve_synced_data_hash_proc || lambda do |record|
      record.public_send(Dionysus::Consumer.configuration.synced_data_attribute).to_h
    end
  end

  def sidekiq_queue
    @sidekiq_queue || self.class.default_sidekiq_queue
  end

  def message_filter
    @message_filter || Dionysus::Utils::DefaultMessageFilter.new(error_handler:
      Dionysus::Utils::NullErrorHandler)
  end

  def message_filter=(val)
    @message_filter = val
    self.message_filters = [val]
  end

  def message_filters
    @message_filters || Array.wrap(message_filter)
  end
end
