# frozen_string_literal: true

require "active_support/concern"

module Dionysus::Producer::Outbox::ActiveRecordPublishable
  extend ActiveSupport::Concern

  OUTBOX_RECORDS_TO_PUBLISH_STORAGE_KEY = :bookingsync_outbox_records_to_publish
  private_constant :OUTBOX_RECORDS_TO_PUBLISH_STORAGE_KEY

  included do
    after_create :dionysus_insert_model_created
    after_update :dionysus_insert_model_updated
    after_destroy :dionysus_insert_model_destroy
    after_commit :publish_outbox_records

    def self.outbox_records_to_publish
      Thread.current[OUTBOX_RECORDS_TO_PUBLISH_STORAGE_KEY] ||= Concurrent::Array.new
    end

    def self.add_outbox_records_to_publish(records)
      outbox_records_to_publish.concat(records)
    end

    def self.clear_records_to_publish
      Thread.current[OUTBOX_RECORDS_TO_PUBLISH_STORAGE_KEY] = Concurrent::Array.new
    end
  end

  def dionysus_publish_updates_after_soft_delete?
    false
  end

  private

  def dionysus_insert_model_created
    add_outbox_records_to_publish(Dionysus::Producer.outbox.insert_created(self))
  end

  def dionysus_insert_model_updated
    add_outbox_records_to_publish(Dionysus::Producer.outbox.insert_updated(self))
  end

  def dionysus_insert_model_destroy
    add_outbox_records_to_publish(Dionysus::Producer.outbox.insert_destroyed(self))
  end

  def add_outbox_records_to_publish(records)
    self.class.add_outbox_records_to_publish(records)
  end

  def publish_outbox_records
    begin
      if publish_after_commit?
        records_processor.call(
          self.class.outbox_records_to_publish.sort_by(&:resource_created_at)
        )
      end
    rescue => e
      Dionysus.logger.error(
        "[Dionysus Outbox from publish_outbox_records] #{e.class}: #{e}"
      )
    end
    self.class.clear_records_to_publish
  end

  def records_processor
    Dionysus::Producer::Outbox::RecordsProcessor.new
  end

  def publish_after_commit?
    Dionysus::Producer.configuration.publish_after_commit
  end
end
