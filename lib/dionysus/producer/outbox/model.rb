# frozen_string_literal: true

module Dionysus::Producer::Outbox::Model
  extend ActiveSupport::Concern

  OBSERVER_TOPIC = "__outbox_observer__"
  CHANGESET_COLUMN = "changeset"
  private_constant :OBSERVER_TOPIC, :CHANGESET_COLUMN

  def self.observer_topic
    OBSERVER_TOPIC
  end

  included do
    scope :fetch_publishable, lambda { |batch_size, topic|
      outbox_worker_publishing_delay = Dionysus::Producer.configuration.outbox_worker_publishing_delay

      records = where(published_at: nil, topic: topic)
        .where("retry_at IS NULL OR retry_at <= ?", Time.current)
        .order(created_at: :asc)
        .limit(batch_size)
      if outbox_worker_publishing_delay > 0.seconds
        records = records.where("created_at <= ?", Time.current + outbox_worker_publishing_delay)
      end
      records
    }
    scope :published_since, ->(time) { where("published_at >= ?", time) }
    scope :not_published, -> { where(published_at: nil) }

    belongs_to :resource, polymorphic: true, foreign_type: :resource_class, optional: true

    def self.pending_topics
      not_published.select("DISTINCT topic").map(&:topic)
    end

    def self.handles_changeset?
      column_names.include?(CHANGESET_COLUMN)
    end

    def self.encrypts_changeset!
      define_method :changeset= do |payload|
        super(payload.to_json)
      end
    end
  end

  def observer?
    topic == Dionysus::Producer::Outbox::Model.observer_topic
  end

  def transformed_changeset
    return {} unless self.class.handles_changeset?

    if changeset.respond_to?(:to_hash)
      changeset.symbolize_keys
    else
      JSON.parse(changeset).symbolize_keys
    end
  end

  def published?
    published_at.present?
  end

  def failed?
    failed_at.present?
  end

  def handle_error(raised_error, clock: Time)
    @error = raised_error
    self.error_class = raised_error.class
    self.error_message = raised_error.message
    self.failed_at = clock.current
    self.attempts ||= 0
    self.attempts += 1
    self.retry_at = clock.current.advance(seconds: Dionysus::Utils::ExponentialBackoff.backoff_for(5,
      attempts))
  end

  def error
    if error_class_arity == 1 || error_class_arity == -1
      error_class.constantize.new(error_message)
    else
      StandardError.new("#{error_class_constant}: #{error_message}")
    end
  end

  def resource_created_at
    return created_at if resource.nil?

    resource.created_at
  end

  def publishing_latency
    return unless published?

    published_at - created_at
  end

  def created_event?
    event_name.to_s.end_with?("created")
  end

  def updated_event?
    event_name.to_s.end_with?("updated")
  end

  private

  def error_class_constant
    error_class.constantize
  end

  def error_class_arity
    error_class_constant.instance_method(:initialize).arity
  end
end
