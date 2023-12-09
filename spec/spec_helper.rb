# frozen_string_literal: true

require "bundler/setup"
require "dionysus-rb"
require "active_record"
require "timecop"
require "byebug"
require "support/stubbed_active_record_interface_and_models"
require "support/is_expected_block"
require "hermes/support/matchers/publish_async_message"
require "sidekiq/testing"
require "rspec-sidekiq"
require "ddtrace"
require "sentry-ruby"
require "crypt_keeper"
require "shoulda-matchers"
require "datadog/statsd"
require "hermes"

ENV["KARAFKA_ENV"] ||= "test"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:all) do
    Time.zone = "UTC"
  end

  config.before(:all) do
    ENV["REDIS_URL"] ||= "redis://localhost:6379/1"
  end

  config.before do
    Dionysus::Producer.reset!
    Dionysus::Consumer.reset!
    if Object.const_defined?(:KarafkaApp)
      Object.send(:remove_const, :KarafkaApp)
    end
    Dionysus.karafka_application = nil
  end

  config.after do
    DBForKarafkaConsumerTest.reset!
    Karafka::App.routes.clear
  end

  config.before(:example, :with_outbox_config) do
    Dionysus::Producer.configure do |conf|
      conf.outbox_model = DionysusOutbox
      conf.transaction_provider = ActiveRecord::Base
    end
  end

  config.around(:example, :with_kafka_delivery_enabled) do |example|
    Karafka.producer.setup do |c|
      c.deliver = true
      c.logger = Logger.new($stdout)
    end

    example.run

    Karafka.producer.setup do |c|
      c.deliver = false
    end
  end

  config.before(:example, :publish_after_commit) do
    allow(Dionysus::Producer.configuration).to receive(:publish_after_commit).and_return(true)
  end

  config.around(:example, :freeze_time) do |example|
    freeze_time = example.metadata[:freeze_time]
    time_now = freeze_time == true ? Time.current.round : freeze_time
    Timecop.freeze(time_now) { example.run }
  end

  Karafka::App.setup do |config|
    config.producer = ::WaterDrop::Producer.new do |producer_config|
      producer_config.deliver = false
      producer_config.kafka = {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': 1
      }
      producer_config.id = :"dionysus".to_s
    end
    config.initial_offset = "latest"
  end

  Karafka.monitor.subscribe(Dionysus::Utils::KarafkaSentryListener)
  Karafka.monitor.subscribe(Dionysus::Utils::KarafkaDatadogListener)

  include IsExpectedBlock

  RSpec::Matchers.define_negated_matcher :avoid_changing, :change

  database_name = ENV.fetch("DATABASE_NAME", "dionysus-test")
  if (posgres_user = ENV["POSTGRES_USER"]) && (postgres_password = ENV["POSTGRES_PASSWORD"])
    database_url = ENV.fetch("DATABASE_URL", "postgres://#{posgres_user}:#{postgres_password}@localhost/#{database_name}")
    postgres_url = ENV.fetch("POSTGRES_URL", "postgres://#{posgres_user}:#{postgres_password}@localhost")
  else
    database_url = ENV.fetch("DATABASE_URL", "postgres://localhost/#{database_name}")
    postgres_url = ENV.fetch("POSTGRES_URL", "postgres://localhost")
  end



  ActiveRecord::Base.establish_connection(database_url)
  begin
    database = ActiveRecord::Base.connection
  rescue ActiveRecord::NoDatabaseError, ActiveRecord::ConnectionNotEstablished
    ActiveRecord::Base.establish_connection(postgres_url).connection.create_database(database_name)
    ActiveRecord::Base.establish_connection(database_url)
    database = ActiveRecord::Base.connection
  end

  database.drop_table(:dionysus_outboxes) if database.table_exists?(:dionysus_outboxes)
  database.create_table(:dionysus_outboxes) do |t|
    t.string "resource_class", null: false
    t.string "resource_id", null: false
    t.string "event_name", null: false
    t.string "topic", null: false
    t.string "partition_key"
    t.datetime "published_at"
    t.jsonb "changeset", null: false, default: {}
    t.datetime "failed_at"
    t.datetime "retry_at"
    t.string "error_class"
    t.string "error_message"
    t.integer "attempts", null: false, default: 0
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false

    t.index %w[resource_class event_name], name: "index_dionysus_outboxes_on_resource_class_and_event"
    t.index %w[resource_class resource_id], name: "index_dionysus_outboxes_on_resource_class_and_resource_id"
    t.index ["topic"], name: "index_dionysus_outboxes_on_topic"
    t.index ["created_at"], name: "index_dionysus_outboxes_on_created_at"
    t.index %w[resource_class created_at], name: "index_dionysus_outboxes_on_resource_class_and_created_at"
    t.index %w[resource_class published_at], name: "index_dionysus_outboxes_on_resource_class_and_published_at"
  end

  database.drop_table(:dionysus_outbox_encr_changesets) if database.table_exists?(:dionysus_outbox_encr_changesets)
  database.create_table(:dionysus_outbox_encr_changesets) do |t|
    t.string "resource_class", null: false
    t.string "resource_id", null: false
    t.string "event_name", null: false
    t.string "topic", null: false
    t.string "partition_key"
    t.datetime "published_at"
    t.text "changeset"
    t.datetime "failed_at"
    t.datetime "retry_at"
    t.string "error_class"
    t.string "error_message"
    t.integer "attempts", null: false, default: 0
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false

    t.index %w[resource_class event_name], name: "index_dionysus_outbox_encr_chsts_on_r_class_and_event"
    t.index %w[resource_class resource_id], name: "index_prnmths_outbox_encr_chsts_on_r_class_and_resource_id"
    t.index ["topic"], name: "index_dionysus_outbox_encr_chsts_on_topic"
    t.index ["created_at"], name: "index_dionysus_outbox_encr_chsts_on_created_at"
    t.index %w[resource_class created_at], name: "index_prnmths_outbox_encr_chsts_on_r_class_and_created_at"
    t.index %w[resource_class published_at], name: "index_prnmths_outbox_encr_chsts_on_r_class_and_published_at"
  end

  if database.table_exists?(:dionysus_outbox_without_changesets)
    database.drop_table(:dionysus_outbox_without_changesets)
  end
  database.create_table(:dionysus_outbox_without_changesets) do |t|
    t.string "resource_class", null: false
    t.string "resource_id", null: false
    t.string "event_name", null: false
    t.string "topic", null: false
    t.string "partition_key"
    t.datetime "published_at"
    t.datetime "failed_at"
    t.datetime "retry_at"
    t.string "error_class"
    t.string "error_message"
    t.integer "attempts", null: false, default: 0
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false

    t.index %w[resource_class event_name], name: "index_pmths_outbox_wo_chset_on_resource_class_and_event"
    t.index %w[resource_class resource_id], name: "index_pmths_outbox_wo_chset_on_resource_class_and_resource_id"
    t.index ["topic"], name: "index_pmths_outbox_wo_chset_on_topic"
    t.index ["created_at"], name: "index_pmths_outbox_wo_chset_on_created_at"
    t.index %w[resource_class created_at], name: "index_pmths_outbox_wo_chset_on_resource_class_and_created_at"
    t.index %w[resource_class published_at], name: "index_pmths_outbox_wo_chset_on_resource_class_and_published_at"
  end

  database.drop_table(:example_resources) if database.table_exists?(:example_resources)
  database.create_table(:example_resources) do |t|
    t.integer "account_id"
    t.integer "example_publishable_cancelable_resource_id"
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false
  end

  database.drop_table(:example_publishable_resources) if database.table_exists?(:example_publishable_resources)
  database.create_table(:example_publishable_resources) do |t|
    t.integer "account_id"
    t.integer "example_resource_id"
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false
  end

  if database.table_exists?(:example_publishable_cancelable_resources)
    database.drop_table(:example_publishable_cancelable_resources)
  end
  database.create_table(:example_publishable_cancelable_resources) do |t|
    t.integer "account_id"
    t.datetime "canceled_at"
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false
  end

  if database.table_exists?(:example_cancelable_resources)
    database.drop_table(:example_cancelable_resources)
  end
  database.create_table(:example_cancelable_resources) do |t|
    t.integer "account_id"
    t.datetime "canceled_at"
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false
  end

  if database.table_exists?(:example_publishable_publish_after_soft_deletion_resources)
    database.drop_table(:example_publishable_publish_after_soft_deletion_resources)
  end
  database.create_table(:example_publishable_publish_after_soft_deletion_resources) do |t|
    t.integer "account_id"
    t.datetime "canceled_at"
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false
  end

  database.drop_table(:syncables) if database.table_exists?(:syncables)
  database.create_table(:syncables) do |t|
    t.string "name"
    t.jsonb "synced_data", default: {}, null: false
    t.datetime "canceled_at"
    t.datetime "created_at", precision: 6, null: false
    t.datetime "updated_at", precision: 6, null: false
  end
  database.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

  class DionysusOutbox < ActiveRecord::Base
    include Dionysus::Producer::Outbox::Model
  end

  class DionysusOutboxEncrChangeset < ActiveRecord::Base
    include Dionysus::Producer::Outbox::Model

    crypt_keeper :changeset, encryptor: :postgres_pgp, key: "secret_key", encoding: "UTF-8"

    encrypts_changeset!
  end

  class DionysusOutboxWithoutChangeset < ActiveRecord::Base
    include Dionysus::Producer::Outbox::Model
  end

  class ExampleResource < ActiveRecord::Base
    has_one :example_publishable_resource
  end

  class ExamplePublishableResource < ActiveRecord::Base
    include Dionysus::Producer::Outbox::ActiveRecordPublishable

    belongs_to :example_resource

    def name
      "name"
    end
  end

  class ExamplePublishableCancelableResource < ActiveRecord::Base
    include Dionysus::Producer::Outbox::ActiveRecordPublishable

    has_many :example_resources
  end

  class ExampleCancelableResource < ActiveRecord::Base
  end

  class ExamplePublishablePublishAfterSoftDeletionResource < ActiveRecord::Base
    include Dionysus::Producer::Outbox::ActiveRecordPublishable

    def dionysus_publish_updates_after_soft_delete?
      true
    end
  end

  class Syncable < ActiveRecord::Base
  end

  class ErrorClassWithArityOne < StandardError
    attr_reader :messsage
    private     :message

    def initialize(message)
      @message = message
    end

    def to_s
      message
    end
  end

  class ErrorClassWithArityTwo < StandardError
    attr_reader :messsage
    private     :message

    def initialize(message, _whatever)
      @message = message
    end

    def to_s
      message
    end
  end

  Shoulda::Matchers.configure do |config|
    config.integrate do |with|
      with.test_framework :rspec

      with.library :active_record
    end
  end

  def ensure_real_karafka_producer
    Karafka.instance_variable_set("@producer", nil)
    Karafka::App.setup do |config|
      config.producer = ::WaterDrop::Producer.new do |producer_config|
        producer_config.deliver = true
        producer_config.kafka = {
          'bootstrap.servers': 'localhost:9092',
          'request.required.acks': 1
        }
      end
      config.initial_offset = "latest"
    end
  end
end
