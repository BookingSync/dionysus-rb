# frozen_string_literal: true

require "spec_helper"

# reproduces v3_bookings booking 22624574: stamped .797910 carrying updated_at 12:03:54.023087,
# while siblings stamped .929308-53.854986 carried the older 12:03:52.997237 and won the group
RSpec.describe "serialized_at describes the attempt that was published" do # rubocop:disable RSpec/DescribeClass
  let(:topic_suffix) { SecureRandom.hex(4) }
  let(:topic_name) { "v102_stamp_#{topic_suffix}" }
  let(:responder) { Dionysus.const_get("#{topic_name.classify}Responder") }
  let(:strategy_klass) { Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy }
  let(:parent) { ExamplePublishableCancelableResource.create! }
  let(:event_name) { "example_publishable_cancelable_resource_updated" }
  let(:record_key) { "ExamplePublishableCancelableResource:#{parent.id}" }

  # loaded before the write below, so it is the competitor that legitimately carries older data
  let(:stale_object) { ExamplePublishableCancelableResource.find(parent.id) }

  # fires once during the contended publish's first serialization: publishes the competitor, which
  # stamps after it, then moves the row so the contended message has to re-serialize
  let(:interference) do
    fired = [false]
    competitor = -> { responder.call([[event_name, [stale_object]]], key: record_key, partition_key: "1") }
    parent_id = -> { parent.id }
    lambda do
      next if fired[0]

      fired[0] = true
      competitor.call
      Thread.new do
        ActiveRecord::Base.connection_pool.with_connection do
          ExamplePublishableCancelableResource.where(id: parent_id.call).update_all(updated_at: Time.current)
        end
      end.join
    end
  end

  before do
    hook = interference
    serializer = Class.new do
      define_singleton_method(:serialize) do |records, **|
        records.map do |record|
          updated_at = record.updated_at
          hook.call
          { id: record.id, updated_at: }
        end
      end
    end

    Dionysus::Producer.configure do |conf|
      conf.instrumenter = Class.new do
        define_singleton_method(:instrument) { |_name, _payload = {}, &block| block.call }
        define_singleton_method(:increment) { |_name, _options = {}| nil }
      end
      conf.outbox_model = DionysusOutbox
      conf.default_partition_key = :id
      conf.transaction_provider = ActiveRecord::Base
      conf.publish_consistent_snapshots = true
      conf.include_serialized_at_in_payload = true
    end

    topic_symbol = :"stamp_#{topic_suffix}"
    Dionysus::Producer.declare do
      namespace :v102 do
        serializer(serializer)
        topic topic_symbol do
          publish "ExamplePublishableCancelableResource"
        end
      end
    end

    Dionysus.initialize_application!(
      environment: "development",
      seed_brokers: ["localhost:9092"],
      client_id: "stamp_#{topic_suffix}",
      logger: Logger.new(File::NULL)
    )
    Karafka::Admin.create_topic(topic_name, 1, 1)

    parent
    stale_object
  end

  around do |example|
    original_producer = Karafka.producer
    ensure_real_karafka_producer
    Karafka.producer.config.deliver = true
    example.run
  ensure
    begin
      Karafka::Admin.delete_topic(topic_name)
    rescue => e
      warn "could not delete #{topic_name}: #{e.message}"
    end
    Karafka.producer.close
    Karafka.instance_variable_set(:@producer, nil)
    Karafka::App.setup { |config| config.producer = original_producer }
  end

  def publish_contended
    fresh = ExamplePublishableCancelableResource.find(parent.id)
    responder.call([[event_name, [fresh]]], key: record_key, partition_key: "1")
  end

  def published_events
    Karafka::Admin.read_topic(topic_name, 0, 10).map { |m| m.payload.fetch("message").first }
  end

  def surviving_event
    messages = Karafka::Messages::Messages.new(Karafka::Admin.read_topic(topic_name, 0, 10), double(:metadata))
    strategy_klass.new.call(messages).to_a.first.payload.fetch("message").first
  end

  it "never stamps a payload earlier than the data it carries" do
    publish_contended

    offenders = published_events.filter_map do |event|
      stamp = Time.parse(event.fetch("serialized_at"))
      data_at = Time.parse(event.fetch("data").first.fetch("updated_at"))
      # a timestamp cannot describe a moment after the one it was taken at
      [stamp, data_at, ((data_at - stamp) * 1000).round] if data_at > stamp
    end

    expect(offenders).to be_empty, "payloads whose updated_at postdates their serialized_at: #{offenders.inspect}"
  end

  it "lets the freshest payload win deduplication, not the one whose stamp was taken first" do
    publish_contended

    events = published_events
    freshest = events.map { |event| Time.parse(event.fetch("data").first.fetch("updated_at")) }.max
    survivor = Time.parse(surviving_event.fetch("data").first.fetch("updated_at"))

    aggregate_failures do
      expect(events.size).to eq 2
      expect(survivor).to eq freshest
    end
  end
end
