# frozen_string_literal: true

require "spec_helper"

# Reproduces, against a real broker, the shape measured on v3_bookings: a payload whose row
# timestamp is OLDER than the associated records embedded beside it.
#
# The production numbers this is built from (2026-08-27, 12:48-13:30 UTC):
#   serialized_at -> broker receipt: median 95ms, p95 489ms, max 3526ms
#   backwards jumps in updated_at  : median 266ms, max 2648ms
#
# The hypothesis: nothing is stale. A serializer reads the record's updated_at, then queries the
# associations, and anything committed in between lands in the payload beside a timestamp taken
# before it. No stale object, no query cache, no clock skew required - only the ordering inside
# serialization and enough time for a write to arrive during it.
RSpec.describe "Reproduction: a payload whose updated_at predates its own associations" do # rubocop:disable RSpec/DescribeClass
  let(:topic_suffix) { SecureRandom.hex(4) }
  let(:topic_name) { "v102_torn_#{topic_suffix}" }
  let(:responder) { Dionysus.const_get("#{topic_name.classify}Responder") }
  let(:parent) { ExamplePublishableCancelableResource.create! }

  # runs between the serializer reading updated_at and it querying the association, which is what a
  # concurrent request does during a serialization the production data says takes 95ms to 3.5s
  # fires during the FIRST serialization only, which is what transient contention looks like: a
  # request commits while a payload is being built, and is gone by the time it is built again
  let(:writes_remaining) { 1 }
  # How long the payload is under construction while the write lands. Production serialization spans
  # 95ms at the median, so hold the window open explicitly instead of letting it be whatever a thread
  # takes to check out a connection on the box running this - that incidental cost measured 37ms
  # locally and 5ms on CI, which turns the assertion below into a race against the hardware rather
  # than a statement about the payload.
  let(:serialization_window) { 0.05 }
  let(:write_during_serialization) do
    remaining = writes_remaining
    window = serialization_window
    lambda do
      next if remaining <= 0

      remaining -= 1
      sleep window
      Thread.new do
        ActiveRecord::Base.connection_pool.with_connection do
          ExampleResource.create!(account_id: 7, example_publishable_cancelable_resource_id: parent.id)
          ExamplePublishableCancelableResource.where(id: parent.id).update_all(updated_at: Time.current)
        end
      end.join
    end
  end

  let(:increments) { [] }
  let(:serializations) { [0] }

  before do
    hook = write_during_serialization
    tally = serializations
    serializer = Class.new do
      define_singleton_method(:serialize) do |records, **|
        tally[0] += 1
        records.map do |record|
          updated_at = record.updated_at            # the row timestamp, read FIRST
          hook.call                                 # a write arrives mid-serialization
          {
            id: record.id,
            updated_at:,
            children: record.example_resources.reload.map { |child| { id: child.id } }
          }
        end
      end
    end

    recorded = increments
    recording_instrumenter = Class.new do
      define_singleton_method(:instrument) { |_name, _payload = {}, &block| block.call }
      define_singleton_method(:increment) { |name, options = {}| recorded << [name, options.fetch(:tags, [])] }
    end

    Dionysus::Producer.configure do |conf|
      conf.instrumenter = recording_instrumenter
      conf.outbox_model = DionysusOutbox
      conf.default_partition_key = :id
      conf.transaction_provider = ActiveRecord::Base
      # global config, and the enabled context below flips it - reset it for every example so the
      # control cannot be silently disarmed by whatever ran before it
      conf.publish_consistent_snapshots = false
    end

    topic_symbol = :"torn_#{topic_suffix}"
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
      client_id: "torn_#{topic_suffix}",
      logger: Logger.new(File::NULL)
    )
    Karafka::Admin.create_topic(topic_name, 1, 1)

    parent
    ExampleResource.create!(account_id: 7, example_publishable_cancelable_resource_id: parent.id)
    ExamplePublishableCancelableResource.where(id: parent.id).update_all(updated_at: Time.current)
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

  def snapshot_tags
    increments.filter_map { |name, tags| tags if name == "dionysus.publish.consistent_snapshot" }
  end

  def published_event
    Karafka::Admin.read_topic(topic_name, 0, 10).map { |m| m.payload.fetch("message").first }.last
  end

  def publish_and_read
    fresh = ExamplePublishableCancelableResource.find(parent.id) # a FRESH object, nothing stale
    responder.call([["example_publishable_cancelable_resource_updated", [fresh]]],
      key: "ExamplePublishableCancelableResource:#{parent.id}", partition_key: "1")

    data = published_event.fetch("data").first
    [Time.parse(data.fetch("updated_at")), data.fetch("children").size]
  end

  context "when publish_consistent_snapshots is disabled" do
    it "emits a payload carrying data the row timestamp predates" do
      payload_updated_at, children = publish_and_read
      row_updated_at = ExamplePublishableCancelableResource.find(parent.id).updated_at

      # the payload embeds BOTH children while reporting a timestamp from before the second existed
      expect(children).to eq 2
      # with the guard off nothing is emitted, so an empty series means "not running" and can never
      # be mistaken for "running clean"
      expect(snapshot_tags).to be_empty
      # an order of magnitude below the window held open above, so this reads as "adrift", not as
      # the millisecond the payload's own timestamp precision could account for on its own
      expect(row_updated_at - payload_updated_at).to be > 0.01
    end
  end

  context "when publish_consistent_snapshots is enabled" do
    before do
      Dionysus::Producer.configure { |conf| conf.publish_consistent_snapshots = true }
    end

    it "re-serializes so the payload describes one moment" do
      payload_updated_at, children = publish_and_read
      row_updated_at = ExamplePublishableCancelableResource.find(parent.id).updated_at

      # the second serialization sees the write that arrived during the first one, and the payload's
      # timestamp now matches the row the data came from
      expect(children).to eq 2
      expect(serializations.first).to eq 2
      expect(snapshot_tags.last).to include("result:consistent", "attempts:2")
      # equal to the precision the payload is serialized at; the control above is a full window adrift
      expect((row_updated_at - payload_updated_at).abs).to be < 0.002
    end

    context "when the record keeps moving through every attempt" do
      let(:writes_remaining) { 99 }

      it "publishes the last attempt rather than looping" do
        expect { publish_and_read }.not_to raise_error
        expect(published_event.fetch("data").first).to include("updated_at")
      end

      it "counts the attempt it gave up on, the only trace that payload may still be torn" do
        publish_and_read

        expect(serializations.first).to eq 3
        expect(snapshot_tags.last).to include("result:exhausted", "attempts:3")
      end

      context "when max_snapshot_attempts is lowered" do
        before do
          Dionysus::Producer.configure { |conf| conf.max_snapshot_attempts = 2 }
        end

        it "stops re-serializing at the configured ceiling" do
          publish_and_read

          expect(serializations.first).to eq 2
          expect(snapshot_tags.last).to include("result:exhausted", "attempts:2")
        end
      end
    end
  end
end
