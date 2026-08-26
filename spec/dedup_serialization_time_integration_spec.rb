# frozen_string_literal: true

require "spec_helper"

# The unit specs rank hand-built payload hashes, which cannot say whether the producer writes the
# stamp where the consumer reads it or whether it survives a round trip - so this one publishes
# with the real responder and reads back off a real broker.
#
# The scenario is booking 22611119 on v3_bookings, offsets 28005577/28005579: the message published
# LAST carried the OLDER snapshot, and the booking was left reading paid_amount 0.
RSpec.describe "Integration scenario: a duplicate published last carrying an older snapshot" do
  let(:topic_suffix) { SecureRandom.hex(4) }
  let(:topic_name) { "v102_dedup_#{topic_suffix}" }
  let(:responder) { Dionysus.const_get("#{topic_name.classify}Responder") }
  let(:producer_config) { Dionysus::Producer.configuration }
  let(:strategy_klass) { Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy }

  let(:stamped_booking_id) { 22_611_119 }
  # published by a producer that has not been flipped yet - the state consumers roll out into
  let(:unstamped_booking_id) { 22_611_120 }

  let(:fresh_paid_amount) { "1447.0" }
  let(:fresh_read_at) { "2026-08-25T11:36:46.671830Z" }
  let(:stale_paid_amount) { "0.0" }
  let(:stale_read_at) { "2026-08-25T11:36:46.398194Z" }

  before do
    topic_symbol = :"dedup_#{topic_suffix}"
    booking_serializer = Class.new do
      def self.serialize(records, dependencies:)
        records.map do |record|
          { id: record.id, paid_amount: record.paid_amount, updated_at: record.updated_at }
        end
      end
    end

    Dionysus::Producer.declare do
      namespace :v102 do
        serializer(booking_serializer)

        topic topic_symbol do
          publish "Booking"
        end
      end
    end

    Dionysus::Consumer.declare do
      namespace :v102 do
        topic topic_symbol
      end
    end

    Dionysus::Consumer.configure do |config|
      config.transaction_provider = ActiveRecord::Base
      config.model_factory = ModelFactoryForKarafkaConsumerTest.new
    end

    Dionysus.initialize_application!(
      environment: "development",
      seed_brokers: ["localhost:9092"],
      client_id: "dedup_#{topic_suffix}",
      logger: Logger.new(File::NULL)
    )

    # after the setup above, so the admin client reads this spec's seed brokers rather than
    # whatever the previously loaded spec file left on the global config
    Karafka::Admin.create_topic(topic_name, 1, 1)
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
    # the delivering producer this spec swapped in is global, and specs loaded after it read
    # things off it - Dionysus/Prometheus derive the publisher service name from its id
    Karafka.producer.close
    Karafka.instance_variable_set(:@producer, nil)
    Karafka::App.setup { |config| config.producer = original_producer }
  end

  def publish_inverted_pair(booking_id:, stamped:)
    producer_config.include_serialized_at_in_payload = stamped

    # published first, read second - the payload the booking should end up with
    publish(booking_id:, paid_amount: fresh_paid_amount, read_at: fresh_read_at)
    # published second, read first - a higher offset carrying an older snapshot
    publish(booking_id:, paid_amount: stale_paid_amount, read_at: stale_read_at)
  end

  def publish(booking_id:, paid_amount:, read_at:)
    booking = double(:booking, id: booking_id, paid_amount:, updated_at: read_at,
      model_name: double(name: "Booking"), class: "Booking")

    # the stamp travels with the snapshot, not with the moment the message is published
    Timecop.freeze(Time.parse(read_at)) do
      responder.call([["booking_updated", [booking]]], key: "Booking:#{booking_id}", partition_key: "1")
    end
  end

  def published_events
    Karafka::Admin.read_topic(topic_name, 0, 10).map { |message| message.payload.fetch("message").first }
  end

  describe "reading the published messages back off the broker" do
    let(:surviving_event) do
      messages = Karafka::Messages::Messages.new(Karafka::Admin.read_topic(topic_name, 0, 10), double(:metadata))

      strategy_klass.new.call(messages).to_a.first.payload.fetch("message").first
    end

    context "when the producer stamps serialized_at" do
      before { publish_inverted_pair(booking_id: stamped_booking_id, stamped: true) }

      it "writes the stamp where the consumer reads it, in a format it accepts" do
        stamps = published_events.map { |event| event["serialized_at"] }

        expect(stamps).to eq([fresh_read_at, stale_read_at])
        expect(stamps).to all(match(strategy_klass::SERIALIZED_AT_FORMAT))
      end

      it "keeps the payload serialized last, not the one published last" do
        expect(surviving_event.fetch("data").first.fetch("paid_amount")).to eq(fresh_paid_amount)
      end
    end

    context "when the producer has not been flipped yet" do
      before { publish_inverted_pair(booking_id: unstamped_booking_id, stamped: false) }

      it "publishes the payload it published before the change" do
        expect(published_events.map(&:keys)).to all(eq(%w[event model_name data]))
      end

      it "falls back to the offset, so consumers can ship before producers" do
        expect(surviving_event.fetch("data").first.fetch("paid_amount")).to eq(stale_paid_amount)
      end
    end
  end

  describe "handing a real batch to the generated consumer" do
    before do
      # both pairs on one topic: four messages, two duplicate groups, one batch - the two rollout
      # states are answered by a single pass of the consumer
      publish_inverted_pair(booking_id: stamped_booking_id, stamped: true)
      publish_inverted_pair(booking_id: unstamped_booking_id, stamped: false)
    end

    it "persists the freshest snapshot, and leaves an unstamped topic behaving as it does today" do
      consume_whole_topic

      expect(DBForKarafkaConsumerTest.bookings.count).to eq(2)
      expect(paid_amount_for(stamped_booking_id)).to eq(fresh_paid_amount)
      expect(paid_amount_for(unstamped_booking_id)).to eq(stale_paid_amount)
    end

    def consume_whole_topic
      consumer = Dionysus.const_get("#{topic_name.classify}Consumer").new
      consumer.messages = Karafka::Messages::Messages.new(
        Karafka::Admin.read_topic(topic_name, 0, 10), double(:metadata)
      )
      consumer.consume
    end

    def paid_amount_for(booking_id)
      DBForKarafkaConsumerTest.bookings.find { |booking| booking.synced_id == booking_id }&.paid_amount
    end
  end
end
