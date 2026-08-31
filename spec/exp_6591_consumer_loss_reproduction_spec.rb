# frozen_string_literal: true

require "spec_helper"

# Reproduces the two money losses reconciled on v3_bookings on 2026-08-31, at the level a fix has to
# work at: a whole batch through the generated consumer, deduplication included, with the payment
# embedded in the booking payload as a has_many - which is what takes the children down with the parent.
#
# Case A - booking 22636750 (0DH6MM), producer-side. Every money-bearing message carried an updated_at
#   44ms OLDER than the zero-money payload the projection had already accepted.
# Case B - booking 22636772 (0DH6N8), consumer-side. Two messages carrying the money at a timestamp the
#   guard accepts were discarded by dedup, which keeps the highest [serialized_at, offset].
module Exp6591
  module DB
    class << self
      def bookings
        @bookings ||= []
      end

      def payments
        @payments ||= []
      end

      def clients
        @clients ||= []
      end

      def reset!
        @bookings = []
        @payments = []
        @clients = []
      end
    end
  end

  class Booking < BaseModelClassForKarafkaConsumerTest
    include ModelAttributesForKarafkaConsumerTest

    attr_accessor :synced_canceled_at, :paid_amount
    attr_reader :to_many_resolutions, :to_one_resolutions

    def initialize(attributes = {})
      @to_many_resolutions = []
      @to_one_resolutions = []
      super
    end

    def model_name
      "Booking"
    end

    # mirrors bsa-notifications Booking#resolve_to_many_association: update_all stamps the cancel
    # column and leaves synced_updated_at untouched
    def resolve_to_many_association(relation_name, synced_ids)
      to_many_resolutions << [relation_name, synced_ids]
      DB.payments
        .select { |payment| payment.synced_booking_id == synced_id && payment.synced_canceled_at.nil? }
        .reject { |payment| synced_ids.include?(payment.synced_id) }
        .each { |payment| payment.synced_canceled_at = Time.now.utc }
    end

    def resolve_to_one_association(relation_name, resolved_id)
      to_one_resolutions << [relation_name, resolved_id]
    end
  end

  class BookingsPayment < BaseModelClassForKarafkaConsumerTest
    include ModelAttributesForKarafkaConsumerTest

    attr_accessor :synced_canceled_at, :synced_booking_id, :amount

    def model_name
      "BookingsPayment"
    end
  end

  class Repository
    def initialize(model_klass, collection)
      @model_klass = model_klass
      @collection = collection
    end

    def find_or_initialize_by(attributes)
      @collection.find { |record| attributes.all? { |name, value| record.public_send(name) == value } } ||
        @model_klass.new(attributes).tap { |record| @collection << record }
    end
  end

  class Client < BaseModelClassForKarafkaConsumerTest
    include ModelAttributesForKarafkaConsumerTest

    attr_accessor :synced_canceled_at, :fullname

    def model_name
      "Client"
    end
  end

  class ModelFactory
    def for_model(model_name)
      case model_name.to_s.singularize.classify
      when "Booking" then Repository.new(Booking, DB.bookings)
      when "BookingsPayment" then Repository.new(BookingsPayment, DB.payments)
      when "Client" then Repository.new(Client, DB.clients)
      end
    end
  end
end

RSpec.describe "EXP-6591: money the consumer discarded" do # rubocop:disable RSpec/DescribeClass
  subject(:consume) { consumer.consume }

  let(:topic) do
    Dionysus::Consumer::Registry::Registration::Topic.new(:v3, "bookings", nil, topic_options)
  end
  let(:topic_options) { {} }
  # what a consuming app can already write in its own Consumer.declare block, with no gem change
  let(:deduplication_disabled) { { params_batch_transformation: nil } }
  let(:config) do
    Dionysus::Consumer::Config.new.tap do |current_config|
      current_config.model_factory = Exp6591::ModelFactory.new
      current_config.transaction_provider = transaction_provider
      current_config.processing_mutex_provider = processing_mutex_provider
      current_config.processing_mutex_method_name = :lock_with
    end
  end
  let(:transaction_provider) do
    Class.new do
      def transaction
        yield
      end

      def connection_pool
        self
      end

      def with_connection
        yield
      end
    end.new
  end
  let(:processing_mutex_provider) do
    Class.new do
      def lock_with(_lock_name)
        yield
      end
    end.new
  end
  let(:consumer_klass) { Dionysus::Consumer::KarafkaConsumerGenerator.new.generate(config, topic) }
  let(:consumer) { consumer_klass.new }
  let(:deserializers) do
    Karafka::Routing::Features::Deserializers::Config.new(
      payload: :raw_payload.to_proc,
      key: :raw_key.to_proc
    )
  end

  before do
    Exp6591::DB.reset!
    allow(consumer).to receive(:messages).and_return(batch)
  end

  def build_batch(messages)
    Karafka::Messages::Messages.new(messages, double(:batch_metadata))
  end

  def build_message(payload, offset, booking_id)
    metadata = Karafka::Messages::Metadata.new(
      offset:, deserializers:, raw_key: "Booking:#{booking_id}"
    )
    Karafka::Messages::Message.new(payload, metadata)
  end

  def booking_payload(booking_id:, updated_at:, paid_amount:, serialized_at:, embedded: {})
    payments = embedded.fetch(:payments, [])
    client = embedded[:client]
    links = { "bookings_payments" => payments.map { |payment| payment.fetch("id") } }
    links["client"] = client.fetch("id") if client
    record = {
      "id" => booking_id,
      "created_at" => "2026-08-31T09:00:00.000000Z",
      "updated_at" => updated_at,
      "canceled_at" => nil,
      "paid_amount" => paid_amount,
      "links" => links,
      "bookings_payments" => payments
    }
    record["client"] = client if client
    event = { "event" => "booking_updated", "model_name" => "Booking", "data" => [record] }
    event["serialized_at"] = serialized_at if serialized_at
    { "message" => [event] }
  end

  def payment_payload(payment_id:, booking_id:, amount:, created_at:, updated_at:)
    {
      "id" => payment_id,
      "created_at" => created_at,
      "updated_at" => updated_at,
      "canceled_at" => nil,
      "amount" => amount,
      "links" => { "booking" => booking_id }
    }
  end

  def stored_booking(booking_id)
    Exp6591::DB.bookings.find { |booking| booking.synced_id == booking_id }
  end

  # traversing children under a rejected parent is also the only new exposure: the purge stamps
  # synced_canceled_at without bumping synced_updated_at, so a tie is how a cancelled child comes back
  describe "Children reached through a parent the guard rejected" do
    let(:booking_id) { 22_636_800 }
    let(:payment_id) { 15_394_500 }
    let(:client_id) { 991_100 }
    let(:projection_synced_at) { Time.parse("2026-08-31T13:20:00.000000Z") }
    let(:stale_updated_at) { "2026-08-31T13:19:59.000000Z" }
    let(:child_synced_at) { "2026-08-31T13:19:50.000000Z" }
    let(:child_updated_at) { child_synced_at }
    let(:purged_at) { Time.parse("2026-08-31T13:20:05.000000Z") }
    let(:batch) do
      build_batch(
        [
          build_message(
            booking_payload(
              booking_id:, updated_at: stale_updated_at, paid_amount: "500.0",
              serialized_at: "2026-08-31T13:20:10.000000Z",
              embedded: {
                payments: [payment_payload(payment_id:, booking_id:, amount: "500.0",
                  created_at: child_synced_at, updated_at: child_updated_at)],
                client: { "id" => client_id, "fullname" => "Rich Piana", "links" => {} }
              }
            ),
            55_200_001, booking_id
          )
        ]
      )
    end

    before do
      Exp6591::DB.bookings << Exp6591::Booking.new(
        synced_id: booking_id, synced_updated_at: projection_synced_at, paid_amount: "0.0"
      )
      Exp6591::DB.payments << Exp6591::BookingsPayment.new(
        synced_id: payment_id, synced_booking_id: booking_id, synced_updated_at: child_synced_at,
        synced_canceled_at: purged_at, amount: "500.0"
      )
    end

    it "does not revive a child the purge cancelled, when the payload only ties on its timestamp" do
      consume

      expect(Exp6591::DB.payments.map(&:synced_canceled_at)).to eq([purged_at])
    end

    it "does not resolve the to_one association off a payload it rejected" do
      consume

      expect(stored_booking(booking_id).to_one_resolutions).to be_empty
    end

    context "when the child is demonstrably newer than the row held locally" do
      let(:child_updated_at) { "2026-08-31T13:21:00.000000Z" }

      it "persists it, so the strict comparison does not block a genuine repair" do
        consume

        expect(Exp6591::DB.payments.map(&:synced_canceled_at)).to eq([nil])
      end
    end
  end

  # a reproduction that fails because the fixture is wrong looks exactly like one that fails because the
  # code is wrong, so establish first that this harness persists a payload the guard accepts
  describe "Control - the same shape at a timestamp the guard accepts" do
    let(:booking_id) { 22_636_772 }
    let(:payment_id) { 15_394_239 }
    let(:projection_synced_at) { Time.parse("2026-08-31T13:12:56.614193Z") }
    let(:batch) do
      build_batch(
        [
          build_message(
            booking_payload(
              booking_id:, updated_at: "2026-08-31T13:12:56.975713Z", paid_amount: "127.0",
              serialized_at: "2026-08-31T13:12:57.097939Z",
              embedded: { payments: [payment_payload(payment_id:, booking_id:, amount: "127.0",
                created_at: "2026-08-31T13:12:57.100000Z", updated_at: "2026-08-31T13:12:57.100000Z")] }
            ),
            55_101_943, booking_id
          )
        ]
      )
    end

    before do
      Exp6591::DB.bookings << Exp6591::Booking.new(
        synced_id: booking_id, synced_updated_at: projection_synced_at, paid_amount: "0.0"
      )
    end

    it "persists the money" do
      consume

      expect(stored_booking(booking_id).paid_amount).to eq("127.0")
    end

    it "creates the embedded payment, so the has_many really is being traversed" do
      consume

      expect(Exp6591::DB.payments.map { |payment| [payment.synced_id, payment.amount] })
        .to eq([[payment_id, "127.0"]])
    end

    it "runs the purge, so the destructive path really is reachable in this harness" do
      consume

      expect(stored_booking(booking_id).to_many_resolutions).to eq([["bookings_payments", [payment_id]]])
    end
  end

  describe "Case A - 22636750, every money-bearing message carried a torn updated_at" do
    let(:booking_id) { 22_636_750 }
    let(:payment_id) { 15_394_101 }
    # the zero-money payload the projection accepted, at 13:06:36.581930
    let(:projection_synced_at) { Time.parse("2026-08-31T13:06:36.581930Z") }
    # the money messages report 13:06:36.537851 - 44ms BEFORE what the projection already holds
    let(:torn_updated_at) { "2026-08-31T13:06:36.537851Z" }
    # the payment row was created after the timestamp its own payload reports
    let(:payment_created_at) { "2026-08-31T13:06:37.073990Z" }
    let(:batch) do
      build_batch(
        [
          build_message(money_payload("2026-08-31T13:06:37.247000Z"), 55_101_152, booking_id),
          build_message(money_payload("2026-08-31T13:06:37.178000Z"), 55_101_153, booking_id)
        ]
      )
    end

    def money_payload(serialized_at)
      booking_payload(
        booking_id:, updated_at: torn_updated_at, paid_amount: "1056.45", serialized_at:,
        embedded: { payments: [payment_payload(payment_id:, booking_id:, amount: "1056.45",
          created_at: payment_created_at, updated_at: payment_created_at)] }
      )
    end

    before do
      Exp6591::DB.bookings << Exp6591::Booking.new(
        synced_id: booking_id, synced_updated_at: projection_synced_at, paid_amount: "0.0"
      )
    end

    it "persists the payment the booking payload carries" do
      consume

      expect(Exp6591::DB.payments.map(&:synced_id)).to eq([payment_id])
    end

    it "leaves paid_amount stale, because no consumer-side rule can prefer an older timestamp" do
      consume

      expect(stored_booking(booking_id).paid_amount).to eq("0.0")
    end

    it "does not purge children off a payload it has just rejected" do
      consume

      expect(stored_booking(booking_id).to_many_resolutions).to be_empty
    end

    # no candidate in this group clears the guard, so the repair cannot be coming from which message
    # survives deduplication - it has to be the traversal of the children
    context "when the consuming app turns deduplication off for the topic" do
      let(:topic_options) { deduplication_disabled }

      it "persists the payment either way" do
        consume

        expect(Exp6591::DB.payments.map(&:synced_id)).to eq([payment_id])
      end
    end
  end

  describe "Case B - 22636772, dedup discarded two messages the guard would have accepted" do
    let(:booking_id) { 22_636_772 }
    let(:payment_id) { 15_394_239 }
    # the zero-money payload the projection accepted, at 13:12:56.614193
    let(:projection_synced_at) { Time.parse("2026-08-31T13:12:56.614193Z") }
    let(:acceptable_updated_at) { "2026-08-31T13:12:56.975713Z" }
    let(:torn_updated_at) { "2026-08-31T13:12:56.598805Z" }
    let(:payment_created_at) { "2026-08-31T13:12:57.100000Z" }
    let(:batch) do
      build_batch(
        [
          build_message(money_payload(acceptable_updated_at, "2026-08-31T13:12:57.097939Z"), 55_101_943, booking_id),
          build_message(money_payload(acceptable_updated_at, "2026-08-31T13:12:57.340585Z"), 55_101_944, booking_id),
          build_message(money_payload(torn_updated_at, "2026-08-31T13:12:57.593432Z"), 55_101_945, booking_id)
        ]
      )
    end

    def money_payload(updated_at, serialized_at)
      booking_payload(
        booking_id:, updated_at:, paid_amount: "127.0", serialized_at:,
        embedded: { payments: [payment_payload(payment_id:, booking_id:, amount: "127.0",
          created_at: payment_created_at, updated_at: payment_created_at)] }
      )
    end

    before do
      Exp6591::DB.bookings << Exp6591::Booking.new(
        synced_id: booking_id, synced_updated_at: projection_synced_at, paid_amount: "0.0"
      )
    end

    it "persists the money, because two messages in the group carry a timestamp the guard accepts" do
      pending "needs guard-driven candidate fallback; ranking on updated_at is disproven by fixture 22578292"
      consume

      expect(stored_booking(booking_id).paid_amount).to eq("127.0")
    end

    it "persists the payment the booking payload carries" do
      consume

      expect(Exp6591::DB.payments.map(&:synced_id)).to eq([payment_id])
    end

    context "when the consuming app turns deduplication off for the topic" do
      let(:topic_options) { deduplication_disabled }

      it "persists the money without any change to this gem" do
        consume

        expect(stored_booking(booking_id).paid_amount).to eq("127.0")
      end
    end

    # isolates deduplication as the cause: the surviving message is the only difference
    context "when the torn message is not in the batch" do
      let(:batch) do
        build_batch(
          [
            build_message(money_payload(acceptable_updated_at, "2026-08-31T13:12:57.097939Z"), 55_101_943,
              booking_id),
            build_message(money_payload(acceptable_updated_at, "2026-08-31T13:12:57.340585Z"), 55_101_944,
              booking_id)
          ]
        )
      end

      it "persists the money" do
        consume

        expect(stored_booking(booking_id).paid_amount).to eq("127.0")
      end
    end
  end
end
