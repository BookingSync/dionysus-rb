# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::Model, type: :model do
  describe ".fetch_publishable" do
    subject(:fetch_publishable) { DionysusOutbox.fetch_publishable(batch_size, topic).to_a }

    let(:batch_size) { 4 }
    let(:topic) { "rentals" }

    let!(:outbox_record_1) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: 5.second.from_now)
    end
    let!(:outbox_record_2) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: 1.week.ago)
    end
    let!(:outbox_record_3) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: Time.current)
    end
    let!(:outbox_record_4) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: 1.month.ago, published_at: Time.current)
    end
    let!(:outbox_record_5) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: "other_topic", created_at: 1.month.ago)
    end
    let!(:outbox_record_6) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: 10.seconds.from_now)
    end
    let!(:outbox_record_7) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: 2.months.ago, retry_at: 1.day.from_now)
    end
    let!(:outbox_record_8) do
      DionysusOutbox.create(resource_class: "ExampleResource", resource_id: 1, event_name: "example_resource",
        topic: topic, created_at: 2.months.ago, retry_at: 1.second.ago)
    end

    before do
      DionysusOutbox.where.not(
        id: [outbox_record_1, outbox_record_2, outbox_record_3, outbox_record_4, outbox_record_5,
          outbox_record_6, outbox_record_7, outbox_record_8]
      ).delete_all
    end

    context "when the outbox_worker_publishing_delay configuration doesn't result in exclusion of some records" do
      before do
        Dionysus::Producer.configure do |config|
          config.outbox_worker_publishing_delay = 5
        end
      end

      it "returns sorted non-published records for a given topic up to a given limit
      that are supposed to be retried now if they failed previously" do
        expect(fetch_publishable).to eq([outbox_record_8, outbox_record_2, outbox_record_3, outbox_record_1])
      end
    end

    context "when the outbox_worker_publishing_delay configuration results in exclusion of some records" do
      before do
        Dionysus::Producer.configure do |config|
          config.outbox_worker_publishing_delay = 4
        end
      end

      it "returns sorted non-published records for a given topic up to a given limit
      that are supposed to be retried now if they failed previously considering the extra delay" do
        expect(fetch_publishable).to eq([outbox_record_8, outbox_record_2, outbox_record_3])
      end
    end
  end

  describe ".published_since", :freeze_time do
    subject(:published_since) { DionysusOutbox.published_since(10.seconds.ago) }

    let(:published_outbox_record_1) do
      DionysusOutbox.create!(published_at: 10.seconds.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let(:published_outbox_record_2) do
      DionysusOutbox.create!(published_at: 11.seconds.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let(:published_outbox_record_3) do
      DionysusOutbox.create!(published_at: 5.seconds.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let(:non_published_outbox_record_4) do
      DionysusOutbox.create!(published_at: nil, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end

    before do
      DionysusOutbox.where.not(
        id: [published_outbox_record_1, published_outbox_record_2, published_outbox_record_3,
          non_published_outbox_record_4]
      ).delete_all
    end

    it { is_expected.to match_array [published_outbox_record_1, published_outbox_record_3] }
  end

  describe ".not_published", :freeze_time do
    subject(:not_published) { DionysusOutbox.not_published }

    let(:published_outbox_record_1) do
      DionysusOutbox.create!(published_at: 10.seconds.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let(:non_published_outbox_record_2) do
      DionysusOutbox.create!(published_at: nil, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let(:published_outbox_record_3) do
      DionysusOutbox.create!(published_at: 5.seconds.ago, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end
    let(:non_published_outbox_record_4) do
      DionysusOutbox.create!(published_at: nil, resource_class: "",
        resource_id: "", event_name: "", topic: "")
    end

    before do
      DionysusOutbox.where.not(
        id: [published_outbox_record_1, non_published_outbox_record_2, published_outbox_record_3,
          non_published_outbox_record_4]
      ).delete_all
    end

    it { is_expected.to match_array [non_published_outbox_record_2, non_published_outbox_record_4] }
  end

  describe "relationships" do
    subject(:model) { DionysusOutbox.new }

    it { is_expected.to belong_to(:resource) }
  end

  describe ".pending_topics", :with_outbox_config do
    subject(:pending_topics) { DionysusOutbox.pending_topics }

    let!(:outbox_record_1) do
      DionysusOutbox.create!(resource_class: resource_1.class, resource_id: resource_1.id, event_name: event_name,
        created_at: Time.current, topic: "rentals")
    end
    let!(:resource_1) { ExampleResource.create!(account_id: account_id) }
    let!(:outbox_record_2) do
      DionysusOutbox.create!(resource_class: resource_2.class, resource_id: resource_2.id, event_name: event_name,
        created_at: 1.year.from_now, topic: "rentals")
    end
    let!(:resource_2) { ExampleResource.create!(account_id: account_id) }
    let!(:outbox_record_3) do
      DionysusOutbox.create!(resource_class: resource_3.class, resource_id: resource_3.id, event_name: event_name,
        created_at: 1.minute.ago, topic: "rentals")
    end
    let!(:resource_3) { ExampleResource.create!(account_id: account_id) }
    let!(:outbox_record_4) do
      DionysusOutbox.create!(resource_class: resource_4.class, resource_id: resource_4.id, event_name: event_name,
        created_at: Time.current, topic: "bookings")
    end
    let!(:resource_4) { ExampleResource.create!(account_id: account_id) }
    let!(:outbox_record_5) do
      DionysusOutbox.create!(resource_class: resource_4.class, resource_id: resource_4.id, event_name: event_name,
        created_at: Time.current, topic: "rates", published_at: Time.current)
    end
    let!(:resource_5) { ExampleResource.create!(account_id: account_id) }
    let(:account_id) { 1 }
    let(:event_name) { "event_name" }

    before do
      DionysusOutbox.where.not(id: [outbox_record_1, outbox_record_2, outbox_record_3, outbox_record_4,
        outbox_record_5]).delete_all
    end

    after do
      DionysusOutbox.delete_all
    end

    it { is_expected.to match_array %w[rentals bookings] }
  end

  describe "#handles_changeset?" do
    subject(:handles_changeset?) { outbox_model.handles_changeset? }

    context "when the model handles changeset" do
      let(:outbox_model) { DionysusOutbox }

      it { is_expected.to be true }
    end

    context "when the model does not handle changeset" do
      let(:outbox_model) { DionysusOutboxWithoutChangeset }

      it { is_expected.to be false }
    end
  end

  describe "#observer?" do
    subject(:observer?) { outbox_record.observer? }

    let(:outbox_record) { DionysusOutbox.new(topic: topic) }

    context "when topic is __outbox_observer__" do
      let(:topic) { "__outbox_observer__" }

      it { is_expected.to be true }
    end

    context "when topic is something else" do
      let(:topic) { "other" }

      it { is_expected.to be false }
    end
  end

  describe "#transformed_changeset" do
    subject(:transformed_changeset) { outbox_record.transformed_changeset }

    context "when the model handles changeset" do
      let(:outbox_record) { DionysusOutbox.new(changeset: changeset) }
      let(:changeset) { { "id" => 1, "name" => "name" } }

      context "when it handles it as jsonb" do
        let(:outbox_record) { DionysusOutbox.new(changeset: changeset) }

        it { is_expected.to eq changeset.symbolize_keys }
      end

      context "when it handles it as encrypted text" do
        let(:outbox_record) { DionysusOutboxEncrChangeset.new(changeset: changeset) }

        it { is_expected.to eq changeset.symbolize_keys }
      end
    end

    context "when the model does not handle changeset" do
      let(:outbox_record) { DionysusOutboxWithoutChangeset.new }

      it { is_expected.to eq({}) }
    end
  end

  describe "#published?" do
    subject(:published?) { outbox_record.published? }

    let(:outbox_record) { DionysusOutbox.new(published_at: published_at) }

    context "when published_at is present" do
      let(:published_at) { Time.current }

      it { is_expected.to be true }
    end

    context "when published_at is not present" do
      let(:published_at) { nil }

      it { is_expected.to be false }
    end
  end

  describe "#failed?" do
    subject(:failed?) { outbox_record.failed? }

    let(:outbox_record) { DionysusOutbox.new(failed_at: failed_at) }

    context "when failed_at is present" do
      let(:failed_at) { Time.current }

      it { is_expected.to be true }
    end

    context "when failed_at is not present" do
      let(:failed_at) { nil }

      it { is_expected.to be false }
    end
  end

  describe "#handle_error", :freeze_time do
    subject(:handle_error) { outbox_record.handle_error(error) }

    let(:outbox_record) { DionysusOutbox.new(attempts: attempts) }
    let(:error) { StandardError.new("some error") }

    describe "general behavior" do
      let(:attempts) { 0 }

      it "sets error-related attributes" do
        expect do
          handle_error
        end.to change { outbox_record.error_class }.to("StandardError")
          .and change { outbox_record.error_message }.to("some error")
          .and change { outbox_record.failed_at }.to(Time.current)
          .and change { outbox_record.attempts }.to(1)
          .and change { outbox_record.retry_at }.to(10.seconds.from_now)
      end

      describe "assigning error" do
        let(:error) { error_class.new("some error") }
        let(:error_class) { RuntimeError }

        it "assigns @error instance variable" do
          handle_error

          expect(outbox_record.error).to eq(error)
        end
      end
    end

    context "when attempts is nil" do
      let(:attempts) { nil }

      it "sets :retry_at to be 10 seconds from now" do
        expect do
          handle_error
        end.to change { outbox_record.retry_at }.to(10.seconds.from_now)
      end
    end

    context "when attempts is 0" do
      let(:attempts) { 0 }

      it "sets :retry_at to be 10 seconds from now" do
        expect do
          handle_error
        end.to change { outbox_record.retry_at }.to(10.seconds.from_now)
      end
    end

    context "when attempts is 1" do
      let(:attempts) { 1 }

      it "sets :retry_at to be 20 seconds from now" do
        expect do
          handle_error
        end.to change { outbox_record.retry_at }.to(20.seconds.from_now)
      end
    end

    context "when attempts is 2" do
      let(:attempts) { 2 }

      it "sets :retry_at to be 40 seconds from now" do
        expect do
          handle_error
        end.to change { outbox_record.retry_at }.to(40.seconds.from_now)
      end
    end
  end

  describe "#error" do
    subject(:error) { outbox_record.error }

    let(:outbox_record) { DionysusOutbox.new(error_class: error_class, error_message: "error message") }

    context "when the error's arity is 1" do
      let(:error_class) { "ErrorClassWithArityOne" }

      it { is_expected.to eq ErrorClassWithArityOne.new("error message") }
    end

    context "when the error's arity is -1" do
      let(:error_class) { "RuntimeError" }

      it { is_expected.to eq RuntimeError.new("error message") }
    end

    context "when the error's arity is different than 1, e.g. 2" do
      let(:error_class) { "ErrorClassWithArityTwo" }

      it { is_expected.to eq StandardError.new("ErrorClassWithArityTwo: error message") }
    end
  end

  describe "#resource_created_at" do
    subject(:resource_created_at) { outbox_record.resource_created_at }

    let(:outbox_record) { DionysusOutbox.new(created_at: 1.week.ago, resource: resource) }

    context "when resource is assigned" do
      let(:resource) { ExampleResource.new(created_at: 1.day.ago) }

      it { is_expected.to eq resource.created_at }
    end

    context "when resource is not assigned" do
      let(:resource) { nil }

      it { is_expected.to eq outbox_record.created_at }
    end
  end

  describe "#publishing_latency", :freeze_time do
    subject(:publishing_latency) { outbox_record.publishing_latency }

    let(:outbox_record) { DionysusOutbox.new(created_at: Time.current, published_at: published_at) }

    context "when outbox_record is published" do
      let(:published_at) { 3.seconds.from_now }

      it { is_expected.to eq 3 }
    end

    context "when outbox_record is not published" do
      let(:published_at) { nil }

      it { is_expected.to be_nil }
    end
  end

  describe "#created_event?" do
    subject(:created_event?) { outbox_record.created_event? }

    let(:outbox_record) { DionysusOutbox.new(event_name: event_name) }

    context "when event_name is nil" do
      let(:event_name) { nil }

      it { is_expected.to be false }
    end

    context "when event_name is _created" do
      let(:event_name) { "rental_created" }

      it { is_expected.to be true }
    end

    context "when event_name is _updated" do
      let(:event_name) { "rental_updated" }

      it { is_expected.to be false }
    end

    context "when event_name is _destroyed" do
      let(:event_name) { "rental_destroyed" }

      it { is_expected.to be false }
    end
  end

  describe "#updated_event?" do
    subject(:updated_event?) { outbox_record.updated_event? }

    let(:outbox_record) { DionysusOutbox.new(event_name: event_name) }

    context "when event_name is nil" do
      let(:event_name) { nil }

      it { is_expected.to be false }
    end

    context "when event_name is _created" do
      let(:event_name) { "rental_created" }

      it { is_expected.to be false }
    end

    context "when event_name is _updated" do
      let(:event_name) { "rental_updated" }

      it { is_expected.to be true }
    end

    context "when event_name is _destroyed" do
      let(:event_name) { "rental_destroyed" }

      it { is_expected.to be false }
    end
  end
end
