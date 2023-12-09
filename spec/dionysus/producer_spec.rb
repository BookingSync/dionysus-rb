# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer do
  describe ".configuration/.configure" do
    subject(:configuration) { described_class.configuration }

    before do
      described_class.configure do |config|
        config.registry = Dionysus::Producer::Registry.new
      end
    end

    it { is_expected.to be_a(Dionysus::Producer::Config) }
  end

  describe ".registry" do
    subject(:registry) { described_class.registry }

    before do
      described_class.configure do |config|
        config.registry = Dionysus::Producer::Registry.new
      end
    end

    it { is_expected.to be_a Dionysus::Producer::Registry }
  end

  describe ".declare" do
    subject(:declare) do
      described_class.declare do
        namespace :v3 do
          serializer Struct.new(:name).new("serializer")

          topic :rentals, partition_key: :account_id do
            publish "Rental"
            publish "RentalsTax"
          end
        end

        namespace :v4 do
          serializer Struct.new(:name).new("serializer_v4")

          topic :bookings do
            publish "Booking"
          end

          topic :accounts do
            publish "Account"
          end
        end
      end
    end

    let(:registry) { described_class.configuration.registry }

    it "assigns Registry to config" do
      declare

      expect(registry).to be_instance_of(Dionysus::Producer::Registry)
    end

    it "generates publishing routing with producers and stores declaration in Registry" do
      declare

      expect(registry.registrations.keys).to eq %i[v3 v4]

      expect(registry.registrations[:v3].topics.count).to eq 1
      expect(registry.registrations[:v3].producers.count).to eq 1
      expect(registry.registrations[:v3].producers.first).to eq Dionysus::V3RentalResponder
      expect(registry.registrations[:v3].topics.first.name).to eq :rentals
      expect(registry.registrations[:v3].topics.first.producer).to eq Dionysus::V3RentalResponder
      expect(registry.registrations[:v3].topics.first.serializer_klass.name).to eq "serializer"
      expect(registry.registrations[:v3].topics.first.options).to eq(partition_key: :account_id)
      expect(registry.registrations[:v3].topics.first.models.count).to eq 2
      expect(registry.registrations[:v3].topics.first.models.first.model_klass).to eq "Rental"
      expect(registry.registrations[:v3].topics.first.models.first.options).to eq({})
      expect(registry.registrations[:v3].topics.first.models.last.model_klass).to eq "RentalsTax"
      expect(registry.registrations[:v3].topics.first.models.last.options).to eq({})

      expect(registry.registrations[:v4].topics.count).to eq 2
      expect(registry.registrations[:v4].producers.count).to eq 2
      expect(registry.registrations[:v4].producers.first).to eq Dionysus::V4BookingResponder
      expect(registry.registrations[:v4].producers.last).to eq Dionysus::V4AccountResponder
      expect(registry.registrations[:v4].topics.first.name).to eq :bookings
      expect(registry.registrations[:v4].topics.first.producer).to eq Dionysus::V4BookingResponder
      expect(registry.registrations[:v4].topics.first.serializer_klass.name).to eq "serializer_v4"
      expect(registry.registrations[:v4].topics.first.options).to eq({})
      expect(registry.registrations[:v4].topics.first.models.count).to eq 1
      expect(registry.registrations[:v4].topics.first.models.first.model_klass).to eq "Booking"
      expect(registry.registrations[:v4].topics.first.models.first.options).to eq({})
      expect(registry.registrations[:v4].topics.last.name).to eq :accounts
      expect(registry.registrations[:v4].topics.last.producer).to eq Dionysus::V4AccountResponder
      expect(registry.registrations[:v4].topics.last.serializer_klass.name).to eq "serializer_v4"
      expect(registry.registrations[:v4].topics.last.options).to eq({})
      expect(registry.registrations[:v4].topics.last.models.count).to eq 1
      expect(registry.registrations[:v4].topics.last.models.first.model_klass).to eq "Account"
      expect(registry.registrations[:v4].topics.last.models.first.options).to eq({})
    end

    describe "auto-inclusion of ActiveRecordPublishable" do
      let(:publishable_module) { Dionysus::Producer::Outbox::ActiveRecordPublishable }

      context "when a given model is a top-level publishable model" do
        class TestClassForActiveRecordPublishableProducerTopLevel < ActiveRecord::Base
          self.table_name = "example_resources"
        end

        subject(:declare) do
          described_class.declare do
            namespace :v3 do
              serializer Struct.new(:name).new("serializer")

              topic :rentals do
                publish TestClassForActiveRecordPublishableProducerTopLevel
              end
            end
          end
        end

        it {
          is_expected_block.to change {
            TestClassForActiveRecordPublishableProducerTopLevel.included_modules.include?(publishable_module)
          }.from(false).to(true)
        }
      end

      context "when a given model is a dependency" do
        class TestClassForActiveRecordPublishableProducerDependency < ActiveRecord::Base
          self.table_name = "example_resources"
        end

        subject(:declare) do
          described_class.declare do
            namespace :v3 do
              serializer Struct.new(:name).new("serializer")

              topic :rentals do
                publish "Booking", with: [TestClassForActiveRecordPublishableProducerDependency]
              end
            end
          end
        end

        it {
          is_expected_block.to change {
            TestClassForActiveRecordPublishableProducerDependency.included_modules.include?(publishable_module)
          }.from(false).to(true)
        }
      end

      context "when a given model is an observable" do
        class TestClassForActiveRecordPublishableProducerObservable < ActiveRecord::Base
          self.table_name = "example_resources"
        end

        subject(:declare) do
          described_class.declare do
            namespace :v3 do
              serializer Struct.new(:name).new("serializer")

              topic :rentals do
                publish ExampleResource, observe: [
                  {
                    model: TestClassForActiveRecordPublishableProducerObservable,
                    attributes: %i[created_at],
                    association_name: :id
                  }
                ]
              end
            end
          end
        end

        it {
          is_expected_block.to change {
            TestClassForActiveRecordPublishableProducerObservable.included_modules.include?(publishable_module)
          }.from(false).to(true)
        }
      end
    end
  end

  describe ".outbox" do
    subject(:outbox) { described_class.outbox }

    it { is_expected.to be_an_instance_of(Dionysus::Producer::Outbox) }
  end

  describe ".outbox_publisher" do
    subject(:outbox_publisher) { described_class.outbox_publisher }

    it { is_expected.to be_an_instance_of(Dionysus::Producer::Outbox::Publisher) }
  end

  describe ".reset!" do
    subject(:reset!) { described_class.reset! }

    before do
      described_class.declare do
        namespace :v3 do
          topic :rentals do
            publish "Rental"
          end
        end
      end
    end

    it "resets config and undefines constants for Responders" do
      expect(Dionysus::V3RentalResponder).to be_present

      expect do
        reset!
      end.to change { described_class.configuration.registry }.to(nil)

      expect do
        Dionysus::V3RentalResponder
      end.to raise_error NameError
    end
  end

  describe ".responders_for" do
    subject(:responders_for) { described_class.responders_for(model) }

    context "when nothing is declared yet" do
      let(:model) { double }

      it { is_expected.to eq([]) }
    end

    context "when declaration is made" do
      before do
        described_class.declare do
          namespace :v3 do
            topic :rentals do
              publish "Rental"
              publish "RentalsTax"
            end
          end

          namespace :v4 do
            topic :bookings do
              publish "Booking"
            end

            topic :rentals do
              publish "Rental"
            end

            topic :accounts do
              publish "Account"
            end
          end
        end
      end

      describe "for Rental" do
        let(:model) { "Rental" }

        it {
          expect(responders_for).to eq [Dionysus::V3RentalResponder,
            Dionysus::V4RentalResponder]
        }
      end

      describe "for Booking" do
        let(:model) { "Booking" }

        it { is_expected.to eq [Dionysus::V4BookingResponder] }
      end

      describe "for Tax" do
        let(:model) { "Tax" }

        it { is_expected.to eq [] }
      end
    end
  end

  describe ".responders_for_model_for_topic" do
    subject(:responders_for) { described_class.responders_for_model_for_topic(model, topic) }

    context "when nothing is declared yet" do
      let(:model) { double }
      let(:topic) { double }

      it { is_expected.to eq([]) }
    end

    context "when declaration is made" do
      let(:model) { "Rental" }
      let(:topic) { "v3_rentals" }

      before do
        described_class.declare do
          namespace :v3 do
            topic :rentals do
              publish "Rental"
              publish "RentalsTax"
            end
          end

          namespace :v4 do
            topic :bookings do
              publish "Booking"
            end

            topic :rentals do
              publish "Rental"
            end

            topic :accounts do
              publish "Account"
            end
          end
        end
      end

      it { is_expected.to eq [Dionysus::V3RentalResponder] }
    end
  end

  describe ".responders_for_dependency_parent" do
    subject(:responders_for_dependency_parent) { described_class.responders_for_dependency_parent(model) }

    context "when nothing is declared yet" do
      let(:model) { double }

      it { is_expected.to eq([]) }
    end

    context "when declaration is made" do
      before do
        described_class.declare do
          namespace :v3 do
            topic :rentals do
              publish "Rental"
              publish "RentalsTax"
            end

            topic :bookings do
              publish "Booking"
            end
          end

          namespace :v4 do
            topic :bookings do
              publish "Booking", with: ["BookingsFee"]
            end

            topic :bookings_fee do
              publish "BookingsFee"
            end
          end
        end
      end

      describe "for BookingsFee" do
        let(:model) { "BookingsFee" }

        it "returns parent model class and responder where a given model is a dependency" do
          expect(responders_for_dependency_parent).to eq [["Booking", Dionysus::V4BookingResponder]]
        end
      end

      describe "for Booking" do
        let(:model) { "Booking" }

        it { is_expected.to eq [] }
      end
    end
  end

  describe ".responders_for_dependency_parent_for_topic" do
    subject(:responders_for_dependency_parent_for_topic) do
      described_class.responders_for_dependency_parent_for_topic(model, topic)
    end

    context "when nothing is declared yet" do
      let(:model) { double }
      let(:topic) { double }

      it { is_expected.to eq([]) }
    end

    context "when declaration is made" do
      let(:model) { "BookingsFee" }
      let(:topic) { "v4_bookings" }

      before do
        described_class.declare do
          namespace :v3 do
            topic :rentals do
              publish "Rental"
              publish "RentalsTax"
            end

            topic :bookings do
              publish "Booking", with: ["BookingsFee"]
            end
          end

          namespace :v4 do
            topic :bookings do
              publish "Booking", with: ["BookingsFee"]
            end

            topic :bookings_fee do
              publish "BookingsFee"
            end
          end
        end
      end

      it "returns parent model class for a given topic and responder where a given model is a dependency" do
        expect(responders_for_dependency_parent_for_topic).to eq [["Booking",
          Dionysus::V4BookingResponder]]
      end
    end
  end

  describe ".topics_models_mapping" do
    subject(:topic_models_mapping) { described_class.topics_models_mapping }

    context "when nothing is declared yet" do
      it { is_expected.to eq({}) }
    end

    context "when declaration is made" do
      before do
        described_class.declare do
          namespace :v3 do
            topic :rentals do
              publish "Rental"
              publish "RentalsTax"
            end

            topic :bookings do
              publish "Booking", with: ["BookingsFee"]
            end
          end

          namespace :v4 do
            topic :bookings do
              publish "Booking", with: ["BookingsFee"]
            end

            topic :bookings_fee do
              publish "BookingsFee"
            end
          end
        end
      end

      let(:expected_mapping) do
        {
          "v3_rentals" => {
            "Rental" => [],
            "RentalsTax" => []
          },
          "v3_bookings" => {
            "Booking" => ["BookingsFee"]
          },
          "v4_bookings" => {
            "Booking" => ["BookingsFee"]
          },
          "v4_bookings_fee" => {
            "BookingsFee" => []
          }
        }
      end

      it { is_expected.to eq expected_mapping }
    end
  end

  describe ".observers_with_responders_for" do
    subject(:observers_with_responders_for) { described_class.observers_with_responders_for(resource, changeset) }

    context "when nothing is declared yet" do
      let(:resource) { double }
      let(:changeset) do
        {}
      end

      it { is_expected.to eq([]) }
    end

    context "when declaration is made" do
      before do
        described_class.declare do
          namespace :v102 do
            serializer(Class.new do
              def self.serialize(records, dependencies:)
                records.map { |record| { id: record.id } }
              end
            end)

            topic :rentals, partition_key: :account_id do
              publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
            end

            topic :other_rentals, partition_key: :account_id do
              publish ExampleResource, with: [ExamplePublishableResource, ExamplePublishableCancelableResource]
            end

            topic :observer_to_one do
              publish ExampleResource, observe: [
                {
                  model: ExamplePublishableResource,
                  attributes: %i[created_at],
                  association_name: :example_resource
                },
                {
                  model: ExamplePublishableCancelableResource,
                  attributes: %i[id],
                  association_name: :example_resources
                }
              ]
            end

            topic :observer_to_many do
              publish ExampleResource, observe: [
                {
                  model: ExamplePublishableCancelableResource,
                  attributes: %i[updated_at],
                  association_name: :example_resources
                },
                {
                  model: ExamplePublishableResource,
                  attributes: %i[id],
                  association_name: :example_resource
                }
              ]
            end

            topic :observer_to_many_duplicate do
              publish ExampleResource, observe: [
                {
                  model: ExamplePublishableCancelableResource,
                  attributes: %i[updated_at],
                  association_name: :example_resources
                },
                {
                  model: ExamplePublishableResource,
                  attributes: %i[id],
                  association_name: "itself.example_resource"
                }
              ]
            end
          end
        end
      end

      context "when resource is ExamplePublishableResource" do
        let(:resource) { ExamplePublishableResource.new(example_resource: related_record) }
        let(:related_record) { ExampleResource.new }

        context "when changeset contains observable attributes" do
          let(:changeset) do
            {
              "id" => [0, 1]
            }
          end

          context "when the related resource is nil" do
            let(:related_record) { nil }
            let(:expected_result) do
              [
                [[], Dionysus::V102ObserverToManyResponder],
                [[], Dionysus::V102ObserverToManyDuplicateResponder]
              ]
            end

            it { is_expected.to eq(expected_result) }
          end

          context "when the related resource is present" do
            let(:expected_result) do
              [
                [[related_record], Dionysus::V102ObserverToManyResponder],
                [[related_record], Dionysus::V102ObserverToManyDuplicateResponder]
              ]
            end

            it { is_expected.to eq(expected_result) }
          end
        end

        context "when changeset does not contain observable attributes" do
          let(:changeset) do
            {
              "name" => [0, 1]
            }
          end

          it { is_expected.to eq([]) }
        end
      end
    end
  end
end
