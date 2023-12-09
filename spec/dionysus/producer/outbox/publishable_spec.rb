# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::Publishable, :with_outbox_config do
  describe "#model" do
    subject(:model) { publishable.model }

    let(:publishable) { described_class.new(publishable_model) }
    let(:publishable_model) { ExampleResource.new }

    it { is_expected.to eq publishable_model }
  end

  describe "#model_class" do
    subject(:model_class) { publishable.model_class }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.new }

    it { is_expected.to eq model.class }
  end

  describe "#primary_key_attribute" do
    subject(:primary_key_attribute) { publishable.primary_key_attribute }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.new }

    it { is_expected.to eq "id" }
  end

  describe "#publishable_id" do
    subject(:publishable_id) { publishable.publishable_id }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.new(id: 123) }

    it { is_expected.to eq 123 }
  end

  describe "#model_name" do
    subject(:model_name) { publishable.model_name }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.new }

    it { is_expected.to eq ExampleResource.model_name }
  end

  describe "#resource_name" do
    subject(:resource_name) { publishable.resource_name }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.new }

    it { is_expected.to eq "example_resource" }
  end

  describe "#previously_changed?" do
    subject(:previously_changed?) { publishable.previously_changed? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.create!(account_id: 1).tap(&:reload) }

    context "when no change happened" do
      it { is_expected.to be false }
    end

    context "when a change happened" do
      before do
        model.update(account_id: 2)
      end

      it { is_expected.to be true }
    end
  end

  describe "#previous_changes_include_canceled?" do
    subject(:previous_changes_include_canceled?) { publishable.previous_changes_include_canceled? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleCancelableResource.create!(account_id: 1).tap(&:reload) }

    context "when no change happened" do
      it { is_expected.to be false }
    end

    context "when a change happened" do
      context "when there was a change in soft_delete_column" do
        before do
          model.update(canceled_at: Time.current)
        end

        it { is_expected.to be true }
      end

      context "when there was no change in soft_delete_column" do
        before do
          model.update(account_id: 2)
        end

        it { is_expected.to be false }
      end
    end
  end

  describe "#previous_changes_uncanceled?" do
    subject(:previous_changes_uncanceled?) { publishable.previous_changes_uncanceled? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleCancelableResource.create!(canceled_at: Time.current).tap(&:reload) }

    context "when getting uncanceled" do
      before do
        model.update(canceled_at: nil)
      end

      it { is_expected.to be true }
    end

    context "when not getting uncanceled" do
      before do
        model.update(canceled_at: 1.week.ago)
      end

      it { is_expected.to be false }
    end
  end

  describe "#previous_changes_canceled?" do
    subject(:previous_changes_canceled?) { publishable.previous_changes_canceled? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleCancelableResource.create!.tap(&:reload) }

    context "when getting canceled" do
      before do
        model.update(canceled_at: Time.current)
      end

      it { is_expected.to be true }
    end

    context "when not getting canceled" do
      before do
        model.update(account_id: 2)
      end

      it { is_expected.to be false }
    end
  end

  describe "#previous_changes_still_canceled?" do
    subject(:previous_changes_still_canceled?) { publishable.previous_changes_still_canceled? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleCancelableResource.create!(canceled_at: Time.current).tap(&:reload) }

    context "when canceled_at changes and the record is still canceled" do
      before do
        model.update(canceled_at: 1.week.ago)
      end

      it { is_expected.to be true }
    end

    context "when canceled_at changes and the record is no longer canceled" do
      before do
        model.update(canceled_at: nil)
      end

      it { is_expected.to be false }
    end
  end

  describe "#soft_deleted?" do
    subject(:soft_deleted?) { publishable.soft_deleted? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleCancelableResource.new }

    context "when soft_deleted" do
      before do
        model.canceled_at = Time.current
      end

      it { is_expected.to be true }
    end

    context "when visible" do
      it { is_expected.to be false }
    end
  end

  describe "#soft_deletable?" do
    subject(:soft_deletable?) { publishable.soft_deletable? }

    let(:publishable) { described_class.new(model) }

    context "when soft deletable" do
      let(:model) { ExampleCancelableResource.new }

      it { is_expected.to be true }
    end

    context "when not soft deletable" do
      let(:model) { ExampleResource.new }

      it { is_expected.to be false }
    end
  end

  describe "#visible?" do
    subject(:visible?) { publishable.visible? }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleCancelableResource.new }

    context "when soft_deleted" do
      before do
        model.canceled_at = Time.current
      end

      it { is_expected.to be false }
    end

    context "when visible" do
      it { is_expected.to be true }
    end
  end

  describe "#topics" do
    subject(:topics) { publishable.topics }

    let(:publishable) { described_class.new(model) }
    let(:model) { ExampleResource.new }

    before do
      Dionysus::Producer.configure do |config|
        config.outbox_model = DionysusOutbox
      end
    end

    context "when the model is not in the config at all" do
      before do
        Dionysus::Producer.declare do
          namespace :v102 do
            serializer(Class.new do
              def self.serialize(records, dependencies:)
                records.map { |record| { id: record.id } }
              end
            end)
          end
        end
      end

      it { is_expected.to eq [] }
    end

    context "when the model is supposed to be published" do
      before do
        Dionysus::Producer.declare do
          namespace :v102 do
            serializer(Class.new do
              def self.serialize(records, dependencies:)
                records.map { |record| { id: record.id } }
              end
            end)

            topic :rentals, genesis_replica: true do
              publish ExampleResource
            end

            topic :other_rentals do
              publish ExampleResource
            end
          end
        end
      end

      it { is_expected.to match_array %w[v102_rentals v102_other_rentals] }
    end

    context "when the model is supposed to be published and is also an observable" do
      before do
        model.save!

        Dionysus::Producer.declare do
          namespace :v102 do
            serializer(Class.new do
              def self.serialize(records, dependencies:)
                records.map { |record| { id: record.id } }
              end
            end)

            topic :rentals do
              publish ExampleResource
              publish ExampleCancelableResource, observe: [
                {
                  model: ExampleResource,
                  association_name: :example_publishable_resource,
                  attributes: %i[created_at]
                }
              ]
            end
          end
        end
      end

      it { is_expected.to match_array %w[v102_rentals __outbox_observer__] }
    end

    context "when the model is just an observable" do
      context "when the model contains proper attributes in the changeset" do
        before do
          model.save!

          Dionysus::Producer.declare do
            namespace :v102 do
              serializer(Class.new do
                def self.serialize(records, dependencies:)
                  records.map { |record| { id: record.id } }
                end
              end)

              topic :rentals do
                publish ExampleCancelableResource, observe: [
                  {
                    model: ExampleResource,
                    association_name: :example_publishable_resource,
                    attributes: %i[created_at]
                  }
                ]
              end
            end
          end
        end

        it { is_expected.to eq ["__outbox_observer__"] }
      end

      context "when the model does not contain proper attributes in the changeset" do
        before do
          Dionysus::Producer.declare do
            namespace :v102 do
              serializer(Class.new do
                def self.serialize(records, dependencies:)
                  records.map { |record| { id: record.id } }
                end
              end)

              topic :rentals do
                publish ExampleCancelableResource, observe: [
                  {
                    model: ExampleResource,
                    association_name: :example_publishable_resource,
                    attributes: %i[created_at]
                  }
                ]
              end
            end
          end
        end

        it { is_expected.to eq [] }
      end
    end

    context "when the model is a sideloadable dependency" do
      before do
        Dionysus::Producer.declare do
          namespace :v102 do
            serializer(Class.new do
              def self.serialize(records, dependencies:)
                records.map { |record| { id: record.id } }
              end
            end)

            topic :rentals do
              publish ExampleCancelableResource, with: [ExampleResource]
            end

            topic :other do
              publish ExampleCancelableResource, with: [ExampleResource]
              publish ExampleResource
            end
          end
        end
      end

      it { is_expected.to match_array %w[v102_rentals v102_other] }
    end
  end

  describe "#changeset", :freeze_time do
    subject(:changeset) { publishable.changeset }

    let(:publishable) { described_class.new(model) }

    context "when record is created" do
      let!(:model) { ExampleCancelableResource.new(account_id: 1) }
      let(:expected_changeset) do
        {
          "account_id" => [nil, 1],
          "id" => [nil, model.id],
          "created_at" => [nil, Time.current],
          "updated_at" => [nil, Time.current]
        }
      end

      before do
        model.save!
      end

      it { is_expected.to eq expected_changeset }
    end

    context "when record is updated" do
      let!(:model) { ExampleCancelableResource.create!(account_id: 1).tap(&:reload) }
      let(:expected_changeset) do
        {
          "account_id" => [1, 2]
        }
      end

      before do
        model.update!(account_id: 2)
      end

      it { is_expected.to eq expected_changeset }
    end

    context "when record is destroyed" do
      let!(:model) { ExampleCancelableResource.create!(account_id: 1).tap(&:reload) }
      let(:expected_changeset) do
        {
          "id" => [model.id, nil],
          "created_at" => [Time.current, nil]
        }
      end

      before do
        model.destroy
      end

      it { is_expected.to eq expected_changeset }
    end
  end
end
