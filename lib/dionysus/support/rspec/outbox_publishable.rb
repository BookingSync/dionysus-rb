# frozen_string_literal: true

RSpec.shared_examples_for "Dionysus Transactional Outbox Publishable" do |extra_attributes|
  let(:attributes_for_model) { extra_attributes.to_h }

  describe "Dionysus Transactional Outbox" do
    let(:outbox_model) { Dionysus::Producer.configuration.outbox_model }

    before do
      outbox_model.delete_all
    end

    context "when the record gets created" do
      subject(:create_record) { create(described_class.model_name.singular.to_sym, attributes_for_model) }

      let(:resource_class) { described_class.model_name.to_s }
      let(:resource_id) { create_record.id }
      let(:event_name) { "#{described_class.model_name.singular}_created" }
      let(:outbox_records_for_resource) do
        outbox_model.where(resource_class: resource_class, event_name: event_name)
      end
      let(:outbox_record_for_created_resource) do
        outbox_model.find_by(resource_class: resource_class, resource_id: resource_id, event_name: event_name)
      end

      it "creates an Outbox Record" do
        expect do
          create_record
        end.to change { outbox_records_for_resource.count }.from(0)
        expect(outbox_record_for_created_resource).to be_present
      end
    end

    context "when the record gets updated" do
      subject(:update_record) { record.update(attributes_for_model.merge(updated_at: Time.current)) }

      let!(:record) { create(described_class.model_name.singular.to_sym) }
      let(:resource_class) { described_class.model_name.to_s }
      let(:resource_id) { record.id }
      let(:event_name) { "#{described_class.model_name.singular}_updated" }
      let(:outbox_records_for_resource) do
        outbox_model.where(resource_class: resource_class, event_name: event_name)
      end
      let(:outbox_record_for_updated_resource) do
        outbox_model.find_by(resource_class: resource_class, resource_id: resource_id, event_name: event_name)
      end

      it "creates an Outbox Record" do
        expect do
          update_record
        end.to change { outbox_records_for_resource.count }.from(0)
        expect(outbox_record_for_updated_resource).to be_present
      end
    end

    context "when the record gets deleted" do
      subject(:delete_record) { record.destroy }

      let!(:record) { create(described_class.model_name.singular.to_sym) }
      let(:resource_class) { described_class.model_name.to_s }
      let(:resource_id) { record.id }
      let(:event_name) { "#{described_class.model_name.singular}_destroyed" }
      let(:outbox_records_for_resource) do
        outbox_model.where(resource_class: resource_class, event_name: event_name)
      end
      let(:outbox_record_for_deleted_resource) do
        outbox_model.find_by(resource_class: resource_class, resource_id: resource_id, event_name: event_name)
      end

      it "creates an Outbox Record" do
        expect do
          delete_record
        end.to change { outbox_records_for_resource.count }.from(0)
        expect(outbox_record_for_deleted_resource).to be_present
      end
    end
  end
end
