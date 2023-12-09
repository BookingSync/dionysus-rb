# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::Outbox::ActiveRecordPublishable do
  let(:outbox_model) { DionysusOutbox }
  let(:created_outbox_record) { DionysusOutbox.last }
  let(:soft_delete_column) { :canceled_at }

  before do
    Dionysus::Producer.declare do
      namespace :v102 do
        serializer(Class.new do
          def self.serialize(records, dependencies:)
            records.map { |record| record.as_json.except("created_at", "updated_at") }
          end
        end)

        topic :rentals do
          publish "Rental"
          publish ExamplePublishableResource
          publish ExamplePublishableCancelableResource
          publish ExamplePublishablePublishAfterSoftDeletionResource
        end

        topic :other do
          publish ExamplePublishableResource
        end

        topic :observer_to_one do
          publish ExampleResource, observe: [
            {
              model: ExamplePublishableResource,
              attributes: %i[account_id],
              association_name: :example_resource
            }
          ]
        end

        topic :observer_to_one_canceled do
          publish ExampleResource, observe: [
            {
              model: ExamplePublishableCancelableResource,
              attributes: %i[canceled_at],
              association_name: :example_resources
            }
          ]
        end
      end
    end

    Dionysus::Producer.configure do |config|
      config.outbox_model = outbox_model
      config.soft_delete_column = soft_delete_column
      config.transaction_provider = ActiveRecord::Base
      config.default_partition_key = :account_id
    end

    DionysusOutbox.delete_all
  end

  describe "after create" do
    subject(:create_record) { resource.save! }

    let(:resource) { ExamplePublishableResource.new }
    let(:other_created_outbox_record) { DionysusOutbox.last(2).first }

    context "when transactional_outbox_enabled is set to true" do
      context "when the observer should be published", freeze_time: "2022-08-12T13:24:24.000Z" do
        let(:resource) { ExamplePublishableResource.new(account_id: 1) }
        let(:created_outbox_record) { DionysusOutbox.find_by(topic: "v102_other") }
        let(:other_created_outbox_record) { DionysusOutbox.find_by(topic: "v102_rentals") }
        let(:observer_outbox_record) { DionysusOutbox.find_by(topic: "__outbox_observer__") }

        before do
          DionysusOutbox.delete_all
        end

        it "creates DionysusOutbox records (for all topics + the special for observers)
        with event_name with 'created' event" do
          expect do
            create_record
          end.to change { DionysusOutbox.count }.by(3)
          expect(created_outbox_record.event_name).to eq "example_publishable_resource_created"
          expect(created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(created_outbox_record.resource_id).to eq resource.id.to_s
          expect(created_outbox_record.topic).to eq "v102_other"
          expect(created_outbox_record.changeset).to eq({})
          expect(created_outbox_record.partition_key).to eq("1")
          expect(other_created_outbox_record.event_name).to eq "example_publishable_resource_created"
          expect(other_created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(other_created_outbox_record.resource_id).to eq resource.id.to_s
          expect(other_created_outbox_record.topic).to eq "v102_rentals"
          expect(other_created_outbox_record.changeset).to eq({})
          expect(other_created_outbox_record.partition_key).to eq("1")
          expect(observer_outbox_record.event_name).to eq "example_publishable_resource_created"
          expect(observer_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(observer_outbox_record.resource_id).to eq resource.id.to_s
          expect(observer_outbox_record.topic).to eq "__outbox_observer__"
          expect(observer_outbox_record.changeset).to eq(
            {
              "account_id" => [nil, 1],
              "created_at" => [nil, "2022-08-12T13:24:24.000Z"],
              "id" => [nil, resource.id],
              "updated_at" => [nil, "2022-08-12T13:24:24.000Z"]
            }
          )
          expect(observer_outbox_record.partition_key).to be_nil
        end
      end

      context "when no observer should be published" do
        let(:resource) { ExamplePublishableResource.new }
        let(:other_created_outbox_record) { DionysusOutbox.last(2).first }

        it "creates DionysusOutbox records (for all topics) with event_name with 'created' event" do
          expect do
            create_record
          end.to change { DionysusOutbox.count }.by(2)
          expect(created_outbox_record.event_name).to eq "example_publishable_resource_created"
          expect(created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(created_outbox_record.resource_id).to eq resource.id.to_s
          expect(created_outbox_record.topic).to eq "v102_other"
          expect(created_outbox_record.changeset).to eq({})
          expect(created_outbox_record.partition_key).to eq("0")
          expect(other_created_outbox_record.event_name).to eq "example_publishable_resource_created"
          expect(other_created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(other_created_outbox_record.resource_id).to eq resource.id.to_s
          expect(other_created_outbox_record.topic).to eq "v102_rentals"
          expect(other_created_outbox_record.changeset).to eq({})
          expect(other_created_outbox_record.partition_key).to eq("0")
        end
      end

      describe "publishing records after commit" do
        let(:resource) { ExamplePublishableResource.new }
        let(:other_created_outbox_record) { DionysusOutbox.last(2).first }

        context "when :publish_after_commit is set to true", :publish_after_commit do
          context "when no error is raised" do
            it "creates DionysusOutbox records and publishes them" do
              expect do
                create_record
              end.to change { DionysusOutbox.count }.by(2)
              expect(created_outbox_record).to be_published
              expect(other_created_outbox_record).to be_published
            end

            it "clears the publishable records storage" do
              create_record

              expect(resource.class.outbox_records_to_publish).to be_empty
            end
          end

          context "when an error is raised" do
            before do
              allow(Dionysus).to receive(:logger).and_return(Logger.new($stdout))
              allow(Dionysus.logger).to receive(:error).and_call_original
              allow_any_instance_of(Dionysus::Producer::Outbox::RecordsProcessor)
                .to receive(:call) { raise "something went wrong" }
            end

            it "creates DionysusOutbox records but does not publish them" do
              expect do
                create_record
              end.to change { DionysusOutbox.count }.by(2)
              expect(created_outbox_record).not_to be_published
              expect(other_created_outbox_record).not_to be_published
            end

            it "clears the publishable records storage" do
              create_record

              expect(resource.class.outbox_records_to_publish).to be_empty
            end

            it "logs the error" do
              create_record

              expect(Dionysus.logger).to have_received(:error)
                .with("[Dionysus Outbox from publish_outbox_records] RuntimeError: something went wrong")
            end
          end
        end

        context "when :publish_after_commit is set to false" do
          it "creates DionysusOutbox records but does not publish them" do
            expect do
              create_record
            end.to change { DionysusOutbox.count }.by(2)
            expect(created_outbox_record).not_to be_published
            expect(other_created_outbox_record).not_to be_published
          end

          it "clears the publishable records storage" do
            create_record

            expect(resource.class.outbox_records_to_publish).to be_empty
          end
        end
      end
    end

    context "when transactional_outbox_enabled is set to false" do
      before do
        Dionysus::Producer.configuration.transactional_outbox_enabled = false
      end

      after do
        Dionysus::Producer.configuration.transactional_outbox_enabled = true
      end

      it { is_expected_block.not_to change { DionysusOutbox.count } }
    end
  end

  describe "after update" do
    context "when resource is soft-deletable" do
      let!(:resource) { ExamplePublishableCancelableResource.create! }

      context "when the record was just touched" do
        subject(:update_record) { resource.touch }

        it { is_expected_block.not_to change { DionysusOutbox.count } }
      end

      context "when the record is properly updated" do
        subject(:update_record) { resource.update!(updated_at: Time.current) }

        context "when transactional_outbox_enabled is set to true" do
          context "when no observer should be published" do
            it "creates DionysusOutbox record with event_name with 'updated' event" do
              expect do
                update_record
              end.to change { DionysusOutbox.count }.by(1)
              expect(created_outbox_record.event_name).to eq "example_publishable_cancelable_resource_updated"
              expect(created_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
              expect(created_outbox_record.resource_id).to eq resource.id.to_s
              expect(created_outbox_record.topic).to eq "v102_rentals"
              expect(created_outbox_record.partition_key).to eq("0")
            end
          end

          context "when the observer should be published", freeze_time: "2022-08-12T13:24:24.000Z" do
            subject(:update_record) do
              resource.save!
              DionysusOutbox.delete_all
              resource.update!(updated_at: Time.current, account_id: 5)
            end

            let(:resource) { ExamplePublishableResource.new(account_id: 1) }
            let(:created_outbox_record) { DionysusOutbox.find_by(topic: "v102_other") }
            let(:other_created_outbox_record) { DionysusOutbox.find_by(topic: "v102_rentals") }
            let(:observer_outbox_record) { DionysusOutbox.find_by(topic: "__outbox_observer__") }

            before do
              DionysusOutbox.delete_all
            end

            it "creates DionysusOutbox records (for all topics + the special for observers)
            with event_name with 'updated' event" do
              expect do
                update_record
              end.to change { DionysusOutbox.count }.by(3)
              expect(created_outbox_record.event_name).to eq "example_publishable_resource_updated"
              expect(created_outbox_record.resource_class).to eq "ExamplePublishableResource"
              expect(created_outbox_record.resource_id).to eq resource.id.to_s
              expect(created_outbox_record.topic).to eq "v102_other"
              expect(created_outbox_record.changeset).to eq({})
              expect(created_outbox_record.partition_key).to eq("5")
              expect(other_created_outbox_record.event_name).to eq "example_publishable_resource_updated"
              expect(other_created_outbox_record.resource_class).to eq "ExamplePublishableResource"
              expect(other_created_outbox_record.resource_id).to eq resource.id.to_s
              expect(other_created_outbox_record.topic).to eq "v102_rentals"
              expect(other_created_outbox_record.changeset).to eq({})
              expect(other_created_outbox_record.partition_key).to eq("5")
              expect(observer_outbox_record.event_name).to eq "example_publishable_resource_updated"
              expect(observer_outbox_record.resource_class).to eq "ExamplePublishableResource"
              expect(observer_outbox_record.resource_id).to eq resource.id.to_s
              expect(observer_outbox_record.topic).to eq "__outbox_observer__"
              expect(observer_outbox_record.changeset).to eq(
                {
                  "account_id" => [1, 5]
                }
              )
              expect(observer_outbox_record.partition_key).to be_nil
            end
          end

          describe "publishing records after commit" do
            context "when :publish_after_commit is set to true", :publish_after_commit do
              it "creates DionysusOutbox records and publishes them" do
                expect do
                  update_record
                end.to change { DionysusOutbox.count }.by(1)
                expect(created_outbox_record).to be_published
              end

              it "clears the publishable records storage" do
                update_record

                expect(resource.class.outbox_records_to_publish).to be_empty
              end
            end

            context "when :publish_after_commit is set to false" do
              it "creates DionysusOutbox records but does not publish them" do
                expect do
                  update_record
                end.to change { DionysusOutbox.count }.by(1)
                expect(created_outbox_record).not_to be_published
              end

              it "clears the publishable records storage" do
                update_record

                expect(resource.class.outbox_records_to_publish).to be_empty
              end
            end
          end
        end

        context "when transactional_outbox_enabled is set to false" do
          before do
            Dionysus::Producer.configuration.transactional_outbox_enabled = false
          end

          after do
            Dionysus::Producer.configuration.transactional_outbox_enabled = true
          end

          it { is_expected_block.not_to change { DionysusOutbox.count } }
        end
      end

      context "when the record gets soft-deleted" do
        subject(:update_record) { resource.update!(soft_delete_column => Time.current, **extra_attributes) }

        let(:extra_attributes) do
          {}
        end

        context "when the record is not soft-deleted", freeze_time: "2022-08-22T08:01:47Z" do
          context "when transactional_outbox_enabled is set to true" do
            context "when some observer should be published" do
              let(:created_outbox_record) { DionysusOutbox.find_by(topic: "v102_rentals") }
              let(:observer_outbox_record) { DionysusOutbox.find_by(topic: "__outbox_observer__") }

              before do
                DionysusOutbox.delete_all
              end

              context "when it's just a soft-delete without update of any other attributes" do
                it "creates DionysusOutbox records with event_name with 'deleted' event" do
                  expect do
                    update_record
                  end.to change { DionysusOutbox.count }.from(0).to(2)
                  expect(created_outbox_record.event_name).to eq "example_publishable_cancelable_resource_destroyed"
                  expect(created_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
                  expect(created_outbox_record.resource_id).to eq resource.id.to_s
                  expect(created_outbox_record.topic).to eq "v102_rentals"
                  expect(created_outbox_record.changeset).to eq({})
                  expect(created_outbox_record.partition_key).to eq("0")
                  expect(observer_outbox_record.event_name).to eq "example_publishable_cancelable_resource_destroyed"
                  expect(observer_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
                  expect(observer_outbox_record.resource_id).to eq resource.id.to_s
                  expect(observer_outbox_record.topic).to eq "__outbox_observer__"
                  expect(observer_outbox_record.changeset).to eq({
                    "canceled_at" => [nil, "2022-08-22T08:01:47.000Z"]
                  })
                  expect(observer_outbox_record.partition_key).to be_nil
                end
              end

              context "when it's soft-delete with an update of some other attributes" do
                let(:extra_attributes) do
                  {
                    account_id: 10
                  }
                end

                it "creates DionysusOutbox records with event_name with 'deleted' event" do
                  expect do
                    update_record
                  end.to change { DionysusOutbox.count }.from(0).to(2)
                  expect(created_outbox_record.event_name).to eq "example_publishable_cancelable_resource_destroyed"
                  expect(created_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
                  expect(created_outbox_record.resource_id).to eq resource.id.to_s
                  expect(created_outbox_record.topic).to eq "v102_rentals"
                  expect(created_outbox_record.changeset).to eq({})
                  expect(created_outbox_record.partition_key).to eq("10")
                  expect(observer_outbox_record.event_name).to eq "example_publishable_cancelable_resource_destroyed"
                  expect(observer_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
                  expect(observer_outbox_record.resource_id).to eq resource.id.to_s
                  expect(observer_outbox_record.topic).to eq "__outbox_observer__"
                  expect(observer_outbox_record.changeset).to eq({
                    "account_id" => [nil, 10],
                    "canceled_at" => [nil, "2022-08-22T08:01:47.000Z"]
                  })
                  expect(observer_outbox_record.partition_key).to be_nil
                end
              end
            end

            describe "publishing records after commit" do
              let(:created_outbox_record) { DionysusOutbox.find_by(topic: "v102_rentals") }
              let(:observer_outbox_record) { DionysusOutbox.find_by(topic: "__outbox_observer__") }

              before do
                DionysusOutbox.delete_all
              end

              context "when :publish_after_commit is set to true", :publish_after_commit do
                it "creates DionysusOutbox records and publishes them" do
                  expect do
                    update_record
                  end.to change { DionysusOutbox.count }.by(2)
                  expect(created_outbox_record).to be_published
                  expect(observer_outbox_record).to be_published
                end

                it "clears the publishable records storage" do
                  update_record

                  expect(resource.class.outbox_records_to_publish).to be_empty
                end
              end

              context "when :publish_after_commit is set to false" do
                it "creates DionysusOutbox records but does not publish them" do
                  expect do
                    update_record
                  end.to change { DionysusOutbox.count }.by(2)
                  expect(created_outbox_record).not_to be_published
                  expect(observer_outbox_record).not_to be_published
                end

                it "clears the publishable records storage" do
                  update_record

                  expect(resource.class.outbox_records_to_publish).to be_empty
                end
              end
            end
          end

          context "when transactional_outbox_enabled is set to false" do
            before do
              Dionysus::Producer.configuration.transactional_outbox_enabled = false
            end

            after do
              Dionysus::Producer.configuration.transactional_outbox_enabled = true
            end

            it { is_expected_block.not_to change { DionysusOutbox.count } }
          end
        end

        context "when the record is soft-deleted already" do
          before do
            resource.update!(soft_delete_column => 1.day.ago)
          end

          it { is_expected_block.not_to change { DionysusOutbox.count } }
        end
      end

      context "when the soft-deleted record gets restored" do
        subject(:update_record) { resource.update!(soft_delete_column => nil) }

        before do
          resource.update!(soft_delete_column => 1.day.ago)
        end

        context "when transactional_outbox_enabled is set to true", freeze_time: "2022-08-22T08:01:47" do
          let(:created_outbox_record) { DionysusOutbox.find_by(topic: "v102_rentals") }
          let(:observer_outbox_record) { DionysusOutbox.find_by(topic: "__outbox_observer__") }

          before do
            DionysusOutbox.delete_all
          end

          it "creates DionysusOutbox records with event_name with 'created' event" do
            expect do
              update_record
            end.to change { DionysusOutbox.count }.from(0).to(2)
            expect(created_outbox_record.event_name).to eq "example_publishable_cancelable_resource_created"
            expect(created_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
            expect(created_outbox_record.resource_id).to eq resource.id.to_s
            expect(created_outbox_record.topic).to eq "v102_rentals"
            expect(created_outbox_record.partition_key).to eq("0")
            expect(observer_outbox_record.event_name).to eq "example_publishable_cancelable_resource_created"
            expect(observer_outbox_record.resource_class).to eq "ExamplePublishableCancelableResource"
            expect(observer_outbox_record.resource_id).to eq resource.id.to_s
            expect(observer_outbox_record.topic).to eq "__outbox_observer__"
            expect(observer_outbox_record.changeset).to eq({
              "canceled_at" => ["2022-08-21T08:01:47.000Z", nil]
            })
            expect(observer_outbox_record.partition_key).to be_nil
          end

          describe "publishing records after commit" do
            context "when :publish_after_commit is set to true", :publish_after_commit do
              it "creates DionysusOutbox records and publishes them" do
                expect do
                  update_record
                end.to change { DionysusOutbox.count }.by(2)
                expect(created_outbox_record).to be_published
                expect(observer_outbox_record).to be_published
              end

              it "clears the publishable records storage" do
                update_record

                expect(resource.class.outbox_records_to_publish).to be_empty
              end
            end

            context "when :publish_after_commit is set to false" do
              it "creates DionysusOutbox records but does not publish them" do
                expect do
                  update_record
                end.to change { DionysusOutbox.count }.by(2)
                expect(created_outbox_record).not_to be_published
                expect(observer_outbox_record).not_to be_published
              end

              it "clears the publishable records storage" do
                update_record

                expect(resource.class.outbox_records_to_publish).to be_empty
              end
            end
          end
        end

        context "when transactional_outbox_enabled is set to false" do
          before do
            Dionysus::Producer.configuration.transactional_outbox_enabled = false
          end

          after do
            Dionysus::Producer.configuration.transactional_outbox_enabled = true
          end

          it { is_expected_block.not_to change { DionysusOutbox.count } }
        end
      end

      context "when the non-soft-deleted record gets restored" do
        subject(:update_record) { resource.update!(soft_delete_column => nil) }

        before do
          resource.update!(soft_delete_column => nil)
        end

        it { is_expected_block.not_to change { DionysusOutbox.count } }
      end

      context "when the soft-deleted record is properly updated" do
        context "when the updates are supposed to be published after soft delete" do
          subject(:update_record) { resource.update!(updated_at: Time.current) }

          let(:resource) do
            ExamplePublishablePublishAfterSoftDeletionResource.create!(soft_delete_column => Time.current)
          end

          context "when transactional_outbox_enabled is set to true" do
            let(:expected_event_name) { "example_publishable_publish_after_soft_deletion_resource_updated" }
            let(:expected_resource_class) { "ExamplePublishablePublishAfterSoftDeletionResource" }

            it "creates DionysusOutbox record with event_name with 'updated' event" do
              expect do
                update_record
              end.to change { DionysusOutbox.count }.by(1)
              expect(created_outbox_record.event_name).to eq expected_event_name
              expect(created_outbox_record.resource_class).to eq expected_resource_class
              expect(created_outbox_record.resource_id).to eq resource.id.to_s
              expect(created_outbox_record.topic).to eq "v102_rentals"
              expect(created_outbox_record.partition_key).to eq("0")
            end

            describe "publishing records after commit" do
              context "when :publish_after_commit is set to true", :publish_after_commit do
                it "creates DionysusOutbox records and publishes them" do
                  expect do
                    update_record
                  end.to change { DionysusOutbox.count }.by(1)
                  expect(created_outbox_record).to be_published
                end

                it "clears the publishable records storage" do
                  update_record

                  expect(resource.class.outbox_records_to_publish).to be_empty
                end
              end

              context "when :publish_after_commit is set to false" do
                it "creates DionysusOutbox records but does not publish them" do
                  expect do
                    update_record
                  end.to change { DionysusOutbox.count }.by(1)
                  expect(created_outbox_record).not_to be_published
                end

                it "clears the publishable records storage" do
                  update_record

                  expect(resource.class.outbox_records_to_publish).to be_empty
                end
              end
            end
          end

          context "when transactional_outbox_enabled is set to false" do
            before do
              Dionysus::Producer.configuration.transactional_outbox_enabled = false
            end

            after do
              Dionysus::Producer.configuration.transactional_outbox_enabled = true
            end

            it { is_expected_block.not_to change { DionysusOutbox.count } }
          end
        end

        context "when the updates are not supposed to be published after soft delete" do
          subject(:update_record) { resource.update!(updated_at: Time.current) }

          let(:resource) { ExamplePublishableCancelableResource.create!(soft_delete_column => Time.current) }

          it { is_expected_block.not_to change { DionysusOutbox.count } }
        end
      end
    end

    context "when resource is not soft-deletable" do
      let!(:resource) { ExamplePublishableResource.create! }

      context "when the record was just touched" do
        subject(:update_record) { resource.touch }

        it { is_expected_block.not_to change { DionysusOutbox.count } }
      end

      context "when the record was properly updated" do
        subject(:update_record) { resource.update!(updated_at: Time.current) }

        let(:other_created_outbox_record) { DionysusOutbox.last(2).first }

        context "when transactional_outbox_enabled is set to true" do
          it "creates DionysusOutbox records (for all topics) with event_name with 'updated' event" do
            expect do
              update_record
            end.to change { DionysusOutbox.count }.by(2)
            expect(created_outbox_record.event_name).to eq "example_publishable_resource_updated"
            expect(created_outbox_record.resource_class).to eq "ExamplePublishableResource"
            expect(created_outbox_record.resource_id).to eq resource.id.to_s
            expect(created_outbox_record.topic).to eq "v102_other"
            expect(created_outbox_record.partition_key).to eq("0")
            expect(other_created_outbox_record.event_name).to eq "example_publishable_resource_updated"
            expect(other_created_outbox_record.resource_class).to eq "ExamplePublishableResource"
            expect(other_created_outbox_record.resource_id).to eq resource.id.to_s
            expect(other_created_outbox_record.topic).to eq "v102_rentals"
            expect(other_created_outbox_record.partition_key).to eq("0")
          end

          describe "publishing records after commit" do
            context "when :publish_after_commit is set to true", :publish_after_commit do
              it "creates DionysusOutbox records and publishes them" do
                expect do
                  update_record
                end.to change { DionysusOutbox.count }.by(2)
                expect(created_outbox_record).to be_published
                expect(other_created_outbox_record).to be_published
              end

              it "clears the publishable records storage" do
                update_record

                expect(resource.class.outbox_records_to_publish).to be_empty
              end
            end

            context "when :publish_after_commit is set to false" do
              it "creates DionysusOutbox records but does not publish them" do
                expect do
                  update_record
                end.to change { DionysusOutbox.count }.by(2)
                expect(created_outbox_record).not_to be_published
                expect(other_created_outbox_record).not_to be_published
              end

              it "clears the publishable records storage" do
                update_record

                expect(resource.class.outbox_records_to_publish).to be_empty
              end
            end
          end
        end

        context "when transactional_outbox_enabled is set to false" do
          before do
            Dionysus::Producer.configuration.transactional_outbox_enabled = false
          end

          after do
            Dionysus::Producer.configuration.transactional_outbox_enabled = true
          end

          it { is_expected_block.not_to change { DionysusOutbox.count } }
        end
      end
    end
  end

  describe "after destroy" do
    subject(:destroy_record) { resource.destroy! }

    context "when transactional_outbox_enabled is set to true" do
      context "when the observer should be published", freeze_time: "2022-08-12T13:24:24.000Z" do
        subject(:destroy_record) { resource.destroy! }

        let!(:resource) { ExamplePublishableResource.create!(account_id: 1) }
        let(:created_outbox_record) { DionysusOutbox.find_by(topic: "v102_other") }
        let(:other_created_outbox_record) { DionysusOutbox.find_by(topic: "v102_rentals") }
        let(:observer_outbox_record) { DionysusOutbox.find_by(topic: "__outbox_observer__") }

        before do
          Dionysus::Producer.declare do
            namespace :v102 do
              serializer(Class.new do
                def self.serialize(records, dependencies:)
                  records.map { |record| record.as_json.except("created_at", "updated_at") }
                end
              end)

              topic :rentals do
                publish ExamplePublishableResource
                publish ExamplePublishableCancelableResource
                publish ExamplePublishablePublishAfterSoftDeletionResource
              end

              topic :other do
                publish ExamplePublishableResource
              end

              topic :observer_to_one do
                publish ExampleResource, observe: [
                  {
                    model: ExamplePublishableResource,
                    attributes: %i[id],
                    association_name: :example_resource
                  }
                ]
              end
            end
          end

          DionysusOutbox.delete_all
        end

        it "creates DionysusOutbox records (for all topics + the special for observers)
        with event_name with 'destroyed' event" do
          expect do
            destroy_record
          end.to change { DionysusOutbox.count }.by(3)
          expect(created_outbox_record.event_name).to eq "example_publishable_resource_destroyed"
          expect(created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(created_outbox_record.resource_id).to eq resource.id.to_s
          expect(created_outbox_record.topic).to eq "v102_other"
          expect(created_outbox_record.changeset).to eq({})
          expect(created_outbox_record.partition_key).to eq("1")
          expect(other_created_outbox_record.event_name).to eq "example_publishable_resource_destroyed"
          expect(other_created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(other_created_outbox_record.resource_id).to eq resource.id.to_s
          expect(other_created_outbox_record.topic).to eq "v102_rentals"
          expect(other_created_outbox_record.changeset).to eq({})
          expect(other_created_outbox_record.partition_key).to eq("1")
          expect(observer_outbox_record.event_name).to eq "example_publishable_resource_destroyed"
          expect(observer_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(observer_outbox_record.resource_id).to eq resource.id.to_s
          expect(observer_outbox_record.topic).to eq "__outbox_observer__"
          expect(observer_outbox_record.changeset).to eq(
            {
              "id" => [resource.id, nil],
              "created_at" => ["2022-08-12T13:24:24.000Z", nil]
            }
          )
          expect(observer_outbox_record.partition_key).to be_nil
        end
      end

      context "when no observer should be published (because the observed attributes haven't changed)" do
        let!(:resource) { ExamplePublishableResource.create! }
        let(:other_created_outbox_record) { DionysusOutbox.last(2).first }

        it "creates DionysusOutbox records (for all topics) with event_name with 'destroyed' event" do
          expect do
            destroy_record
          end.to change { DionysusOutbox.count }.by(2)
          expect(created_outbox_record.event_name).to eq "example_publishable_resource_destroyed"
          expect(created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(created_outbox_record.resource_id).to eq resource.id.to_s
          expect(created_outbox_record.topic).to eq "v102_other"
          expect(created_outbox_record.changeset).to eq({})
          expect(created_outbox_record.partition_key).to eq("0")
          expect(other_created_outbox_record.event_name).to eq "example_publishable_resource_destroyed"
          expect(other_created_outbox_record.resource_class).to eq "ExamplePublishableResource"
          expect(other_created_outbox_record.resource_id).to eq resource.id.to_s
          expect(other_created_outbox_record.topic).to eq "v102_rentals"
          expect(other_created_outbox_record.changeset).to eq({})
          expect(other_created_outbox_record.partition_key).to eq("0")
        end
      end

      describe "publishing records after commit" do
        let!(:resource) { ExamplePublishableResource.create! }
        let(:other_created_outbox_record) { DionysusOutbox.last(2).first }

        context "when :publish_after_commit is set to true", :publish_after_commit do
          it "creates DionysusOutbox records and publishes them" do
            expect do
              destroy_record
            end.to change { DionysusOutbox.count }.by(2)
            expect(created_outbox_record).to be_published
            expect(other_created_outbox_record).to be_published
          end

          it "clears the publishable records storage" do
            destroy_record

            expect(resource.class.outbox_records_to_publish).to be_empty
          end
        end

        context "when :publish_after_commit is set to false" do
          it "creates DionysusOutbox records but does not publish them" do
            expect do
              destroy_record
            end.to change { DionysusOutbox.count }.by(2)
            expect(created_outbox_record).not_to be_published
            expect(other_created_outbox_record).not_to be_published
          end

          it "clears the publishable records storage" do
            destroy_record

            expect(resource.class.outbox_records_to_publish).to be_empty
          end
        end
      end
    end

    context "when transactional_outbox_enabled is set to false" do
      let!(:resource) { ExamplePublishableResource.create!(account_id: 1) }

      before do
        Dionysus::Producer.configuration.transactional_outbox_enabled = false
      end

      after do
        Dionysus::Producer.configuration.transactional_outbox_enabled = true
      end

      it { is_expected_block.not_to change { DionysusOutbox.count } }
    end
  end

  describe "publishing messages after commit" do
    describe "ordering of the messages" do
      subject(:create_record) { resource.save! }

      let(:resource) { ExamplePublishableResource.new }
      let(:other_resource) { ExamplePublishableResource.new(created_at: 1.week.from_now) }
      let(:other_created_outbox_record) { DionysusOutbox.last(2).first }
      let(:outbox_record_for_other_resource) { DionysusOutbox.new(resource: other_resource) }
      let(:records_processor) { Dionysus::Producer::Outbox::RecordsProcessor.new }

      before do
        resource.class.add_outbox_records_to_publish([outbox_record_for_other_resource])
        allow(resource).to receive(:records_processor).and_return(records_processor)
        allow(records_processor).to receive(:call).and_call_original
      end

      it "sorts messages by resource's created at, not by the outbox entry created_at", :freeze_time,
        :publish_after_commit do
        create_record

        expect(records_processor).to have_received(:call)
          .with([other_created_outbox_record, created_outbox_record, outbox_record_for_other_resource])
      end
    end
  end
end
