# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Producer::KarafkaResponderGenerator do
  describe "#generate" do
    subject(:generate) { described_class.new.generate(config, topic) }

    let(:config) do
      Dionysus::Producer::Config.new
    end
    let(:topic) do
      Dionysus::Producer::Registry::Registration::Topic.new(
        :v8, "rentals", serializer, partition_key: :account_id, genesis_replica: genesis_replica
      )
    end
    let(:genesis_replica) { true }
    let(:serializer) do
      Class.new do
        def self.serialize(records, dependencies:)
          {
            records: records.map { |record| { id: record.id, name: record.name } },
            dependencies: dependencies
          }
        end
      end
    end
    let(:rental_1) do
      double(id: 1, name: "Villa Saganaki", model_name: double(name: "Rental"), class: "Rental")
    end
    let(:rental_2) { double(id: 2, name: "Villa Sivota", model_name: double(name: "Rental"), class: "Rental") }
    let(:tax) { double(id: 3, name: "VAT", model_name: double(name: "Tax"), class: "Tax") }

    before do
      topic.publish("Rental", with: ["RentalsFee"])
      topic.publish("Tax")
    end

    describe "responder class" do
      context "when genesis is set to true" do
        let(:genesis_replica) { true }

        it "generates responder class for a given topic" do
          responder_klass = generate

          expect(Dionysus::V8RentalResponder).to be_present
          expect(responder_klass).to eq Dionysus::V8RentalResponder
          expect(responder_klass.superclass).to eq Dionysus::Producer::BaseResponder

          expect(responder_klass.publisher_of?("Rental")).to be true
          expect(responder_klass.publisher_for_topic?("v8_rentals")).to be true
          expect(responder_klass.publisher_for_topic?("v8_rentals_genesis")).to be true
          expect(responder_klass.publisher_for_topic?("v1_rentals")).to be false
          expect(responder_klass.publisher_for_topic?("v1_rentals_genesis")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v8_rentals")).to be true
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v8_rentals_genesis")).to be true
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v1_rentals")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v1_rentals_genesis")).to be false
          expect(responder_klass.publisher_of?("Tax")).to be true
          expect(responder_klass.publisher_for_topic?("v1_rentals")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v8_rentals")).to be true
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v8_rentals_genesis")).to be true
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v1_rentals")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v1_rentals_genesis")).to be false
          expect(responder_klass.publisher_of?("Booking")).to be false
          expect(responder_klass.primary_topic).to eq("v8_rentals")
          expect(responder_klass.topics.keys).to eq %w[v8_rentals v8_rentals_genesis]
        end
      end

      context "when genesis is not set to true" do
        let(:genesis_replica) { false }

        it "generates responder class for a given topic" do
          responder_klass = generate

          expect(Dionysus::V8RentalResponder).to be_present
          expect(responder_klass).to eq Dionysus::V8RentalResponder
          expect(responder_klass.superclass).to eq Dionysus::Producer::BaseResponder

          expect(responder_klass.publisher_of?("Rental")).to be true
          expect(responder_klass.publisher_for_topic?("v8_rentals")).to be true
          expect(responder_klass.publisher_for_topic?("v8_rentals_genesis")).to be false
          expect(responder_klass.publisher_for_topic?("v1_rentals")).to be false
          expect(responder_klass.publisher_for_topic?("v1_rentals_false")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v8_rentals")).to be true
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v8_rentals_genesis")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v1_rentals")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Rental", "v1_rentals_genesis")).to be false
          expect(responder_klass.publisher_of?("Tax")).to be true
          expect(responder_klass.publisher_for_topic?("v1_rentals")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v8_rentals")).to be true
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v8_rentals_genesis")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v1_rentals")).to be false
          expect(responder_klass.publisher_of_model_for_topic?("Tax", "v1_rentals_genesis")).to be false
          expect(responder_klass.publisher_of?("Booking")).to be false
          expect(responder_klass.primary_topic).to eq("v8_rentals")
          expect(responder_klass.topics.keys).to eq ["v8_rentals"]
        end
      end
    end

    describe "publishing" do
      describe "consistent snapshot instrumentation" do
        subject(:call) { responder.call([event_1, event_2], partition_key: "Smily", key: "#WhateverItTakes") }

        let(:responder) { generate.new }
        let(:event_1) { ["rental_created", [rental_1, rental_2]] }
        let(:event_2) { ["tax_created", [tax]] }
        let(:increments) { [] }

        before do
          recorded = increments
          config.instrumenter = Class.new do
            define_singleton_method(:instrument) { |_name, _payload = {}, &block| block.call }
            define_singleton_method(:increment) { |name, options = {}| recorded << [name, options.fetch(:tags, [])] }
          end
        end

        def snapshot_tags
          increments.filter_map { |name, tags| tags if name == "dionysus.publish.consistent_snapshot" }
        end

        context "when publish_consistent_snapshots is disabled" do
          before { config.publish_consistent_snapshots = false }

          it "emits nothing, so the metric's absence means the guard is off rather than passing" do
            call

            expect(snapshot_tags).to be_empty
          end
        end

        context "when publish_consistent_snapshots is enabled" do
          before { config.publish_consistent_snapshots = true }

          it "reports one result per event, tagged unsupported for records carrying no updated_at" do
            call

            expect(snapshot_tags.size).to eq 2
            expect(snapshot_tags.first).to include("result:unsupported", "attempts:1", "topic:v8_rentals")
            expect(snapshot_tags.last).to include("result:unsupported", "attempts:1", "topic:v8_rentals")
          end
        end
      end

      describe "responding to Kafka" do
        let(:responder_klass) { generate }
        let(:responder) { responder_klass.new }

        let(:event_1) { ["rental_created", [rental_1, rental_2]] }
        let(:event_2) { ["tax_created", [tax]] }

        context "when there is genesis topic" do
          let(:genesis_replica) { true }

          context "when there is a flag to publish only for genesis" do
            subject(:call) do
              responder.call([event_1, event_2], partition_key: "Smily", key: "#WhateverItTakes", genesis_only: true)
            end

            context "when there is no :serialize option specified" do
              let(:event_1) { ["rental_created", [rental_1, rental_2]] }
              let(:event_2) { ["tax_created", [tax]] }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals_genesis" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements expected by Karafka, serializes events batches
            and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            context "when :serialize is set to true" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: true }] }
              let(:event_2) { ["tax_created", [tax], { serialize: true }] }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals_genesis" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements interface expected by Karafka, serializes events batches
          and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            context "when :serialize is set to false on some of the events" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: false }] }
              let(:event_2) { ["tax_created", [tax]] }
              let(:rental_1) { double(model_name: double(name: "Rental"), as_json: { id: 1 }) }
              let(:rental_2) { double(model_name: double(name: "Rental"), as_json: { id: 2 }) }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals_genesis" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => [{ "id" => 1 }, { "id" => 2 }]
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements interface expected by Karafka,
          bypasses serialization via serializer if serialize is set to false and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            describe "instrumentation" do
              context "when instrumenter is specified" do
                let(:instrumenter) do
                  Class.new do
                    attr_reader :registrations

                    def initialize
                      @registrations = Hash.new { |hash, key| hash[key] = [] }
                    end

                    def instrument(name, payload = {})
                      registrations[name] << payload
                      yield
                    end
                  end.new
                end
                let(:expected_registrations) do
                  {
                    "dionysus.respond.Dionysus::V8RentalResponder" => [{}]
                  }
                end

                before do
                  config.instrumenter = instrumenter
                end

                it "uses that instrumenter for instrumentation" do
                  expect do
                    call
                  end.to change { instrumenter.registrations }.from({}).to(expected_registrations)
                end
              end

              context "when instrumenter is not specified" do
                it "does not blow up" do
                  expect do
                    call
                  end.not_to raise_error
                end
              end
            end

            describe "publishing events via EventBus" do
              context "when event bus is specified" do
                let(:event_bus) do
                  Class.new do
                    attr_reader :events

                    def initialize
                      @events = []
                    end

                    def publish(event_name, payload = {})
                      events << [event_name, payload]
                    end
                  end.new
                end
                let(:expected_events) do
                  [
                    [
                      "dionysus.respond",
                      {
                        topic_name: "v8_rentals_genesis",
                        message: [
                          {
                            event: "rental_created",
                            model_name: "Rental",
                            data: {
                              records: [
                                {
                                  id: 1,
                                  name: "Villa Saganaki"
                                },
                                {
                                  id: 2,
                                  name: "Villa Sivota"
                                }
                              ],
                              dependencies: ["RentalsFee"]
                            }
                          },
                          {
                            event: "tax_created",
                            model_name: "Tax",
                            data: {
                              records: [
                                {
                                  id: 3,
                                  name: "VAT"
                                }
                              ],
                              dependencies: []
                            }
                          }
                        ],
                        options: {
                          partition_key: "Smily",
                          key: "#WhateverItTakes"
                        }
                      }
                    ]
                  ]
                end

                before do
                  config.event_bus = event_bus
                end

                it "uses that event bus for publishing events" do
                  expect do
                    call
                  end.to change { event_bus.events }.from([]).to(expected_events)
                end
              end

              context "when event bus is not specified" do
                it "does not blow up" do
                  expect do
                    call
                  end.not_to raise_error
                end
              end
            end

            describe "publishing tombstones" do
              subject(:call) do
                responder.call(nil, partition_key: "Smily", key: "#WhateverItTakes", genesis_only: true)
              end

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals_genesis" => [["null",
                    { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }]]
                }
              end
              let(:event_bus) do
                Class.new do
                  attr_reader :events

                  def initialize
                    @events = []
                  end

                  def publish(event_name, payload = {})
                    events << [event_name, payload]
                  end
                end.new
              end
              let(:expected_events) do
                [
                  ["dionysus.respond",
                    {
                      topic_name: "v8_rentals_genesis",
                      message: nil,
                      options: { key: "#WhateverItTakes", partition_key: "Smily" }
                    }]
                ]
              end

              before do
                config.event_bus = event_bus
              end

              it "publishes a tombstone (i.e. nil)" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end

              it "publishes this message to event bus" do
                expect do
                  call
                end.to change { event_bus.events }.from([]).to(expected_events)
              end
            end
          end

          context "when there is no flag to publish only for genesis" do
            subject(:call) do
              responder.call([event_1, event_2], partition_key: "Smily", key: "#WhateverItTakes")
            end

            context "when there is no :serialize option specified" do
              let(:event_1) { ["rental_created", [rental_1, rental_2]] }
              let(:event_2) { ["tax_created", [tax]] }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }
                    ]
                  ],
                  "v8_rentals_genesis" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements expected by Karafka, serializes events batches
            and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            context "when :serialize is set to true" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: true }] }
              let(:event_2) { ["tax_created", [tax], { serialize: true }] }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }
                    ]
                  ],
                  "v8_rentals_genesis" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements interface expected by Karafka, serializes events batches
          and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            context "when :serialize is set to false on some of the events" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: false }] }
              let(:event_2) { ["tax_created", [tax]] }
              let(:rental_1) { double(model_name: double(name: "Rental"), as_json: { id: 1 }) }
              let(:rental_2) { double(model_name: double(name: "Rental"), as_json: { id: 2 }) }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => [{ "id" => 1 }, { "id" => 2 }]
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }
                    ]
                  ],
                  "v8_rentals_genesis" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => [{ "id" => 1 }, { "id" => 2 }]
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements interface expected by Karafka,
          bypasses serialization via serializer if serialize is set to false and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            describe "instrumentation" do
              context "when instrumenter is specified" do
                let(:instrumenter) do
                  Class.new do
                    attr_reader :registrations

                    def initialize
                      @registrations = Hash.new { |hash, key| hash[key] = [] }
                    end

                    def instrument(name, payload = {})
                      registrations[name] << payload
                      yield
                    end
                  end.new
                end
                let(:expected_registrations) do
                  {
                    "dionysus.respond.Dionysus::V8RentalResponder" => [{}]
                  }
                end

                before do
                  config.instrumenter = instrumenter
                end

                it "uses that instrumenter for instrumentation" do
                  expect do
                    call
                  end.to change { instrumenter.registrations }.from({}).to(expected_registrations)
                end
              end

              context "when instrumenter is not specified" do
                it "does not blow up" do
                  expect do
                    call
                  end.not_to raise_error
                end
              end
            end

            describe "publishing events via EventBus" do
              context "when event bus is specified" do
                let(:event_bus) do
                  Class.new do
                    attr_reader :events

                    def initialize
                      @events = []
                    end

                    def publish(event_name, payload = {})
                      events << [event_name, payload]
                    end
                  end.new
                end
                let(:expected_events) do
                  [
                    [
                      "dionysus.respond",
                      {
                        topic_name: "v8_rentals",
                        message: [
                          {
                            event: "rental_created",
                            model_name: "Rental",
                            data: {
                              records: [
                                {
                                  id: 1,
                                  name: "Villa Saganaki"
                                },
                                {
                                  id: 2,
                                  name: "Villa Sivota"
                                }
                              ],
                              dependencies: ["RentalsFee"]
                            }
                          },
                          {
                            event: "tax_created",
                            model_name: "Tax",
                            data: {
                              records: [
                                {
                                  id: 3,
                                  name: "VAT"
                                }
                              ],
                              dependencies: []
                            }
                          }
                        ],
                        options: {
                          partition_key: "Smily",
                          key: "#WhateverItTakes"
                        }
                      }
                    ],
                    [
                      "dionysus.respond",
                      {
                        topic_name: "v8_rentals_genesis",
                        message: [
                          {
                            event: "rental_created",
                            model_name: "Rental",
                            data: {
                              records: [
                                {
                                  id: 1,
                                  name: "Villa Saganaki"
                                },
                                {
                                  id: 2,
                                  name: "Villa Sivota"
                                }
                              ],
                              dependencies: ["RentalsFee"]
                            }
                          },
                          {
                            event: "tax_created",
                            model_name: "Tax",
                            data: {
                              records: [
                                {
                                  id: 3,
                                  name: "VAT"
                                }
                              ],
                              dependencies: []
                            }
                          }
                        ],
                        options: {
                          partition_key: "Smily",
                          key: "#WhateverItTakes"
                        }
                      }
                    ]
                  ]
                end

                before do
                  config.event_bus = event_bus
                end

                it "uses that event bus for publishing events" do
                  expect do
                    call
                  end.to change { event_bus.events }.from([]).to(expected_events)
                end
              end

              context "when event bus is not specified" do
                it "does not blow up" do
                  expect do
                    call
                  end.not_to raise_error
                end
              end
            end

            describe "publishing tombstones" do
              subject(:call) do
                responder.call(nil, partition_key: "Smily", key: "#WhateverItTakes")
              end

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [["null", { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }]],
                  "v8_rentals_genesis" => [["null",
                    { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals_genesis" }]]
                }
              end
              let(:event_bus) do
                Class.new do
                  attr_reader :events

                  def initialize
                    @events = []
                  end

                  def publish(event_name, payload = {})
                    events << [event_name, payload]
                  end
                end.new
              end
              let(:expected_events) do
                [
                  ["dionysus.respond",
                    {
                      topic_name: "v8_rentals",
                      message: nil,
                      options: { key: "#WhateverItTakes", partition_key: "Smily" }
                    }],
                  ["dionysus.respond",
                    {
                      topic_name: "v8_rentals_genesis",
                      message: nil,
                      options: { key: "#WhateverItTakes", partition_key: "Smily" }
                    }]
                ]
              end

              before do
                config.event_bus = event_bus
              end

              it "publishes a tombstone (i.e. nil)" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end

              it "publishes this message to event bus" do
                expect do
                  call
                end.to change { event_bus.events }.from([]).to(expected_events)
              end
            end
          end
        end

        context "when there is no genesis topic" do
          let(:genesis_replica) { false }

          context "when there is a flag to publish only for genesis" do
            subject(:call) do
              responder.call([event_1, event_2], partition_key: "Smily", key: "#WhateverItTakes", genesis_only: true)
            end

            context "when there is no :serialize option specified" do
              let(:event_1) { ["rental_created", [rental_1, rental_2]] }
              let(:event_2) { ["tax_created", [tax]] }

              it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
            end

            context "when :serialize is set to true" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: true }] }
              let(:event_2) { ["tax_created", [tax], { serialize: true }] }

              it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
            end

            context "when :serialize is set to false on some of the events" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: false }] }
              let(:event_2) { ["tax_created", [tax]] }
              let(:rental_1) { double(model_name: double(name: "Rental"), as_json: { id: 1 }) }
              let(:rental_2) { double(model_name: double(name: "Rental"), as_json: { id: 2 }) }

              it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
            end

            describe "instrumentation" do
              context "when instrumenter is specified" do
                let(:instrumenter) do
                  Class.new do
                    attr_reader :registrations

                    def initialize
                      @registrations = Hash.new { |hash, key| hash[key] = [] }
                    end

                    def instrument(name, payload = {})
                      registrations[name] << payload
                      yield
                    end
                  end.new
                end
                let(:expected_registrations) do
                  {
                    "dionysus.respond.Dionysus::V8RentalResponder" => [{}]
                  }
                end

                before do
                  config.instrumenter = instrumenter
                end

                it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
              end

              context "when instrumenter is not specified" do
                it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
              end
            end

            describe "publishing events via EventBus" do
              context "when event bus is specified" do
                let(:event_bus) do
                  Class.new do
                    attr_reader :events

                    def initialize
                      @events = []
                    end

                    def publish(event_name, payload = {})
                      events << [event_name, payload]
                    end
                  end.new
                end

                before do
                  config.event_bus = event_bus
                end

                it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
              end

              context "when event bus is not specified" do
                it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
              end
            end

            describe "publishing tombstones" do
              subject(:call) do
                responder.call(nil, partition_key: "Smily", key: "#WhateverItTakes", genesis_only: true)
              end

              let(:event_bus) do
                Class.new do
                  attr_reader :events

                  def initialize
                    @events = []
                  end

                  def publish(event_name, payload = {})
                    events << [event_name, payload]
                  end
                end.new
              end

              before do
                config.event_bus = event_bus
              end

              it { is_expected_block.to raise_error(%r{cannot execute genesis}) }
            end
          end

          context "when there is no flag to publish only for genesis" do
            subject(:call) do
              responder.call([event_1, event_2], partition_key: "Smily", key: "#WhateverItTakes")
            end

            context "when there is no :serialize option specified" do
              let(:event_1) { ["rental_created", [rental_1, rental_2]] }
              let(:event_2) { ["tax_created", [tax]] }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements expected by Karafka, serializes events batches
              and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            context "when :serialize is set to true" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: true }] }
              let(:event_2) { ["tax_created", [tax], { serialize: true }] }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 1,
                                  "name" => "Villa Saganaki"
                                },
                                {
                                  "id" => 2,
                                  "name" => "Villa Sivota"
                                }
                              ],
                              "dependencies" => ["RentalsFee"]
                            }
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements interface expected by Karafka, serializes events batches
            and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            context "when :serialize is set to false on some of the events" do
              let(:event_1) { ["rental_created", [rental_1, rental_2], { serialize: false }] }
              let(:event_2) { ["tax_created", [tax]] }
              let(:rental_1) { double(model_name: double(name: "Rental"), as_json: { id: 1 }) }
              let(:rental_2) { double(model_name: double(name: "Rental"), as_json: { id: 2 }) }

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [
                    [
                      {
                        "message" => [
                          {
                            "event" => "rental_created",
                            "model_name" => "Rental",
                            "data" => [{ "id" => 1 }, { "id" => 2 }]
                          },
                          {
                            "event" => "tax_created",
                            "model_name" => "Tax",
                            "data" => {
                              "records" => [
                                {
                                  "id" => 3,
                                  "name" => "VAT"
                                }
                              ],
                              "dependencies" => []
                            }
                          }
                        ]
                      }.to_json,
                      { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }
                    ]
                  ]
                }
              end

              it "generates responder class that implements interface expected by Karafka,
            bypasses serialization via serializer if serialize is set to false and publishes them to Kafka" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end
            end

            describe "instrumentation" do
              context "when instrumenter is specified" do
                let(:instrumenter) do
                  Class.new do
                    attr_reader :registrations

                    def initialize
                      @registrations = Hash.new { |hash, key| hash[key] = [] }
                    end

                    def instrument(name, payload = {})
                      registrations[name] << payload
                      yield
                    end
                  end.new
                end
                let(:expected_registrations) do
                  {
                    "dionysus.respond.Dionysus::V8RentalResponder" => [{}]
                  }
                end

                before do
                  config.instrumenter = instrumenter
                end

                it "uses that instrumenter for instrumentation" do
                  expect do
                    call
                  end.to change { instrumenter.registrations }.from({}).to(expected_registrations)
                end
              end

              context "when instrumenter is not specified" do
                it "does not blow up" do
                  expect do
                    call
                  end.not_to raise_error
                end
              end
            end

            describe "publishing events via EventBus" do
              context "when event bus is specified" do
                let(:event_bus) do
                  Class.new do
                    attr_reader :events

                    def initialize
                      @events = []
                    end

                    def publish(event_name, payload = {})
                      events << [event_name, payload]
                    end
                  end.new
                end
                let(:expected_events) do
                  [
                    [
                      "dionysus.respond",
                      {
                        topic_name: "v8_rentals",
                        message: [
                          {
                            event: "rental_created",
                            model_name: "Rental",
                            data: {
                              records: [
                                {
                                  id: 1,
                                  name: "Villa Saganaki"
                                },
                                {
                                  id: 2,
                                  name: "Villa Sivota"
                                }
                              ],
                              dependencies: ["RentalsFee"]
                            }
                          },
                          {
                            event: "tax_created",
                            model_name: "Tax",
                            data: {
                              records: [
                                {
                                  id: 3,
                                  name: "VAT"
                                }
                              ],
                              dependencies: []
                            }
                          }
                        ],
                        options: {
                          partition_key: "Smily",
                          key: "#WhateverItTakes"
                        }
                      }
                    ]
                  ]
                end

                before do
                  config.event_bus = event_bus
                end

                it "uses that event bus for publishing events" do
                  expect do
                    call
                  end.to change { event_bus.events }.from([]).to(expected_events)
                end
              end

              context "when event bus is not specified" do
                it "does not blow up" do
                  expect do
                    call
                  end.not_to raise_error
                end
              end
            end

            describe "publishing tombstones" do
              subject(:call) do
                responder.call(nil, partition_key: "Smily", key: "#WhateverItTakes")
              end

              let(:expected_payload_to_be_published) do
                {
                  "v8_rentals" => [["null", { partition_key: "Smily", key: "#WhateverItTakes", topic: "v8_rentals" }]]
                }
              end
              let(:event_bus) do
                Class.new do
                  attr_reader :events

                  def initialize
                    @events = []
                  end

                  def publish(event_name, payload = {})
                    events << [event_name, payload]
                  end
                end.new
              end
              let(:expected_events) do
                [
                  ["dionysus.respond",
                    {
                      topic_name: "v8_rentals",
                      message: nil,
                      options: { key: "#WhateverItTakes", partition_key: "Smily" }
                    }]
                ]
              end

              before do
                config.event_bus = event_bus
              end

              it "publishes a tombstone (i.e. nil)" do
                call

                expect(responder.messages_buffer).to eq expected_payload_to_be_published
              end

              it "publishes this message to event bus" do
                expect do
                  call
                end.to change { event_bus.events }.from([]).to(expected_events)
              end
            end
          end
        end
      end
    end

    describe "stamping serialized_at" do
      subject(:call) { responder.call([event], partition_key: "Smily", key: "#WhateverItTakes") }

      let(:responder) { generate.new }
      let(:genesis_replica) { false }
      let(:event) { ["rental_created", [rental_1]] }
      let(:published_event) do
        JSON.parse(responder.messages_buffer.fetch("v8_rentals").first.first).fetch("message").first
      end

      context "when the stamp is disabled" do
        it "leaves the published envelope untouched" do
          call

          expect(published_event).not_to have_key("serialized_at")
        end
      end

      context "when the payload is read after the stamp is taken" do
        # ordering duplicates by the stamp is only sound while every read feeding the payload
        # happens at or after it - a pre-loaded association would make the data older than its stamp
        let(:read_at) { [] }
        let(:serializer) do
          times = read_at
          Class.new do
            define_singleton_method(:serialize) do |records, dependencies:|
              times << Time.now.utc
              { records: records.map { |record| { id: record.id } }, dependencies: dependencies }
            end
          end
        end

        before { config.include_serialized_at_in_payload = true }

        it "never stamps later than the read it describes" do
          call

          expect(Time.parse(published_event.fetch("serialized_at"))).to be <= read_at.first
        end
      end

      context "when the stamp is enabled", :freeze_time do
        before { config.include_serialized_at_in_payload = true }

        it "records when the payload was serialized, to microseconds" do
          call

          aggregate_failures do
            expect(published_event.fetch("serialized_at")).to eq Time.now.utc.iso8601(6)
            expect(published_event.fetch("event")).to eq "rental_created"
            expect(published_event.fetch("data")).to be_present
          end
        end
      end
    end
  end
end
