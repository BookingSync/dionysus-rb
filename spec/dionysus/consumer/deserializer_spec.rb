# frozen_string_literal: true

require "spec_helper"

RSpec.describe Dionysus::Consumer::Deserializer do
  describe "#deserialize" do
    subject(:deserialize) { described_class.new(data).deserialize }

    context "when data is present" do
      let(:data) do
        [
          {
            "links" => {
              "account" => 2,
              "fee" => 10,
              "bookings" => [3, 4],
              "rental" => nil,
              "taxes" => [],
              "clients" => [20],
              "users" => [],
              "bookable" => {
                "type" => "Property",
                "id" => 123
              },
              "empty_bookable" => {}
            },
            "id" => 1,
            "name" => "BookingSync",
            "created_at" => "2020-01-01 12:00:00",
            "updated_at" => "2020-01-02 12:00:01",
            "canceled_at" => "2020-01-02 12:00:02",
            "comments" => [{ "content" => "comment" }],
            "settings" => { "default_arrival_time" => 11 },
            "fee" => {
              "links" => {
                "rentals_fee" => 101
              },
              "id" => 10,
              "name" => "cleaning",
              "rentals_fee" => {
                "id" => 101,
                "always_applied" => true,
                "created_at" => "2021-01-01 12:00:00",
                "updated_at" => "2021-01-02 12:00:01",
                "canceled_at" => "2021-01-02 12:00:02"
              }
            },
            "bookable" => {
              "links" => {},
              "id" => 123,
              "synced_created_at" => "2100-01-01 12:00:00"
            },
            "clients" => [
              {
                "links" => {
                  "messages" => [201]
                },
                "id" => 20,
                "fullname" => "Rich Piana",
                "messages" => [
                  {
                    "id" => 201,
                    "message" => "inbox message"
                  }
                ]
              }
            ]
          }
        ]
      end

      let(:deserialized_canonical_format) do
        [
          attributes: {
            "comments" => [{ "content" => "comment" }],
            "name" => "BookingSync",
            "settings" => { "default_arrival_time" => 11 },
            "synced_account_id" => 2,
            "synced_created_at" => "2020-01-01 12:00:00",
            "synced_fee_id" => 10,
            "synced_id" => 1,
            "synced_rental_id" => nil,
            "synced_bookable_id" => 123,
            "synced_bookable_type" => "Property",
            "synced_empty_bookable_id" => nil,
            "synced_empty_bookable_type" => nil,
            "synced_updated_at" => "2020-01-02 12:00:01",
            "synced_canceled_at" => "2020-01-02 12:00:02",
            "synced_booking_ids" => [3, 4],
            "synced_client_ids" => [20],
            "synced_tax_ids" => [],
            "synced_user_ids" => []
          },
          has_many: [
            ["bookings", nil],
            ["taxes", nil],
            [
              "clients",
              [
                {
                  attributes: { "synced_message_ids" => [201], "fullname" => "Rich Piana", "synced_id" => 20 },
                  has_many: [
                    [
                      "messages",
                      [
                        {
                          attributes: { "message" => "inbox message", "synced_id" => 201 },
                          has_many: [],
                          has_one: []
                        }
                      ]
                    ]
                  ],
                  has_one: []
                }
              ]
            ],
            ["users", nil]
          ],
          has_one: [
            ["account", nil],
            [
              "fee",
              {
                attributes: { "name" => "cleaning", "synced_id" => 10, "synced_rentals_fee_id" => 101 },
                has_many: [],
                has_one: [
                  [
                    "rentals_fee",
                    {
                      attributes: {
                        "always_applied" => true,
                        "synced_created_at" => "2021-01-01 12:00:00",
                        "synced_id" => 101,
                        "synced_updated_at" => "2021-01-02 12:00:01",
                        "synced_canceled_at" => "2021-01-02 12:00:02"
                      },
                      has_many: [],
                      has_one: []
                    }
                  ]
                ]
              }
            ],
            ["rental", nil],
            [
              "Property",
              {
                attributes: { "synced_id" => 123, "synced_created_at" => "2100-01-01 12:00:00" },
                has_many: [],
                has_one: []
              }
            ],
            ["empty_bookable", nil]
          ]
        ]
      end

      it { is_expected.to eq deserialized_canonical_format }
    end

    context "when data is nil" do
      let(:data) { nil }

      it { is_expected.to eq [] }
    end
  end
end
