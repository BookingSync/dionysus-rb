# frozen_string_literal: true

class ModelFactoryForKarafkaConsumerTest
  SUPPORTED_MODELS = %w[Rental Client Booking BookingsFee Tax Fee Account].freeze

  def for_model(model_name)
    model_name = model_name.to_s.singularize.classify
    ModelRepositoryForKarafkaConsumerTest.new(model_name) if SUPPORTED_MODELS.include?(model_name)
  end
end

class ModelRepositoryForKarafkaConsumerTest
  attr_reader :model_name
  private     :model_name

  def self.dionysus_import(data)
    @dionysus_import_data = data
  end

  class << self
    attr_reader :dionysus_import_data
  end

  def self.dionysus_destroy(data)
    @dionysus_destroy_data = data
  end

  class << self
    attr_reader :dionysus_destroy_data
  end

  def initialize(model_name)
    @model_name = model_name
  end

  def find_or_initialize_by(attributes)
    all.find do |record|
      attributes.all? { |attribute, value| record.public_send(attribute) == value }
    end || initialize_model(attributes)
  end

  def all
    DBForKarafkaConsumerTest.public_send(model_name.downcase.pluralize)
  end

  def initialize_model(attributes)
    Object.const_get("#{model_name}ForKarafkaConsumerTest").new(attributes).tap do |model|
      all << model
    end
  end

  def dionysus_import(data)
    self.class.dionysus_import(data)
  end

  def dionysus_destroy(data)
    self.class.dionysus_destroy(data)
  end
end

class DBForKarafkaConsumerTest
  def self.rentals
    @rentals ||= []
  end

  def self.bookings
    @bookings ||= []
  end

  def self.clients
    @clients ||= []
  end

  def self.bookings_fees
    @bookings_fees ||= []
  end

  def self.taxes
    @taxes ||= []
  end

  def self.fees
    @fees ||= []
  end

  def self.accounts
    @accounts ||= []
  end

  def self.reset!
    @rentals = []
    @bookings = []
    @clients = []
    @bookings_fees = []
    @taxes = []
    @fees = []
    @accounts = []
  end
end

class BaseModelClassForKarafkaConsumerTest
  attr_reader :saved, :destroyed

  attr_accessor :synced_data

  def initialize(attributes = {})
    @saved = false
    @destroyed = false
    @old_attributes = {}
    assign_attributes(attributes)
  end

  def assign_attributes(attributes)
    raise "attributes must be Hash" unless attributes.respond_to?(:to_hash)

    attributes.each do |key, value|
      @old_attributes[key.to_s] = public_send(key)
      public_send("#{key}=", value)
    end
  end

  def save
    @saved = true
    @old_attributes = {}
  end

  def update!(attributes)
    assign_attributes(attributes)
  end

  def destroy
    @destroyed = true
  end

  def destroyed?
    @destroyed
  end

  def changes
    @old_attributes.reject { |key, value| value == public_send(key) }
      .to_h { |key, value| [key, [value, public_send(key)]] }
  end
end

module ModelAttributesForKarafkaConsumerTest
  attr_accessor :synced_id, :synced_created_at, :synced_updated_at
end

class RentalForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  attr_accessor :name, :rental_type, :synced_canceled_at, :bookings, :tax, :account

  attr_reader :canceled, :to_many_associations_resolutions, :to_one_associations_resolutions

  def model_name
    "Rental"
  end

  def initialize(*)
    super
    @canceled = false
    @to_one_associations_resolutions = {}
    @to_many_associations_resolutions = {}
  end

  def cancel
    @canceled = true
  end

  def resolve_to_one_association(name, id)
    @to_one_associations_resolutions[name] = id
  end

  def resolve_to_many_association(name, ids)
    @to_many_associations_resolutions[name] = ids
  end
end

class RentalWithModifiedTimestampsForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  attr_accessor :synced_id, :bookingsync_created_at, :bookingsync_updated_at, :bookingsync_canceled_at,
    :bookingsync_data

  def model_name
    "Rental"
  end
end

class RentalWithModifiedTimestampsWithoutCanceledAtForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  attr_accessor :synced_id, :bookingsync_created_at, :bookingsync_updated_at, :bookingsync_data

  def model_name
    "Rental"
  end

  def cancel
    @canceled = true
  end

  def canceled?
    !!@canceled
  end
end

class RentalWithoutSyncedUpdatedAtForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  attr_accessor :synced_id, :synced_created_at, :name

  attr_reader :canceled

  def model_name
    "Rental"
  end

  def initialize(*)
    super
    @canceled = false
  end

  def cancel
    @canceled = true
  end
end

class BookingForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  attr_accessor :synced_id, :start_at

  def model_name
    "Booking"
  end
end

class BookingsFeeForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  def model_name
    "BookingsFee"
  end
end

class ClientForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  def model_name
    "Client"
  end
end

class TaxForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  attr_accessor :name

  def model_name
    "Tax"
  end
end

class FeeForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  attr_accessor :name

  def model_name
    "Fee"
  end
end

class AccountForKarafkaConsumerTest < BaseModelClassForKarafkaConsumerTest
  include ModelAttributesForKarafkaConsumerTest

  attr_accessor :name

  def model_name
    "Account"
  end
end
