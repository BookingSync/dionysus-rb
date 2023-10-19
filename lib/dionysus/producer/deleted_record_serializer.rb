# frozen_string_literal: true

class Dionysus::Producer::DeletedRecordSerializer < Dionysus::Producer::ModelSerializer
  def as_json
    super.merge(primary_key => primary_key_value)
  end

  private

  def primary_key
    record.class.primary_key
  end

  def primary_key_value
    record.public_send(primary_key)
  end
end
