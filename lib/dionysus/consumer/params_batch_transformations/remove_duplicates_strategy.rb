# frozen_string_literal: true

require "time"

class Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy
  # messages produced before "serialized_at" existed sort below every stamped message
  FALLBACK_SERIALIZED_AT = Time.at(0).utc.freeze

  def call(params_batch)
    return params_batch if duplicates_removal_not_applicable?(params_batch)

    Karafka::Messages::Messages.new(transform_messages_array(params_batch), params_batch.metadata)
  end

  private

  # the idea is following:
  # 1. group messages by event and id - for a given model we can expect unique messages for _created and _deleted events
  #   but we can have multiple _updated events, so this is where we are interested in removing duplicates
  # 2. from each group keep the FRESHEST payload, ranked by the producer's "serialized_at" stamp,
  #   with the offset as a tie-break
  # 3. flatten the array of arrays as all groups will have a single item
  #
  # Neither of the two fields that used to be used for this is a freshness signal on its own:
  #
  #   "updated_at" is the record's own timestamp. It does not move when only an associated record
  #   changes, so two payloads with different values can share one updated_at.
  #
  #   the offset is PUBLISH order, not SERIALIZATION order. Publisher#publish does find_by ->
  #   generate_message -> responder.call, so a message produced later can carry an earlier
  #   snapshot. Measured in production: the message a batch ended on was published at
  #   11:36:46.905 carrying a payload read at 11:36:46.398194, while an earlier offset published
  #   at .837 carried a payload read at .671830 with the current state. Ranking on offset
  #   therefore selected the stale payload.
  #
  # "serialized_at" is stamped where the payload is actually built, so it orders payloads by when
  # they were read, which is what "freshest" means here.
  #
  # Messages produced before this field existed do not carry it. They fall back to Time.at(0) and
  # are ordered among themselves by offset - i.e. exactly the previous behaviour - while any
  # stamped message wins over an unstamped one, which is correct: a stamped message can only have
  # come from a newer producer, therefore later.
  def transform_messages_array(params_batch)
    params_batch
      .to_a
      .group_by { |batch| grouping_key_by_event_and_id(batch) }
      .map { |_, group| group.max_by { |message| [serialized_at_for(message), message.offset] } }
      .flatten
  end

  def serialized_at_for(message)
    raw = message.payload.fetch("message").first["serialized_at"]

    raw ? Time.parse(raw.to_s) : FALLBACK_SERIALIZED_AT
  rescue StandardError
    FALLBACK_SERIALIZED_AT
  end

  def grouping_key_by_event_and_id(batch)
    [
      batch.payload.fetch("message").first.fetch("event"),
      batch.payload.fetch("message").first["data"].first.fetch("id", nil)
    ].join
  end

  def duplicates_removal_not_applicable?(params_batch)
    any_message_containing_more_than_one_event?(params_batch) || any_event_containing_more_than_one_item(params_batch)
  end

  def any_message_containing_more_than_one_event?(params_batch)
    params_batch.any? { |batch| batch.payload.to_h.fetch("message", []).size != 1 }
  end

  def any_event_containing_more_than_one_item(params_batch)
    params_batch.any? { |batch| batch.payload.to_h.fetch("message", []).first.to_h.fetch("data", []).size != 1 }
  end
end
