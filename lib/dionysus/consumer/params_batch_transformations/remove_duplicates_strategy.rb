# frozen_string_literal: true

class Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy
  def call(params_batch)
    return params_batch if duplicates_removal_not_applicable?(params_batch)

    Karafka::Messages::Messages.new(transform_messages_array(params_batch), params_batch.metadata)
  end

  private

  # the idea is following:
  # 1. group messages by event and id - for a given model we can expect unique messages for _created and _deleted ecents
  #   but we can have multiple _updated events, so this is where we are interested in removing duplicates
  # 2. sort each group by updated_at, reverse (because the sort is ascending) and take the first one -
  #   this way we will have the most recent update
  # 3. flatten the array of arrays as all groups will have a single item
  # It is safer to apply sorting just for the _updated events to a given model as otherwise we could change the order of
  #   the messages for a different type which might be not desirable
  def transform_messages_array(params_batch)
    params_batch
      .to_a
      .group_by { |batch| grouping_key_by_event_and_id(batch) }
      .map { |_, group| group.max_by { |batch| updated_at_from_batch(batch) } }
      .flatten
  end

  def grouping_key_by_event_and_id(batch)
    [
      batch.payload.fetch("message").first.fetch("event"),
      batch.payload.fetch("message").first["data"].first.fetch("id", nil)
    ].join
  end

  def updated_at_from_batch(batch)
    timestamp = batch.payload.fetch("message", []).first.to_h.fetch("data", []).first.to_h.fetch("updated_at", nil)

    return timestamp.to_datetime if timestamp.respond_to?(:to_datetime)

    Time.current
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
