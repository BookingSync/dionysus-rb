# frozen_string_literal: true

require "time"

class Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy
  SERIALIZED_AT_FORMAT = %r{\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z\z}

  def call(params_batch)
    return params_batch if duplicates_removal_not_applicable?(params_batch)

    Karafka::Messages::Messages.new(transform_messages_array(params_batch), params_batch.metadata)
  end

  private

  # the idea is following:
  # 1. group messages by event and id - for a given model we can expect unique messages for _created and _deleted events
  #   but we can have multiple _updated events, so this is where we are interested in removing duplicates
  # 2. from each group keep the message whose payload was serialized last, with the offset as a tie-break -
  #   the offset alone is publish order, and a message published later can carry an earlier snapshot
  # 3. flatten the array of arrays as all groups will have a single item
  def transform_messages_array(params_batch)
    params_batch
      .to_a
      .group_by { |batch| grouping_key_by_event_and_id(batch) }
      .map { |_, group| freshest(group) }
      .flatten
  end

  # a group is ranked by its stamps only when every message in it carries one - during the
  # producer rollout a batch can hold both, and a stamp says nothing about an unstamped neighbour
  def freshest(group)
    stamps = group.map { |message| serialized_at_for(message) }

    return group.max_by(&:offset) if stamps.any?(&:nil?)

    group.zip(stamps).max_by { |message, serialized_at| [serialized_at, message.offset] }.first
  end

  def serialized_at_for(message)
    raw = message.payload.fetch("message").first["serialized_at"]

    return unless raw.is_a?(String) && raw.match?(SERIALIZED_AT_FORMAT)

    Time.parse(raw)
  rescue
    nil
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
