## [Unreleased]

## [1.6.0]
- Judge a payload's embedded children on their own timestamps instead of dropping them with the parent. When `persist_with_dionysus?` rejects a record, `persist` used to `next` past the whole loop body - the child `persist` calls included - so every `has_many` and `has_one` record embedded in that payload was discarded with it, with no error and no counter. The parent is still not written; only the traversal of its children is decoupled from its verdict. Each child re-enters `persist` and is guarded on its own `synced_updated_at || synced_created_at`, so a child the consumer has never seen is created and a child older than the row held locally is still skipped.
- Reproduced at the consumer, not argued: `spec/exp_6591_consumer_loss_reproduction_spec.rb` runs a whole batch through the generated consumer with deduplication in place, from the two production cases reconciled on `v3_bookings` on 2026-08-31. In the first, all seven money-bearing messages carried an `updated_at` 44 ms older than the zero-money payload already accepted, and the payment embedded in them - a row that did not exist locally - was never created. Three control examples pin the fixture down: the same shape at a timestamp the guard accepts persists the money, creates the embedded payment and calls `resolve_to_many_association`, so a red assertion is a statement about this gem rather than about the payload being malformed.
- `resolve_to_many_association` and `resolve_to_one_association` are suppressed for the whole subtree beneath a rejected parent. Cancelling or deleting the children absent from the payload's list is how a genuine deletion propagates, and it requires a complete current list - which is exactly what a payload already judged stale cannot provide. Consuming applications implement that hook destructively, several with `destroy_all`, and the old `next` was shielding them by accident; suppressing it deliberately keeps that behaviour rather than changing it.
- Children beneath a rejected parent are compared strictly, through the new `SynchronizableModel#advances_dionysus_state?`. The ordinary guard accepts a tie, and a purge stamps the cancel column with `update_all` without bumping `synced_updated_at` - so a stale payload that merely ties on a child's timestamp is precisely how a cancelled child would be revived. A tie carries nothing that is not already held, so requiring the child to be demonstrably newer closes that without costing a repair. `persist_with_dionysus?` itself is untouched and behaves as before on the ordinary path.
- Not fixed here, and left as a pending example rather than a silent gap: when deduplication keeps a message whose `updated_at` the guard rejects while a sibling in the same group would have been accepted, the parent's own columns are still lost. Recovering that needs the guard - not a payload timestamp - to choose the survivor. Keeping siblings whose `updated_at` looks newer is disproven by the existing fixture for booking 22578292, where the correct payload carries the *lower* `updated_at`; ranking on any single key has a production counterexample, so `RemoveDuplicatesStrategy` is unchanged. A consuming application that needs the parent half today can pass `params_batch_transformation: nil` for the topic, which is covered by an example here.

## [1.5.0]
- Publish the survivor of a collapsed run of duplicates once more after a delay, behind `config.republish_deduplicated_records` (default `false`) with `config.republish_deduplicated_records_delay` (default 30 seconds). It does nothing unless `remove_consecutive_duplicates_before_publishing` is also on, since there is no run to collapse otherwise.
- What a collapsed run means. `DuplicatesFilter` keeps the last record of each consecutive run for the same resource, event and topic. A run longer than one only happens when the record was written again while an earlier message for it was still queued - which is the same window in which a payload can be serialized across a write and end up carrying an `updated_at` older than its own contents. `publish_consistent_snapshots` bounds its retries and publishes the last attempt regardless, so a payload that may still be torn does get published; this schedules one more publish once the writes have settled.
- Why a republish is sufficient, and why it needs no change on the consumer. The outbox stores a pointer, not a payload: `Outbox::Publisher#publish` re-reads the row with `find_by`, so the scheduled record is serialized fresh at publish time and carries the settled state. The consumer's guard is `event_updated_at >= synced_at`, and `synced_at` is its mirror of the row's `updated_at` as of the last accepted message - a value the row's own `updated_at` can never fall below. A freshly-read republish is therefore accepted by construction, so nothing has to be forced past the guard and no timestamp has to be altered to make it win.
- Scheduled with `retry_at`, not a future `created_at`. `fetch_publishable` already honours `retry_at` exactly, whereas `created_at` is compared against `Time.current + outbox_worker_publishing_delay` - so a future `created_at` publishes early by the length of that look-ahead window, and would report a negative `publishing_latency`. `retry_at` leaves `created_at` honest; `failed_at` and `error_class` stay `nil`, so a scheduled republish is distinguishable from a failed record awaiting retry.
- Termination. A scheduled record that comes due on its own is a run of one, so it schedules nothing further. If the record is still being written when it comes due, it collapses again and schedules once more, which is the intended behaviour - the extra load is bounded at one additional record per key per batch, and pile-ups for the same key collapse through the same filter.
- Observer records are skipped: they carry a changeset and publish through `publish_observers`, a different path.
- Records that failed to publish are skipped too - they are already retried by `handle_error`'s backoff, so scheduling a republish for them would duplicate that.
- Operational note: the scheduled records sit unpublished for the delay, so they raise the average and maximum reported by the outbox latency gauges. That is a truthful measurement - those messages really do wait - but any alert threshold on outbox latency should be checked before switching this on.

## [1.4.1]
- Stamp `serialized_at` when the attempt that is actually published begins, not before the retry loop. 1.4.0 takes the stamp once and then re-serializes up to `max_snapshot_attempts` times, so a message can be published a whole retry cycle after its own stamp - and the payload it publishes is the one read last, not the one the stamp describes. Consumers rank duplicates by `[serialized_at, offset]`, so the freshest payload ended up carrying the oldest stamp and losing its group to a staler sibling. That is the failure `serialized_at` was added to prevent, reintroduced by the guard added to prevent a different one.
- Measured on the affected topic over 65 minutes: 205 of 6,572 messages (3.12%) carried an `updated_at` LATER than their own `serialized_at`, the worst by 1,225 ms. A timestamp cannot describe a moment after the one it was taken at, so that alone proves the stamp preceded the payload.
- `serialize_consistently` now returns `[payload, read_at]`. The looseness between stamp and payload is bounded by a single serialization pass again, as it was before 1.4.0, rather than by the whole retry cycle - so contended records, which retry most, are no longer the ones stamped most wrongly.
- Covered end to end against a real broker in `spec/serialized_at_tracks_published_attempt_spec.rb`: a contended publish retries while a competing publish serializes once and stamps later, both are read back off the topic, and the real `RemoveDuplicatesStrategy` is asked which survives. The spec fails on 1.4.0 on both assertions.

## [1.4.0]

### Fixed

- Re-serialize a record whose row moved while its payload was being built, behind `config.publish_consistent_snapshots` (default `false`), so the payload describes a single moment.
- The defect, reproduced against a real broker in `spec/torn_payload_reproduction_spec.rb`. A serializer reads a record's own columns and then queries its associations. Anything committed in between lands in the payload beside a timestamp taken before it, so the payload's `updated_at` can predate the records embedded next to it. The reproduction publishes from a record read fresh moments earlier and still emits a payload tens of milliseconds adrift carrying an association row the timestamp predates - no stale object, no query cache, no clock skew and no replica involved.
- Why that is a lost record rather than a stale field. Consumers rank duplicates and then compare the payload's `updated_at` against what they have stored. A payload whose timestamp predates its own contents loses that comparison and is skipped by `next`, and the has_many records embedded in it are persisted further down the same loop body, so they are dropped with it: the parent keeps its old values and the child rows are never created, with no error and no counter.
- Measured on the sibling gem's affected topic in production: serialization spans 95 ms at the median and up to 3.5 s, which is ample time for a write to arrive; 141 of 1,476 records receiving more than one message showed `updated_at` moving backwards, and in 69 of those the message that wins deduplication reported a timestamp older than one already published at a lower offset.
- The check reads the row's timestamp back with the query cache bypassed. Publishing runs inside the request when `publish_after_commit` is on, where the cache is live on that connection, and a cached read would report that nothing had moved and hand back the very payload the check exists to catch.
- Retries are bounded by `config.max_snapshot_attempts` (default `Producer::MAX_SNAPSHOT_ATTEMPTS`, 3) and the last attempt is published rather than dropped - a payload that may still be torn beats no message at all. Records without an `updated_at`, and anything that is not an `ActiveRecord::Base`, are serialized once as before.
- The retry is the expensive half, and its cost runs opposite to its benefit. Measured over 5 events on one record with the serializer held at the production median of 95 ms: a quiet record costs nothing beyond one indexed read per message, but a record written every 40 ms pins to the ceiling on every event - 15 serializations instead of 5, and 1.5 s instead of 0.5 s - and still publishes an attempt that may be torn. The hottest records are both the most expensive to guard and the least likely to converge, so `max_snapshot_attempts` is configurable to bound it.
- Every guarded message increments `dionysus.publish.consistent_snapshot` exactly once, tagged `result:consistent|exhausted|unsupported`, `attempts:N`, `topic` and `model`. `exhausted` is the case worth alerting on: the payload is published with no marker and consumers see an ordinary message, so the counter is the only place that outcome is ever visible. Nothing is emitted while the feature is off, so an empty series reads as "not running" rather than "running clean".

## [1.3.0]

### Fixed

- Read the record and its associations without the query cache while publishing, behind `config.publish_with_uncached_reads` (default `false`). The payload is built from the record plus the associations declared with `with:`. When `publish_after_commit` is on, publishing runs inside the request, where Rails' query cache is live on that connection - so a row read earlier in the request is served from the cache while its associations are queried fresh. The payload then carries an `updated_at` older than the data it contains.
- Measured in production on the sibling gem over a two and a half hour window on one topic: of 1,476 records that received more than one message, 141 had a payload whose `updated_at` moved backwards as `serialized_at` moved forwards, and in 69 of those the message that wins deduplication carried an `updated_at` lower than one already published at a lower offset. One record published a payload serialized at `09:27:23.662` reporting `updated_at 09:27:22.139557`, when a payload serialized 1.3 seconds earlier had already reported `09:27:22.345092` - a value a row's `updated_at` cannot return to.
- The consequence is not a stale field but a discarded record. Consumers rank duplicates and then apply `persist_with_dionysus?`, which compares the payload's `updated_at` against what is stored; a payload that loses that comparison is skipped by `next`, and the `has_many` records embedded in it are persisted further down the same loop body, so they are dropped with it. The parent keeps its old values and the new child rows are never created, with no error, no counter and nothing to self-heal it.
- This is why no ordering over the fields already in the message could fix both known incidents: `updated_at` was in the message all along and is simply wrong. `serialized_at`, added in 1.2.0, correctly identifies the last-serialized message - which is precisely the one the guard rejects when its `updated_at` is stale.
- Rolling this out needs no consumer changes: the fix is entirely on the producer, and the payload's shape is unchanged.

## [1.2.0]

### Fixed

- Rank duplicate messages in a consumer batch by when their payload was serialized, not by Kafka offset. The previous release ranked on the highest offset, on the premise that offsets strictly increase in publish order. They do - but publish order is not serialization order: `Publisher#publish` does `find_by` -> `generate_message` -> `responder.call`, so a message produced later can carry an earlier snapshot. Measured in production on one record, eight `updated` events inside 700 ms: the offset the batch ended on was published at `.905` carrying a payload read at `.398194` with `paid_amount 0.0`, while an earlier offset published at `.837` carried a payload read at `.671830` with the real value of `1447.0`. Ranking on offset kept the stale payload and the consumer's value stuck at `0.00` with nothing to self-heal it.
- Neither field already in the message is a freshness signal. `updated_at` is the record's own timestamp and does not move when only an associated record changes, so two payloads with different computed values can share one `updated_at` - that is the tie the previous release was written for. The offset is publish order, per above. The two known incidents are mirror images: in one the correct message has the higher offset and the older `updated_at`, in the other the lower offset and the newer `updated_at`, so no ordering over the existing fields fixes both.
- The producer therefore stamps `serialized_at` where the payload is actually built, and the consumer ranks on `[serialized_at, offset]`. Messages without the field fall back to `Time.at(0)` and are ordered among themselves by offset, which is exactly the previous behaviour; a stamped message wins over an unstamped one, which is correct because a stamp can only come from a newer producer.
- The stamp is gated on `config.include_serialized_at_in_payload`, default `false`, so published envelopes are unchanged until it is switched on. Roll consumers out first - with the fallback that deploy is a no-op - then enable it on the producer.

## [1.1.0]

### Fixed

- Keep the last published message when removing duplicates from a consumer batch. `RemoveDuplicatesStrategy` kept the message with the highest payload `updated_at` per event and id, but payloads are serialized at publish time, so freshness follows publish order rather than `updated_at`: a payload carrying values computed on read from associated records changes without the record's own `updated_at` changing, and can then arrive with an older `updated_at` than a staler message. Those messages were silently discarded, losing the change in every consumer with no error and no trace. The message with the highest Kafka offset is kept instead - a Karafka batch comes from a single topic-partition, so offsets strictly increase in publish order.

## [1.0.0]

Brings `dionysus-rb` back in line with the private gem it was extracted from, which had moved on
by four releases.

### Breaking changes

- Require Ruby >= 3.3 (was >= 2.7).
- Require karafka `~> 2.4` (was `~> 2.0`). Follow https://karafka.io/docs/Upgrades-2.4/ - most
  notably, `Karafka::Messages::Metadata` no longer carries a single `deserializer`, it carries a
  `deserializers` config with separate `payload` and `key` entries, and `key` is now `raw_key`.
  This only affects code that constructs metadata by hand, such as consumer specs.
- The instrumenter is now expected to respond to `increment(name, options)` in addition to
  `instrument`. `Dionysus::Utils::NullInstrumenter` implements it; a custom instrumenter that does
  not will raise when a publish produces no message (see below).

### Added

- Notify when publishing an outbox record produces no Kafka message at all.
  `Dionysus::Producer::Outbox::Publisher#publish` could complete successfully having sent nothing -
  a model declared only as a `with:` dependency has no top-level responder, so when its parent
  could not be resolved the call returned normally - and `RecordsProcessor` then marked the record
  as published, losing the change in every consumer with no error and no retry. Such a record is
  now logged, counted as `dionysus.publish.nothing_published` and reported through the configured
  `error_handler`. It notifies rather than raises: the producer cannot distinguish a legitimately
  deleted parent from one that is not visible yet.
- `Dionysus.initialize_application!` accepts `consumer_group_name`, which is used verbatim as the
  consumer group name and takes precedence over `consumer_group_prefix`. `consumer_group_prefix`
  stays supported and unchanged, so the documented migration path from `bookingsync-prometheus`
  keeps working.

### Fixed

- Compatibility with the `datadog` gem 2.0, which renamed the `span_type:` keyword of
  `Datadog::Tracing#trace` to `type:`. `Dionysus::Producer::Outbox::DatadogTracer` now picks the
  right keyword depending on whether the pre-2.0 `ddtrace` gem is loaded.
- Do not publish the `dionysus.consume_batch` event when the batch is empty.
- `Dionysus::Producer::Outbox::Publisher` no longer trips over a `nil` entry in a parent
  association collection.

## [0.5.0]
- Publish consumed messages batch as `dionysus.consume_batch` event.

## [0.4.0]
- Allow `Dionysus::Producer::Genesis::StreamJob`/`Dionysus::Producer::Genesis::Streamer` to take more options and perform filtering by extra conditions. This is useful if you only need to stream some of the records.

## [0.3.0]
- Allow to provide multiple message filters. `message_filter` stays for backwards compatibility and depends on `message_filters`.

## [0.2.0]
- Fix creation of outbox records when updating a record that was soft-deleted

## [0.1.0]

- Turn private gem `bookingsync-prometheus` into `dionysus-rb` and release it publicly
- Allow to customize `consumer_group_prefix`
- Support Ruby 3.2


Migration path from `bookingsync-prometheus`:
  - Rename all references of `Bookingsync::Prometheus` to `Dionysus`
  - **CRITICAL** - when calling `Dionysus.initialize_application!` (previously `Bookingsync::Prometheus.initialize_application!`) provide a new keyword argument: `consumer_group_prefix` with a name of `prometheus_consumer_group_for` to preserve the same consumer group
  - For producers, `config.hermes_event_producer` no longer defaults to `Hermes::EventProducer`, if you want to preserve the behavior make sure to configure this attribute explicitly: `config.hermes_event_producer = Hermes::EventProducer`
  - `Bookingsync::Prometheus::Producer::Genesis::Performed` event was renamed to `Dionysus::Producer::Genesis::Performed`
  - In the instrumentation context, all references to `prometheus`/`bookingsync_prometheus` were replaced to `dionysus`, which might require some renaming in the apps or observability tools.
  - In the Datadog context (e.g. for `Dionysus::Producer::Outbox::DatadogLatencyReporter`), all references to `bookingsync_prometheus` were replaced to `dionysus`, which might require some renaming in the apps or observability tools, especially if you have a monitor based on this metric.
  - In the Event Bus context, all references to `prometheus` were replaced to `dionysus`, which might require some renaming in the apps.


## Changelog from bookingsync-prometheus gem (original private gem that was renamed to dionysus-rb) - keeping it for historical reasons as the commits are not preserved since it's a new repo

## [1.17.2]
- Get back to using `in_batches` as the combination with `lazy` does the job and looks better han `find_in_batches`

## [1.17.1]
- Address memory leak in `Bookingsync::Prometheus::Producer::Genesis::Streamer::BaseJob` caused by the fact that the way `in_batches` was used was returning all the records in the end which was not needed.

## [1.17.0]
- Change batch size for Genesis to 1000 as it's a wiser default than 100 (and it's the same as ActiveRecord's default for `in_batches`).

## [1.16.1]
- Do not create outbox records for Genesis Replica topics as that would mean the same record would be published twice to Genesis Replica (first time with the standard topic, the second time with the Genesis one)

## [1.16.0]
- Introduce `genesis_replica: true` option for topics which assumes existence of twin topic where the storage would be infinite.
- Due to internal changes in responders to handle `genesis_replica`, it no longer makes sense to have an instrumentation in the responder following the pattern `prometheus.respond.#{topic_name}"`, but rather `"prometheus.respond.#{responder_class_name}"`, which should have been the case from the beginning.

## [1.15.0]
- Allow lambda (or an object responding to `call` method) for `partition_key` that takes a single argument - the resource

## [1.14.0]
- Introduce message filter on consumer level to allow filtering out messages that are not desired for some reason (e.g. the are too big and cause OOMs).

## [1.13.0]
- Provide an option to chain methods when resolving `association` for Observers (via `association_name`).

## [1.12.0]
- Introduce `Bookingsync::Prometheus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy` and `remove_consecutive_duplicates_before_publishing` config option.

## [1.11.0]
- Adjust `Bookingsync::Prometheus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy` to sort messages by timestamps before dropping duplicates to not assume that the messages are ordered.

## [1.10.2]
- Make `bin/karafka_health_check` more lightweight by loading only the check instead of whole prometheus and rails

## [1.10.1]
- Make `resource` to-one relationship optional to not blow up the Rails validations

## [1.10.0]

- Introduce `Bookingsync::Prometheus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy` and use it by default
- Do not set @transactional_outbox_enabled when calling the reader in the scenario where the variable is not set, just return the default

## [1.9.2]

- Make `Bookingsync::Prometheus::Checks::HealthCheck` even less aggressive - increase `expiry_time_in_seconds`.

## [1.9.1]

- Make `Bookingsync::Prometheus::Checks::HealthCheck` less aggressive - increase `expiry_time_in_seconds`.

## [1.9.0]

- Provide a service to assign values to columns from `synced_data`

## [1.8.0]

- Do not allow to execute Genesis for a model that is only a dependency to prevent enqueuing of duplicates.

## [1.7.0]

- Rescue from `publish_outbox_records` (inline handling of outbox publishing) and let the worker retry later.
- Do not send excessive notifications via error handler from `Bookingsync::Prometheus::Producer::Outbox::Publisher`, use logger instead.

## [1.6.0]

- Handle the case of a massive number of observers to be published - instead of publishing them inline (beyond a threshold), execute Genesis for them

## [1.5.0]

- Do not blow up with an exception when outbox publishes _updated event for a deleted record, send a notification instead

## [1.4.1]

- Register a heartbeat for a healthcheck when the messages are consumed

## [1.4.0]

- Move to file-based healthchecks, instead of using Redis-based ones.

## [1.3.0]

- Make health checks for Karafka work with liveness probe as well if statistics emission is enabled (check Readme for more details)

## [1.2.0]

- report `"#{namespace}.bookingsync_prometheus.producer.outbox.latency.highest_since_creation_date` to Datadog

## [1.1.1]

- Do not publish records for _created events if the they don't exist
- Do not cancel the dependent records of the aggregate for _destroyed event, only do this via attributes assignment in such a case

## [1.1.0]

- Allow extra configuration of topics to take advantage of Karafka 2.0 features like Dead Letter Queue
- Fix specs for KarafkaSentryListener and KarafkaDatadogListener for unknown events, as the behavior of Karafka monitor changed

## [1.0.0]

- [BREAKING CHANGES] Require Karafka 2.0 which has a lot of consequences
- `Bookingsync::Prometheus.initialize_application!` no longer takes `batch_fetching` and `backend` arguments
- `sidekiq-backend` is no longer supported
- batch fetching/batch consuming is always enabled. `config.batch_consuming` option is no longer available.
- `seed_brokers` argument provided to `Bookingsync::Prometheus.initialize_application!` should no longer contain the protocol, i.e. use `["localhost:9092"]` instead of `["kafka://localhost:9092"]`
- `ruby-kafka` is no longer used under the hood, it's replaced by `librdkafka`. Which implies a lot of config options are no longer available, e.g `kafka.fetcher_max_queue_size`, `kafka.offset_commit_interval`, `kafka.offset_commit_threshold`.
- when overriding kafka-specific settings in a block provided to `Bookingsync::Prometheus.initialize_application!`, you no longer can use writers to modify the config, instead, you modify the `kafka` hash settings. For example, instead of `config.kafka.heartbeat_interval = 120` you should use `config.kafka[:"heartbeat.interval.ms"] = 120_000`. Notice that the keys are symbols and the unit might be different (seconds vs. milliseconds for example).
- custom partition assignment strategy is no longer supported
- metrics from `ruby-kafka` are no longer available
- Use `kafka://localhost:9092` instead of `kafka://127.0.0.1:9092` in development environment
- There is no more `ruby-kafka` so the Datadog integration it was providing will also not work. Karafka/WaterDrop 2.0 comes with its own implementation: https://karafka.io/docs/Monitoring-and-logging/#datadog-and-statsd-integration, https://github.com/karafka/waterdrop#datadog-and-statsd-integration
- health check needs to be outside the `Rails.application.config.to_prepare` block
- Make sure you use `ddtrace` gem at 1.0 or higher
- Follow the guide: https://karafka.io/docs/Upgrades-2.0/
- If the application is just a producer, use the following config:

``` rb
Karafka::App.setup do |config|
  config.producer = ::WaterDrop::Producer.new do |producer_config|
    producer_config.kafka = {
      'bootstrap.servers': ENV.fetch("PROMETHEUS_SEED_BROKER").gsub("kafka://", "").tr(";", ","), # this needs to be comma-separates list of brokers
      'request.required.acks': 1,
      "client.id": :id_of_the_producer_goes_here.to_s
    }
    producer_config.id = :id_of_the_producer_goes_here.to_s
    producer_config.deliver = true
  end
end
```

- Migration for some popular ruby-kafka options:
  - `config.kafka.max_bytes_per_partition` => `config.kafka[:"max.partition.fetch.bytes"]`
  - `config.kafka.socket_timeout` => `config.kafka[:"socket.timeout.ms"]` (the unit changes to milliseconds, you might need to multiply it by 1000)
  - `config.kafka.session_timeout` => `config.kafka[:"session.timeout.ms"]` (the unit changes to milliseconds, you might need to multiply it by 1000)
  - `config.kafka.heartbeat_interval` => `config.kafka[:"heartbeat.interval.ms"]` (the unit changes to milliseconds, you might need to multiply it by 1000)

## [0.3.7]

- Fix inferring error inside `Bookingsync::Prometheus::Producer::Outbox::Model` if arity is different than -1 or 1


## [0.3.6]

- Ensure publishing outbox observers won't blow up if to-one relationship is nil

## [0.3.5]

- Remove `datadog_statsd` prefix in favour of `namespace`.

## [0.3.4]

- Add latency tracking via Datadog for publishing via outbox

## [0.3.3]

- Enforce order by resource's `created_at` when publishing from after_commit

## [0.3.2]

- Explicitly require `concurrent/array` to avoid `uninitialized constant Concurrent::Array`.

## [0.3.1]

- Fix serialization of deleted records
- Fix issue in the outbox publisher for the scenario where record is a dependency of the parent but no parents exist

## [0.3.0]

- Allow publishing outbox records right after the transaction is committed
- Require `partition_key` column as otherwise, it's not possible resolve the partition key for deleted records

## [0.2.7]

- Add column validation rake task

## [0.2.6]

- Remove attributes validation in `Bookingsync::Prometheus::Producer::Registry` as it was messing up the initializers in Rails apps and required hacks for rake tasks. Replace it with a service to validate the models.

## [0.2.5]

- Handle auto-inclusion of `Bookingsync::Prometheus::Producer::Outbox::ActiveRecordPublishable`

## [0.2.4]

- Fix unlocking behavior (use `lock` with a block to automatically unlock upon completion)

## [0.2.3]

- Fix outbox records creation for models that are just a dependency for parents
- Improve exposed shared behavior to make it possible to pass extra attributes

## [0.2.2]

- Fix shared behavior

## [0.2.1]

- Fix constants redefinition errors

## [0.2.0]

- Add Transactional Outbox and standardize usage of the gem

## [0.1.0]

- Initial release
