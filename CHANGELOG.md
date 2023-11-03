## [Unreleased]

## [0.1.0] - 2023-10-18

- Turn private gem `bookingsync-prometheus` into `dionysus-rb` and release it publicly
- Allow to customize `consumer_group_prefix`


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
