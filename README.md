# Dionysus::Rb

![Dionysus](assets/logo.svg)

`Dionysus` - a framework on top of [Karafka](http://github.com/karafka/karafka) for Change Data Capture on the domain model level.

In distibuted systems, transferring data between applications is often a challenge. There are multiple ways how of to do this, especially when using Kafka. There is a good chance that you be familiar with [Change Data Capture](https://www.confluent.io/learn/change-data-capture/) pattern, often applied to relational databases such as PostgreSQL, which is a way of extracting row-level changes in real time. In that cases CDC focuses on INSERTs, UPDATEs and DELETEs of rows. If you are familiar with logical replication, this concept ring a bell. When exploring Kafka, you might have you  heard of [Debezium](https://debezium.io), which makes CDC via Kafka simple.

However, there is one problem with this kind of CDC - they are all about row-level changes. This could work for simple cases, but in more complex domains there is a good chance that a database row is not a great reprentation of a domain model. This would be especiallty true if you apply Domain-Driven Design methodology and what you would like to replicate is an Aggregate that could be composed of several rows coming from different tables.

Fortunately, mighty Dionysus himself, powered by wine from [Karafka](https://karafka.io/docs/), has got your back - Dionysus can handle CDC on the domain model level. On the producer side, it will publish `model_created`, `model_updated` and `model_destroyed` events with a snapshot of a given model using custom serializers, also handling dependencies and computed properties (where the value of attribute depends on the value from the other model), with a possiblity of using [transactional outbox pattern](https://karolgalanciak.com/blog/2022/11/12/the-inherent-unreliability-of-after_commit-callback-and-most-service-objects-implementation/) to ensure that everthing gets published. On the consumer side, it will make sure that the snapshots of models are persisted and that you could react to all changes not only via ActiveRecord callbacks but also via event bus. And all of this is achievable merely via a couple of config opions and powerful DSL!


## Installation
Install the gem and add to the application's Gemfile by executing:

    $ bundle add "dionysus-rb"

If bundler is not being used to manage dependencies, install the gem by executing:

    $ gem install "dionysus-rb"

## Usage

Please read [this article first](https://www.smily.com/engineering/integration-patterns-for-distributed-architecture-how-we-use-kafka-in-smily-and-why) to understand the context how this gem was built. Also, it's just recently been made public, so some part of the docs might require clarification. If you find any section like that, don't hesitate to submit an issue.

### TODO - update the article is published.
Also, [read this article], which is an introduction to the gem.


Any application can be both consumer and the producer of Karafka events, so let's take a look how to handle configuration for both scenario.


### Producer

First, you need to define a file `karafka.rb` with a content like this:

``` rb
# frozen_string_literal: true

Dionysus.initialize_application!(
  environment: ENV["RAILS_ENV"],
  seed_brokers: ENV.fetch("DIONYSUS_SEED_BROKER").split(";"),
  client_id: "NAME_OF_THE_APP",
  logger: Rails.logger
)
```

`DIONYSUS_SEED_BROKER` is a string containing all the brokers separated a *semicolon*, e.g. `localhost:9092`. Protocol should not be included.

This is going to handle the initialization process.

If you are migration from the gem prior to making `dionysus-rb` public, most likely you will need to also provide `consumer_group_prefix` for backwards compatibility:

``` rb
Dionysus.initialize_application!(
  environment: ENV["RAILS_ENV"],
  seed_brokers: ENV.fetch("DIONYSUS_SEED_BROKER").split(";"),
  client_id: "NAME_OF_THE_APP",
  logger: Rails.logger,
  consumer_group_prefix: "prometheus_consumer_group_for"
)
```

By default, the name of the consumer grpup will be  "NAME_OF_THE_APP_dionysus_consumer_group_for_NAME_OF_THE_APP" where `dionysus_consumer_group_for` is a `consumer_group_prefix`.


And define `dionysus.rb` initializer with your Kafka topics:

``` rb config/initializers/dionysus.rb
Rails.application.config.to_prepare do
  Karafka::App.setup do |config|
    config.producer = ::WaterDrop::Producer.new do |producer_config|
      producer_config.kafka = {
        'bootstrap.servers': 'localhost:9092', # this needs to be comma-separates list of brokers
        'request.required.acks': 1,
        "client.id": "id_of_the_producer_goes_here"
      }
      producer_config.id = "id_of_the_producer_goes_here"
      producer_config.deliver = true
    end
  end

  Dionysus::Producer.declare do
    namespace :v3 do # the name of the namespace is supposed to group topics that use the same serializer, think of it as an API versioning. The name of the namespace is going to be included in the topics' names, e.g. `v3_accounts`
      serializer YourCustomSerializerClass

      topic :accounts, genesis_replica: true, partition_key: :id do # Refer to Genesis section for more details regarding this options, by default it's false
        publish Account
      end

      topic :rentals, partition_key: :account_id do # partition key as a name of the attribute
        publish Availability
        publish Bathroom
        publish Bedroom
        publish Rental
      end

      bookings_topic_partition_key_resolver = ->(resource) do # a partition key can also be a lambda
        resource.id.to_s if resource.class.name == "Booking"
        resource.rental_id.to_s if resource.respond_to?(:rental_id)
      end

      topic :bookings, partition_key: bookings_topic_partition_key_resolver do
        publish Booking, with: [BookingsFee, BookingsTax]
      end

      topic :los_records, partition_key: :rental_id do
        publish LosRecord
      end
    end
  end
end
```


There are a couple of important things to understand here.
- A namespace might be used for versioning so that you can have e.g., `v3` and `v4` format working at the same time and consumers consumings from different ones as they need. Namespace is a part of the topic name, in the example above the following topics are declared: `v3_accounts`, `v3_rentals`, `v3_bookings`, `v3_los_records`. Most likely you will need to create them manually in the production environement, depending the Kafka cluster configuration.
- `topic` is a declaration of Kafka `topics`. To understand more about topics and what would be some rule of thumbs when designing them, please [read this article](https://www.smily.com/engineering/integration-patterns-for-distributed-architecture-intro-to-kafka).
- Some entities might have attributes depending on other entities (computed properties) or might need to be always published together (kind of like Domain-Driven Design Aggregate). For these cases, use `with` directive which is an equivalent of sideloading from REST APIs.  E.g., Booking could have `final_price` attribute that depends on other models, like BookingsFee or BookingsTax, which contribute to that price. Publishing these items separately, e.g. first Bookings Fee first and then Booking with the changed final price might lead to inconsistency on the consumer side where `final_price` value doesn't match the value that would be obtained by summing all elements of the price. That's why all these records need to be published together. That's what `with` option is supposed to cover: `publish Booking, with: [BookingsFee, BookingsTax]`. Thanks to that declaration, any update to Booking or change its dependencies (BookingsFee, BookingsTax) such as creation/update/deletion will result in publishing `booking_updated` event.


#### Serializer

Serializer  is a class that needs implements `serialize` method with the following signature:

``` rb
class YourCustomSerializerClass
  def self.serialize(record_or_records, dependencies:)
    # do stuff here
  end
end
```

`record_or_records` is either a single record or the array of records and dependencies are what is defined via `with` option, in most cases this is going to be an empty array, in cases like Bookings in the example above it is going to be an array of dependencies to be sideloaded. The job of the serializer is to figure out how to find the right serializer (kind of like a factory) for a given model and how to sideload the dependencies and return the array of serialized payloads (could be one-element array when passing single record, but it needs to be an array).

The best way to implement serialization part would be to create `YourCustomSerializerClass` class inheriting from `Dionysus::Producer::Serializer`. Then, you would need to implement just a single method: `infer_serializer`:

``` rb
class YourCustomSerializerClass < Dionysus::Producer::Serializer
  def infer_serializer
    somehow_figure_out_the_right_serializer_for_the_model_klass(model_klass)
  end
end
```

The `record` method will be available inside the class so that's how you can get a serializer for a specific model. And to implement the actual serializer for the model, you can create classes inherting from `ModelSerializer`:

``` rb
class SomeModelSerializer < Dionysus::Producer::ModelSerializer
  attributes :name, :some_other_attribute

  has_one :account
  has_many :related_records
end
```

The declared attributes/relationships will be delegated to the given record by default, although you can override these methods.

To resolve serializers for declared relationships, also `YourCustomSerializerClass` will be used.

When testing serializers, you can just limit the scope of the test to `as_json` method:

``` rb
SomeModelSerializer.new(record_to_serialize, include: array_of_underscored_dependencies_to_be_sideloaded, context_serializer: YourCustomSerializerClass).as_json
```

You can also try testing using `YourCustomSerializerClass`, so that you could also verity that `infer_serializer` method works as expected:

``` rb
YourCustomSerializerClass.serialize(record_or_records, dependencies: dependencies)
```


### Config options

#### Bypassing serializers


For a large volume of data, sometimes it doesn't make sense to use serializers foe certain use cases to serialize records individually. One example would be deleting records for models that are soft-deletable. If the only thing you expect your consumers to do is to delete records by ID for a given model and the amount of data is huge, you will be better off sending a single event with a lot of IDs instead of sending multiple events for every record individually while perofrming the full serialization. This is usually combined with `import` option on consumer side to make the cosuming even more efficient.


Here is a complere example of a use case where serialization is bypassed using `serialize: false` option with some extras that will be useful when thinking about `import` option on the consumer side. Let's consider hypotehtical `Record` model:


``` rb

Dionysus::Producer.responders_for(Record).each do |responder|
  partition_key = account.id.to_s
  key = "RecordsCollection:#{account_id}"
  created_records = Record.for_accounts(account).visible
  canceled_records = Record.for_accounts(account).soft_deleted

  message = [].tap do |current_message|
    current_message << ["record_created", created_records.to_a, {}]
    if canceled_records.any?
      current_message << ["record_destroyed", canceled_records.map { |record| RecordPrometheusDTO.new(record) }, { serialize: false }]
    end
  end

  result = responder.call(message, partition_key: partition_key, key: key)
end

class RecordDionysusDTO < SimpleDelegator
  def as_json
    {
      id: id
    }
  end
end
```

That way, the serializer will not load any relationships, etc., it will just serialize IDs for `records_destroyed` event and as a bonus part, it covers the case where it might be useful to not deal with records one by one but with a huge batch at once and then using something like [activerecord-import](https://github.com/zdennis/activerecord-import) on the consumer side.

#### Responders

Prior to Karafka 2.0, there used to be a concept of Responders that were responsible for publishing messages. This concept was dropped in Karafka 2.0, but a similar concept is still used in Dionysus as its predecessor was built on top of Karafka 1.x.

Responders implement `call` method that take `message` as a positional argument and `partition_key` and message `key`. Most likely you are not going to need to use this knowledge, but in case you do something really, here is an API to get responders:

- `Dionysus::Producer.responders_for(model_klass)` - get all responders for a given model class, regardless of the topic
- `Dionysus::Producer.responders_for_model_for_topic(model_klass, topic)` - get all responders for a given model class, for a given topic
- `Dionysus::Producer.responders_for_dependency_parent(model_klass)` - get all parent-responders for a given model class that is a dependency (when using `with` direcive), regardless of a topic
- `Dionysus::Producer.responders_for_dependency_parent(model_klass topic)` - get all parent-responders for a given model class that is a dependency (when using `with` direcive), for a given topic


#### Instrumentation & Event Bus


Instrumenter - an object for instrumentation expecting the following interface (this is the default class):

``` rb
class Dionysus::Utils::NullInstrumenter
  def self.instrument(name, payload = {})
    yield
  end
end
```


Event Bus is useful if you want to react to some events, with the following interface ((this is the default class)):

``` rb
class Dionysus::Utils::NullEventBus
  def self.publish(name, payload)
  end
end
```

For the instrumentation, the entire publishing logic is wrapped with the following block: `instrumenter.instrument("Dionysus.respond.#{responder_class_name}")`.

For the event_bus, the event it published after getting a success response from Kafka: `event_bus.publish("Dionysus.respond", topic_name: topic_name, message: message, options: final_options)`

You can configure those dependencies in the initializer:

``` rb
Dionysus::Producer.configure do |config|
  config.instrumenter = MyInstrumentation
  config.event_bus = MyEventBusForDionysus
end
```

They are not required though; null-object-pattern-based objects are injected by default.


#### Sentry and Datadog integration

This is applicable to both consumers and producers. For Sentry and Datadog integration, add these 2 lines to your initializer:

``` rb
Karafka.monitor.subscribe(Dionysus::Utils::KarafkaSentryListener)
Karafka.monitor.subscribe(Dionysus::Utils::KarafkaDatadogListener)
```

Don't put these inside `Rails.application.config.to_prepare do` block.

#### Transactional Outbox Pattern

The typical problem on the producer's side you will experience is a possibility of losing some messages due to the lack of transactional boundaries as things like publishing events happens usually in `after_commit` event.

To prevent that, you could take advantage of [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) which is implemented in this gem.

The idea is simple - store the messages in a temporary table (in the same transaction where creating/updating/deleting publishable record happens) and then publish them in a separate process and mark these messages as published.

Dionysus has also an extra optimization allowing publish from both after commit callbacks (for performance reasons) and also after a certain delay, to publish from a separate worker that reads data from the transactional outbox table, which covers the cases where some records were not published - in that case they will not be lost, but just retried later.

##### Making models publishable

To make ActiveRecord models publishable, you need to make sure that 'Dionysus::Producer::Outbox::ActiveRecordPublishable' module is included in the model. This should be handled automatically by the gem when a model is declared inside `Dionysus::Producer.declare`.

Thanks to that, an outbox record will be created after each create/update/destroy event.

In some cases, you might want to publish update events, even after the record is soft-deleted. To do that, you need to override `dionysus_publish_updates_after_soft_delete?` method:

```rb
def dionysus_publish_updates_after_soft_delete?
  true
end
```


##### Outbox configuration

``` rb
Dionysus::Producer.configure do |config|
  config.database_connection_provider = ActiveRecord::Base # required
  config.transaction_provider = ActiveRecord::Base # required
  config.outbox_model = DionysusOutbox # required
  config.outbox_publishing_batch_size = 100 # not required, defaults to 100
  config.lock_client = Redlock::Client.new([ENV["REDIS_URL"]]) # required if you want to use more than a single worker/more than a single thread per worker, defaults to Dionysus::Producer::Outbox::NullLockClient. Check its interface and the interface of `redlock` gem. To cut the long story short, when the lock is acquired, a hash with the structure outlined in Dionysus::Producer::Outbox::NullLockClient should be yielded. If the lock is not acquired, a nil should be yielded.
  config.lock_expiry_time  = 10_000 # not required, defaults to 10_000, in milliseconds
  config.error_handler = Sentry # not required but highly recommended, defaults Dionysus::Utils::NullErrorHandler. When using Sentry, you will probably want to exclude SignalException `config.excluded_exceptions += ["SignalException"]`.
  config.soft_delete_column = :deleted_at # defaults to "canceled_at" when not provided
  config.default_partition_key = :some_id # defaults to  :account_id when not provided, you can override it per topic when declaring them with `partition_key` config option. You can pass either a symbol or a lambda taking the resource as the argument.
  config.outbox_worker_sleep_seconds = 1 # defaults to 0.2 second when not provided, it's the time interval between each iteration of the outbox worker which fetches pubishable records, publishes them to Kafka and marks them as finished
  config.transactional_outbox_enabled = false # not required, defaults to `true`. Set it to `false` only if you want to disable creating outbox records (which might be useful for the migration period). If you are not sure if you need this config setting or not, then probably you don't
  config.publish_after_commit = true # not required, defaults to `false`. Check `Publishing records right after the transaction is committed` section for more details.
  config.outbox_worker_publishing_delay = 5 # non required, defaults to 0 a delay in seconds until the outbox record is considered publishable. Check `Publishing records right after the transaction is committed` section for more details.
  config.remove_consecutive_duplicates_before_publishing = true # not required, defaults to false. If set to true, the consecutive duplicates in the publishable batch will be removed and only one message will be published to a given topic. For example, if for whatever reason there are ten messages in a row for a given topic to publish `user_updated` ecent, only the last will be published. Check `Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy` for exact implementation. To verify if this feature is useful, it's recommended to browse Karafka UI and check messages in the topics if there are any obvious duplicates happening often.
  config.observers_inline_maximum_size = 100 # not required, defaults to 1000. This config setting matters in case there is a huge amount of dependent records (observers). If the threshold is exceeded, the observers will be published via Genesis process to not cause issues like blocking the outbox worker.
  config.sidekiq_queue = :default # required, defaults to :dionysus. The queue will be used for a genesis process
end
```

##### DionysusOutbox model

Generate a model for the outbox:

```
rails generate model DionysusOutbox
```

and use the following migration code:

``` rb
create_table(:dionysus_outboxes) do |t|
  t.string "resource_class", null: false
  t.string "resource_id", null: false
  t.string "event_name", null: false
  t.string "topic", null: false
  t.string "partition_key"
  t.datetime "published_at"
  t.datetime "failed_at"
  t.datetime "retry_at"
  t.string "error_class"
  t.string "error_message"
  t.integer "attempts", null: false, default: 0
  t.datetime "created_at", precision: 6, null: false
  t.datetime "updated_at", precision: 6, null: false

  # some of these indexes are not needed, but they are here for convenience when checking stuff in console or when using a tartarus for archiving
  t.index ["topic", "created_at"], name: "index_dionysus_outboxes_publishing_idx", where: "published_at IS NULL"
  t.index ["resource_class", "event_name"], name: "index_dionysus_outboxes_on_resource_class_and_event"
  t.index ["resource_class", "resource_id"], name: "index_dionysus_outboxes_on_resource_class_and_resource_id"
  t.index ["topic"], name: "index_dionysus_outboxes_on_topic"
  t.index ["created_at"], name: "index_dionysus_outboxes_on_created_at"
  t.index ["resource_class", "created_at"], name: "index_dionysus_outboxes_on_resource_class_and_created_at"
  t.index ["resource_class", "published_at"], name: "index_dionysus_outboxes_on_resource_class_and_published_at"
  t.index ["published_at"], name: "index_dionysus_outboxes_on_published_at"
end
```

You also need to include `Dionysus::Producer::Outbox::Model` module in your model:

``` rb
class DionysusOutbox < ApplicationRecord
  include Dionysus::Producer::Outbox::Model
end
```

For testing publishable models, you can take advantage of the `Dionysus Transactional Outbox Publishable"` shared behavior. First, you need to require the following file:

``` rb
require "dionysus/support/rspec/outbox_publishable"
```

And then just add `it_behaves_like "Dionysis Transactional Outbox Publishable"` in models' specs.


##### Running outbox worker

Use the following Rake task:

```
DIONYSUS_THREADS_NUMBER=5 DB_POOL=10 bundle exec rake dionysus:producer
```

If you want to use just a single thread:

```
bundle exec rake dionysus:producer
```

##### Publishing records right after the transaction is committed

When the throughput of outbox records' creation is really high, there is a very good chance that it might take even few minutes to publish some records from the workers (due to the limited capacity).

In such case you might consider publishing records right after the transaction is committed. To do so, you need to:

1. Enable publishing globally:

``` rb
Dionysus::Producer.configure do |config|
  config.publish_after_commit = true
end
```

2. Or enable/disable it per model where `Dionysus::Producer::Outbox::ActiveRecordPublishable` is included

``` rb
class MyModel < ApplicationRecord
  include Dionysus::Producer::Outbox::ActiveRecordPublishable

  private

  def publish_after_commit?
    true
  end
end
```

To avoid publishing something or running into some conflicts with publishing records after the transaction (from `after_commit` callback) and from the outbox worker, it is recommended to add some delay for the outbox records until they will be considered publishable.

``` rb
Dionysus::Producer.configure do |config|
  config.outbox_worker_publishing_delay = 5 # in seconds, defaults to 0
end
```

By default, the records will be considered publishable right away. With that config option, it will take 5 seconds after creation until they are considered publishable.

##### Outbox Publishing Latency Tracking

It's highly recommended to tracking latency of publishing outbox records defined as the difference between the `published_at` and `created_at` timestamps.

``` rb
Dionysus::Producer.configure do |config|
  config.datadog_statsd_client = Datadog::Statsd.new("localhost", 8125, namespace: "application_name.production") # required for latency tracking, defaults to `nil`
  config.high_priority_sidekiq_queue = :critical # not required, defaults to `:dionysus_high_priority`
end
```

You also need to add a job to the sidekiq-cron schedule that will run every 1 minute:

``` rb
Sidekiq.configure_server do |config|
  config.on(:startup) do
    Dionysus::Producer::Outbox::DatadogLatencyReporterScheduler.new.add_to_schedule
  end
end
```

With this setup, you will have the following metrics available on DataDog:

- `"#{namespace}.dionysus.producer.outbox.latency.minimum"`
- `"#{namespace}.dionysus.producer.outbox.latency.maximum"`
- `"#{namespace}.dionysus.producer.outbox.latency.average"`
- `"#{namespace}.dionysus.producer.outbox.latency.highest_since_creation_date`

##### Archiving old outbox records

You will probably want to periodically archive/delete published outbox records. It's recommended to use [tartarus-rb](https://github.com/BookingSync/tartarus-rb) for that.

Here is an example config:

```
tartarus.register do |item|
  item.model = DionysusOutbox
  item.cron = "5 4 * * *"
  item.queue = "default"
  item.archive_items_older_than = -> { 3.days.ago }
  item.timestamp_field = :published_at
  item.archive_with = :delete_all_using_limit_in_batches
end
```


##### Events, hooks and monitors

You can subscribe to certain events that are published by `Dionysus.monitor`. The monitor is based on [`dry-monitor`](https://github.com/dry-rb/dry-monitor).

Available events and arguments are:

- "outbox_producer.started", no arguments
- "outbox_producer.stopped", no arguments
- "outbox_producer.shutting_down", no arguments
- "outbox_producer.error", arguments: error, error_message
- "outbox_producer.publishing_failed", arguments: outbox_record
- "outbox_producer.published", arguments: outbox_record
- "outbox_producer.processing_topic", arguments: topic
- "outbox_producer.processed_topic", arguments: topic
- "outbox_producer.lock_exists_for_topic", arguments: topic


##### Outbox Worker Health Check

You need to explicitly enable the health check (e.g. in the initializer, but it needs to be outside the `Rails.application.config.to_prepare` block):

``` rb
Dionysus.enable_outbox_worker_healthcheck
```

To perform the actual health check, use `bin/outbox_worker_health_check`. On success, the script exits with `0` status and on failure, it logs the error and exits with `1` status.

```
bundle exec outbox_worker_health_check
```

It works for both readiness and liveness checks.

#### Tombstoning records

The only way to get rid of messages under a given key from Kafka is to tombstone them. Use `Dionysus::Producer::Outbox::TombstonePublisher` to do it:

``` rb
Dionysus::Producer::Outbox::TombstonePublisher.new.publish(resource, responder)
```

Or if you want a custom `key`/`partition_key`:

``` rb
Dionysus::Producer::Outbox::TombstonePublisher.new.publish(resource, responder, partition_key: partition_key, key: key)
```


#### Genesis

When you add `dionysus-rb` to the existing application, there is a good chance that you will need to stream all of the existing records of the publishable models. Or maybe you changed the schema of the serializer introducing some new attributes and you want to re-stream the records. In either case you need to publish everything from scratch. Or in other words, perform a Genesis.

The way to handle this is to use `Dionysus::Producer::Genesis#stream` method, which is going to enqueue some Sidekiq jobs.

This method takes the following keyword arguments:
- `topic` - required, the name of the topic where you want to publish a given model (this is necessary as one model might be published to multiple topics)
- `model` - required, the model class you want to publish
- `from` - non-required, to be used together with `to`, it's establish the timeline defined by `from` and `to` timestamps to scope the records to the ones that were updated only during this time. Defaults to `nil`. Don't provide any value if you want to publish all records.
- `to` - non-required, to be used together with `from`, it's establish the timeline defined by `from` and `to` timestamps to scope the records to the ones that were updated only during this time. Defaults to `nil`. Don't provide any value if you want to publish all records.
- `number_of_days` - required, this arguments defined the timeline for executing all the jobs. If you set it to 7, it means the jobs to publish records to Kafka will be evenly distributed over 7 days. You can use fractions here as well, e.g. 0.5 for half of the day (12 hours).
- `streamer_job` non-required, defaults to `Dionysus::Producer::Genesis::Streamer::StandardJob`. In majority of the cases, you don't want to change this argument, but sometimes it might happen that you want to use a different strategy for streaming the records. For example, you might use model X that has a relationship to model Y and usually single record X contains thousands of models Y. In such case, you might intercept model X and provide some custom publishing logic for model Y. Check `Dionysus::Producer::Genesis::Streamer::BaseJob` if you want to apply some customization.

If you need the full mapping of all available topics and models, use `Dionysus::Producer.topics_models_mapping`.

When executing Genesis, `Dionysus::Producer::Genesis::Performed` event is going to be published via [Hermes](http://github.com/BookingSync/hermes-rb) if the gem is included. If you don't want for whatever reason to use Hermes, you can use `Dionysus::Utils::NullHermesEventProducer` (which is the default), the config options are described below.

Config options dedicated for Genesis feature:

``` rb
Dionysus::Producer.configure do |config|
  config.sidekiq_queue = :messaging # non-required, defaults to `:dionysus`. Remember that you need to add this queue to the Sidekiq config file.
  config.publisher_service_name = "my_service" # non-required, defaults to `WaterDrop.config.client_id`
  config.genesis_consistency_safety_delay = 120.seconds # non-required, defaults to `60.seconds`, this is an extra delay taking into consideration the time it might take to schedule the jobs so that you can have an accurate timeline for the Genesis window which is needed for `Dionysus::Producer::Genesis::Performed` event
  config.hermes_event_producer = Dionysus::Utils::NullHermesEventProducer # non-required
end
```

However, such a setup might not be ideal. If you have just a single topic where you publish both current events actually happening in the application at the given moment and also want to re-stream all the records, there is a good chance that you will end up with a huge lag on consumers' side at some point.

The recommended approach is to have two separate topics:

1. a standard one, used for publishing current events - e.g. "v3_rentals". This topic should have also a limited retention configured, e.g. to be 7 days.
2. a genesis-one, used for publishing everything - e.g. "v3_rentals_genesis". You might consider having an infinite retention in this topic.

Thanks to such a separation, there will not be an extra lag on the consumers' side causing delays with processing potentially critical events.

To achieve this result, add `genesis_replica: true` option when declaring a topic on the Producer's decide:

``` rb
Dionysus::Producer.declare do
  namespace :v3 do
    serializer YourCustomSerializerClass

    topic :accounts, genesis_replica: true do
      publish Account
    end
  end
end
```

When a topic is declared as such, there are 2 possible scenarios of publishing events
1. When calling `Dionysus::Producer::Genesis#stream` with the *primary* topic as an argument (based on the example above: `v3_accounts`), the events will be published to both `v3_accounts` and `v3_accounts_genesis` topics
2. When calling `Dionysus::Producer::Genesis#stream` with the *genesis* topic as an argument (based on the example above: `v3_accounts_genesis`), the events will be published to only `v3_accounts_genesis` topic

That implies that the event is always published to the genesis-topic. Only the primary one can be skipped. **IMPORTANT** This behavior is exactly the same during "standard" publishing, outside Genesis - the event will be published to both standard and genesis topic if the topic is declared as a genesis one.

The reasons behind this behavior is that the Genesis topic cannot have stale data, especially that it's expected to have an infinite retention.

It's a highly opinionated choice design, if you don't want to maintain a separate topic because you don't need an infinite storage, you can either set a super-short retention for the Genesis replica topic, or enable/disable the feature conditionally, e.g. via ENV variable:

``` rb
use_genesis_replica_for_accounts_topics = (ENV.fetch("USE_GENESIS_REPLICA_FOR_ACCOUNTS_TOPIC", false).to_s == "true")

topic :accounts, genesis_replica: use_genesis_replica_for_accounts_topics do
  publish Account
end
```

Alternatively, feel free to submit a PR with a cleaner solution.

**Notice for consumers**: if you decide to introduce such a separation, it would be recommended to use dedicated consumers just for the genesis-topic.

### Observers for dependencies for computed properties

Imagine the case where you have for example a Rental model, with some config attribute, for example `check_in_time`. Such an attribute might not necessarily be something that is directly readable from `rentals` table as a simple column. The logic might work in the way that the value from `rentals` table is returned if it's present or delegated to a related `Account` as a fallback. That means that the `Rental` has a dependency on `Account` and you probably want to observe `Account` and publish related rentals if some `default_check_in_time` attribute changes.

To handle it, you need to do 2 things:

1. Add `changeset` column to the outbox model. If you don't need an encryption, just use `jsonb` type for the column. If you need encryption, use `text` type.
2. Add a proper topic declaration. For the example described above, it could look like this:
``` rb
topic :rentals do
   publish Rental, observe: [
     {
       model: Account,
       attributes: %i[default_check_in_time],
       association_name: :rentals
     }
   ]
end
```

It's going to work for both to-one and to-many relationships.

To make sure you the columns specified `attribues` actually exist, you can use the following service to validate them:

``` rb
Dionysus::Producer::Registry::Validator.new.validate_columns
```

You can put it in a separate spec to keep things simple or just use the following rake task:

```
bundle exec rake dionysus:validate_columns
```

You could also pass string of chained methods as `association_name`, for example: `association_name: "other.association.rentals"`. Also, when using strings, the validation will be skipped whether a given association exists or not (which is performed for symbols).

#### Encryption of changesets

If you store some sensitive data (e.g. anything in scope of GDPR), it will be a good idea to encrypt `changeset`. The recommend solution would be to use [crypt_keeper](https://github.com/jmazzi/crypt_keeper) gem. To make outbox records work with encrypted changesets, call `encrypts_changeset!` class method after declaring the encryption:

``` rb
class DionysusOutboxEncrChangeset < ApplicationRecord
  include Dionysus::Producer::Outbox::Model

  crypt_keeper :changeset, encryptor: :postgres_pgp, key: ENV.fetch("CRYPT_KEEPER_KEY"), encoding: "UTF-8"

  encrypts_changeset!
end
```

### Consumer

First, you need to define a file `karafka.rb` with a content like this:

``` rb karafka.rb
# frozen_string_literal: true

Dionysus.initialize_application!(
  environment: ENV["RAILS_ENV"],
  seed_brokers: [ENV.fetch("DIONYSUS_SEED_BROKER")],
  client_id: NAME_OF_THE_APP,
  logger: Rails.logger
)
```

`DIONYSUS_SEED_BROKER` is a string containing all the brokers separated a semicolon, e.g. `localhost:9092`. Protocol should not be included.


If you are migration from the gem prior to making `dionysus-rb` public, most likely you will need to also provide `consumer_group_prefix` for backwards compatibility:

``` rb
Dionysus.initialize_application!(
  environment: ENV["RAILS_ENV"],
  seed_brokers: ENV.fetch("DIONYSUS_SEED_BROKER").split(";"),
  client_id: ["NAME_OF_THE_APP"][],
  logger: Rails.logger,
  consumer_group_prefix: "prometheus_consumer_group_for"
)
```

By default, the name of the consumer grpup will be  "NAME_OF_THE_APP_dionysus_consumer_group_for_NAME_OF_THE_APP" where `dionysus_consumer_group_for` is a `consumer_group_prefix`.


And define `dionysus.rb` initializer:

``` rb config/initializers/dionysus.rb
Rails.application.config.to_prepare do
  Dionysus::Consumer.declare do
    namespace :v3 do
      topic :rentals do
        dead_letter_queue(topic: "dead_messages", max_retries: 2)
      end
    end

    Dionysus::Consumer.configure do |config|
      config.transaction_provider = ActiveRecord::Base # not required, but highly recommended
      config.model_factory = DionysusModelFactory # required
    end
  end

  Dionysus.initialize_application!(
    environment: ENV["RAILS_ENV"],
    seed_brokers: [ENV.fetch("DIONYSUS_SEED_BROKER")],
    client_id: NAME_OF_THE_APP,
    logger: Rails.logger
  )
end
```

Notice that you can provide a block to the `topic` method, which allows you to provide some extra configuration options (the same ones as in Karafka, e.g. a Dead Letter Queue config).

The structure of namespace/topics must reflect what is configured by a Producer! You just don't need to declare specific models, that happens automatically/can be configured with `model_factory` where you could e.g., return nil for the models that you don't want to be processed.

`model_factory` is an object that returns a model class (or a proper factory! It does not need to be a model class, but returning ActiveRecord model class will work and will be the simplest way to deal with it. Check specs for more details if you want to decouple it from using model classes directly) for a given name, e.g.:

``` rb
class DionysusModelFactory
  def self.for_model(model_name)
    model_name.classify.gsub("::", "").constantize rescue nil
  end
end
```

Start `karafka server`:

```
bundle exec karafka server
```

That will be enough to process `_created`, `_updated`, and `_destroyed` events in a generic way.

So far, Dionysus expects format to be compliant with [BookingSync API v3](https://developers.bookingsync.com/reference/). It also performs some special mapping (the notation is attribute from payload to local attribute):
- id -> synced_id
- created_at -> synced_created_at
- updated_at -> synced_updated_at
- canceled_at -> synced_canceled_at
- relationship_id -> synced_relationship_id
- relationship_type -> synced_relationship_type (for polymorphic associations)

Also, Dionysus checks timestamps (`updated_at` or `created_at` from payload with local `synced_updated_at` or `synced_created_at` values). If the remote timestamp is from the past comparing to local timestamps, the persistence will not be executed. `synced_updated_at`/`synced_created_at` are configurable (check config options reference)

#### Consumer Base Class

If you are happy with `Karafka::BaseConsumer` being a base class for all your consumers, you don't need to do anything as this is a default. If you want to customize it, you have two options:

1. Global config - specify a base class in Consumer Config in an initializer via `consumer_base_class` attribute:


``` rb
Dionysus::Consumer::Config.configure do |config|
  config.consumer_base_class = CustomConsumerClassInhertingFromKarafkaBaseConsumer
end
```

2. Specify per topic - which also takes precedence over a global config (so you can use both of these options!) via `consumer_base_class` option:

``` rb
topic :rentals, consumer_base_class: CustomConsumerClassInhertingFromKarafkaBaseConsumer
```

Here is an example:

``` rb
class CustomConsumerClassInhertingFromKarafkaBaseConsumer < Karafka::BaseConsumer
  alias_method :original_on_consume, :on_consume

  def on_consume
    Retryable.perform(times: 3, errors: errors_to_retry, before_retry: BeforeRetry) do
      original_on_consume
    end
  end

  private

  def errors_to_retry
    @errors_to_retry ||= [ActiveRecord::StatementInvalid, PG::ConnectionBad, PG::Error]
  end

  class Retryable
    def self.perform(times:, errors:, before_retry: ->(_error) {})
      executed = 0
      begin
        executed += 1
        yield
      rescue *errors => e
        if executed < times
          before_retry.call(e)
          retry
        else
          raise e
        end
      end
    end
  end

  class BeforeRetry
    def self.call(_error)
      ActiveRecord::Base.clear_active_connections!
    end
  end
end
```


#### Retryable consuming

When consuming the events, it might happen that some errors will occur (similar case mentioned already for consumer base class section). If you want to retry from the errors in some way, you can inject a custom `retry_provider` which is supposed to be an object implementing `retry` method that should yield a block. You can specify it on a config level:

``` rb
Dionysus::Consumer::Config.configure do |config|
  config.retry_provider = CustomRetryProvider.new
end
```

Here is an example:


``` rb
class CustomRetryProvider
  def retry(&block)
    Retryable.perform(times: 3, errors: errors_to_retry, before_retry: BeforeRetry, &block)
  end

  private

  def errors_to_retry
    @errors_to_retry ||= [ActiveRecord::StatementInvalid, PG::ConnectionBad, PG::Error]
  end

  class Retryable
    def self.perform(times:, errors:, before_retry: ->(_error) {})
      executed = 0
      begin
        executed += 1
        yield
      rescue *errors => e
        if executed < times
          before_retry.call(e)
          retry
        else
          raise e
        end
      end
    end
  end

  class BeforeRetry
    def self.call(_error)
      ActiveRecord::Base.clear_active_connections!
    end
  end
end
```

#### Association/disassociation of relationships

For relationships, especially the sideloaded ones, Dionysus doesn't know if something is has_many or has_many through relationship type, so it doesn't automatically perform linking between records. If Booking is serialized with BookingsFee, it will create/update Booking and BookingsFee as if there were some separate events, but will not magically link them. Most likely, in this scenario, BookingsFee will be linked to Booking anyway via foreign keys via synced attributes (BookingsFee will have synced_booking_id), bu `has_many :through relationship` it is not going to happen. Dionysus doesn't try to guess and allows to define the way associations should be linked to the consumer. The models need to implement the following methods:

``` rb
def resolve_to_one_association(name, id_from_remote_payload)
end

def resolve_to_many_association(name, ids_from_remote_payload)
end
```

If you don't have has_one through relationships, you can leave `resolve_to_one_association` empty. If you don't have has_many through relationships, you can implement `resolve_to_many_association` in the following way:

``` rb
def resolve_to_many_association(name, ids_from_remote_payload)
  public_send(name).where.not(id: ids_from_remote_payload).destroy_all
end
```

and add it to `ApplicationRecord`.

Thay way, e.g., BookingsFees that are locally associated to the Booking but were not removed yet (but were on the Producer side, that's why they are no longer in the payload) will be cleaned up.

### Handling deletion/cancelation/restoration (soft-delete)

By default, all records are restored on create/update event by setting `soft_deleted_at_timestamp_attribute` (by default, `synced_canceled_at`) to nil.

For soft delete, there are a couple of ways this can work:
- if it's possible to soft delete record by setting a timestamp, it will be done that way (e.g., by setting `:synced_canceled_at` to a timestamp from payload)
- if `canceled_at` is not available in the payload, but the model responds to the method configured via `soft_delete_strategy` (by default: `:cancel`), that method will be called.
- if there is no other option to soft-delete the record, `destroy` method will be called.

#### Batch import

For high volume of data, you don't probably want to process records one by one, but batch-import them. In that case, you can specify `import` option for the topic:

``` rb
topic :heavy_records, import: true
```

That way, for `heavy_record_created` event there will not be persistence executed one by one, but instead, `dionysus_import` method will be called on the object returned by a model factory (here, most likely HeavtRecord class, or any other model class). The argument of the method is an array of deserialized data access objects responding to `attributes`, `has_many` and `has_one` methods. You may want to inspect the payload of each of them, although most likely you will be just interested here in the `attributes` only (unless something is sideloaded) which will contain the serialized payload (on the producer side) for attributes with some transformations applied on top for reserved attributes (id, created_at, canceled_at, updated_at).

Also, this will impact `heavy_record_destroyed` event. In that case, you need to handle the logic using `dionysus_destroy` method that is called exactly in the same way as `dionysus_import`. The recommended way to handle the payload would be to extract IDs and find the corresponding records and (sof)delete them using `update_all` method with a single query.

#### Batch transformation

You can perform some transformations on the batch of records before the batch is processed. By default `Dionysus::Consumer::ParamsBatchTransformations::RemoveDuplicatesStrategy` is applied which removes duplicates for `_updated` events from the batch (based on message `key`) and keeps the most recent event only.

You can disable it by explicitly setting it to `nil`:

``` rb
topic :my_topic, params_batch_transformation: nil
```

It is also a very useful addition when using `import: true` option. `params_batch` will always contain multiple items that will be processed sequentially, one by one. When using `import: true` that probably doesn't make much sense for the performance reasons and it might a better idea to just merge all the batches into a single one or some grouped batches.

You can do that by applying `params_batch_transformation` which expects an object with lambda-like interface responding to `call` method taking a single argument which is `params_batch`:

``` rb
topic :heavy_records, import: true, params_batch_transformation: ->(params_batch) { do_some_merging_logic_here }
```

#### Concurrency

If you process records only from a single topic/partition, you will not have any issue with concurrent processing of the same record, but if you process from multiple partitions where the same record can get published, you might run into some conflicts. In such a case, you might consider using a mutex, like advisory lock from Postgres. You can use `processing_mutex_provider` and `processing_mutex_method_name` for that that:

``` rb
Dionysus::Consumer.configure do |config|
  config.processing_mutex_provider = ActiveRecord::Base # optional
  config.processing_mutex_method_name = :with_advisory_lock # optional, https://github.com/ClosureTree/with_advisory_lock
end
```

Keep in mind that this is going impact database load.

#### Storing entire payload

It might be the case that when you added a given model to consuming application it didn't contain all the attributes that were serialized in the message and these attribute will be needed in the future. You have 2 options how to handle it:

1. Reset offset and consume everything from the beginning (absolutely not recommended for large volume of data)
2. Store all the attributes that were present in the message for a given record so that you can reuse them later

Dionysus forces you to go with the second option and expects that all entities will have `synced_data` accessor (although the name is configurable) which will store that payload.

If you want to configure the behavior of this attribute (you might for example want to store the data as a separate model), just override the attribute:

``` rb
class MyModel < ApplicationRecord
  after_save :persist_synced_data

  attr_accessor :synced_data

  def synced_data=(data)
    @synced_data = data
  end

  private

  def persist_synced_data
    synced_data_entity = SyncedDataEntity.find_or_initialize_by(model_name: self.class.to_s, model_id: id)
    synced_data_entity.synced_data = synced_data
    synced_data_entity.save!
  end
end
```

That way you can store the payloads under a polymorphic SyncedDataEntity model. Or you can avoid storing anthing if that's your choice.

##### Assigning values from `synced_data`

Sometimes it might happen that you would like to assign a value from `synced_data` to a model's column, e.g., when some column was missing.

To do that, you can use `Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedDataJob.enqueue`:

``` rb
Dionysus::Consumer::SyncedData::AssignColumnsFromSyncedDataJob.enqueue(model_class, columns, batch_size:) # batch_size defaults to 1000
```

To make it work, you need to make sure these values on the config level are properly set:


``` rb
Dionysus::Consumer.configure do |config|
  config.resolve_synced_data_hash_proc = ->(record) { record.synced_data_model.synced_data_hash } # optional, defaults to ->(record) { record.public_send(Dionysus::Consumer.configuration.synced_data_attribute).to_h }
  config.sidekiq_queue = :default # optional, defaults to `:dionysus`
end
```

If you store `synced_data` as a `jsonb` attribute on the model level, you don't need to adjust `resolve_synced_data_hash_proc`.


#### Globalize extensions

(This is no related to Dionysis itself, but might be useful)

If you use `globalize` gem, there might be a chance that you will serialize translatable attributes to the following format:

```rb
{
  "translatable_attribute" => {
    "en" => "English",
    "fr" => "French"
  }
}
```

To handle translated attributes correctly on the consumer, you might need the following patch for `globalize` gem, you can put it, e.g., in the initializer:

``` rb
# frozen_string_literal: true

module Globalize::ActiveRecord::ClassMethods
  protected

  def define_translated_attr_writer(name)
    define_method(:"#{name}=") do |value|
      if value.is_a?(Hash)
        send("#{name}_translations").each_key { |locale| value[locale] ||= "" }
        value.each do |(locale, val)|
          write_attribute(name, val, locale: locale)
        end
      else
        write_attribute(name, value)
      end
    end
  end

  def define_translations_accessor(name)
    attribute(name, ::ActiveRecord::Type::Value.new) if Globalize.rails_5?
    define_translations_reader(name)
    define_translations_writer(name)
    define_translation_used_locales(name)
  end

  def define_translation_used_locales(name)
    define_method(:"#{name}_used_locales") do
      send("#{name}_translations").select { |_key, value| value.present? }.keys
    end
  end
end
```

And the specs (you might need to adjust a `model`):

``` rb
# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Globalize extensions" do
  describe "assigning hash" do
    subject(:model_name_translations) { model.name_translations }

    context "when hash is not empty" do
      let(:assign_name) { model.name = name_translations }
      let(:model) { Record.new }
      let(:name_translations) do
        {
          "en" => "record",
          "fr" => "record in French"
        }
      end

      it "assigns values to a proper locale" do
        assign_name

        expect(model_name_translations).to eq name_translations
      end
    end

    context "when hash is empty and some translations were assigned before" do
      let(:assign_name) { model.name = name_translations }
      let(:model) { Record.new }
      let(:original_translations) do
        {
          "en" => "record",
          "fr" => "record in French"
        }
      end
      let(:name_translations) do
        {}
      end
      let(:expected_result) do
        {
          "en" => "",
          "fr" => ""
        }
      end

      before do
        model.name = original_translations
      end

      it "assigns nullified values for all locales" do
        assign_name

        expect(model_name_translations).to eq expected_result
      end
    end
  end

  describe "used locales" do
    subject(:name_used_locales) { model.name_used_locales }

    let(:assign_name) { model.name = name_translations }
    let(:model) { Record.new }
    let(:name_translations) do
      {
        "en" => "record",
        "fr" => "record in French"
      }
    end

    it "adds a method that extracts used locales" do
      assign_name

      expect(name_used_locales).to match_array %w[en fr]
    end
  end
end


```

#### Config options

Full config reference:

``` rb
Dionysus::Consumer.configure do |config|
  config.transaction_provider = ActiveRecord::Base # not required, but highly recommended
  config.model_factory = DionysusModelFactory # required
  config.instrumenter = MyInstrumentation # optional
  config.processing_mutex_provider = ActiveRecord::Base # optional
  config.processing_mutex_method_name = :with_advisory_lock # optional
  config.event_bus = MyEventBusForDionysus  # optional
  config.soft_delete_strategy = :cancel # optional, default: :cancel
  config.soft_deleted_at_timestamp_attribute = :synced_canceled_at # optional, default: :synced_canceled_at
  config.synced_created_at_timestamp_attribute = :synced_created_at # optional, default: :synced_created_at
  config.synced_updated_at_timestamp_attribute = :synced_updated_at # optional, default: :synced_updated_at
  config.synced_id_attribute = :synced_id # optional, default: :synced_id
  config.synced_data_attribute = :synced_data # required, default: :synced_data
  config.resolve_synced_data_hash_proc = ->(record) { record.synced_data_model.synced_data_hash } # optional, defaults to ->(record) { record.public_send(Dionysus::Consumer.configuration.synced_data_attribute).to_h }
  config.sidekiq_queue = :default # optional, defaults to `:dionysus`
  config.message_filter = FilterIgnoringLargeMessageToAvoidOutofMemoryErrors.new(error_handler: Sentry) # not required, defaults to Dionysus::Utils::DefaultMessageFilter, which doesn't ignore any messages. It can be useful when you want to ignore some messages, e.g. some very large ones that would cause OOM error. Check the implementation of `Dionysus::Utils::DefaultMessageFilter for more details to understand what kind of arguments are available to set the condition. `error_handler` needs to implement Sentry-like interface.

  # if you ever need to provide mapping:

  config.add_attributes_mapping_for_model("Rental") do
    {
      local_rental_type: :remote_rental_type
    }
  end
end
```

#### Instrumentation & Event Bus


Check publisher for reference about instrumentation and event bus. The only difference is about the methods that are instrumented and events that are published.

For the event bus, you may expect the `dionysus.consume` event. It contains the following attributes:
- `topic_name`, e.g. "v3_inbox", "v3_rentals"
- `model_name`, e.g. "Conversation", "Rental"
- `event_name`, e.g. "rental_created", "converation_updated", "message_destroyed"
- `transformed_data`, deserialized event payload. Please check out DeserializedRecord in `Dionysus::Consumer::Deserializer`
- `local_changes`, this contains all changes that took place while handling this event. It contains a hash with keys as array of two elements: model/relationship name and its id from Core. Every value is a result of `ActiveModel#changes` that is called before committing it locally. This will contain changes of the main resource as well as all of relationships included. An example of possible value:

``` rb
{
  ["Rental", 1] => {"name" => ["old name", "Villa Saganaki"] },
  ["bookings", 101] => {"start_at" => [nil, 1] }
}
```

Event bus is the recommended way to do something upon consuming events if you want to avoid putting that logic into ActiveRecord callbacks.



#### Karafka Worker Health check

If you want to perform a karafka health check (for consumer apps), use `Dionysus::Checks::HealthCheck.check`.

To make it work, you need to assign healthcheck to `Dionysus`:

``` rb
# in the initializer, after calling `initialize_application!`
Dionysus.health_check = Dionysus::Checks::HealthCheck.new
```

It works for both readiness and liveness probes. However, keep in mind that you need to enable statistics emission for Karafka to liveness checks work (by setting `statistics.interval.ms' - [more about it here](https://karafka.io/docs/Monitoring-and-Logging/#naming-considerations-for-custom-events)).

To perform the actual health check, use `bin/karafka_health_check`. On success, the script exits with `0` status and on failure, it logs the error and exits with `1` status.

```
bundle exec karafka_health_check
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/BookingSync/dionysus-rb.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
