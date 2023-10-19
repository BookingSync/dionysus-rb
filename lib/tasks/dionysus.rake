# frozen_string_literal: true

namespace :dionysus do
  desc "Starts the Dionysus RB Outbox Producer"
  task producer: :environment do
    $stdout.sync = true
    Rails.logger.info("Running dionysus:producer rake task.")
    threads_number = ENV.fetch("DIONYSUS_RB_THREADS_NUMBER", 1).to_i
    Dionysus::Producer.start_outbox_worker(threads_number: threads_number)
  end

  desc "Validates config for :attributes for observables"
  task validate_columns: :environment do
    $stdout.sync = true
    Rails.logger.info("Running dionysus:validate_columns rake task.")
    Dionysus::Producer::Registry::Validator.new.validate_columns
  end
end
