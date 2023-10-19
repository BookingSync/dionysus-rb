# frozen_string_literal: true

class Dionysus::Railtie < Rails::Railtie
  rake_tasks do
    load "tasks/dionysus.rake"
  end
end
