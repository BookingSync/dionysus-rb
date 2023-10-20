# frozen_string_literal: true

if defined?(Rails)
  class Dionysus::Railtie < Rails::Railtie
    rake_tasks do
      load "tasks/dionysus.rake"
    end
  end
end
