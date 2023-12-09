# frozen_string_literal: true

class Dionysus::Consumer
  def self.configuration
    @configuration ||= Dionysus::Consumer::Config.new
  end

  def self.configure
    yield configuration
  end

  def self.registry
    configuration.registry
  end

  def self.declare(&config)
    registry = Dionysus::Consumer::Registry.new
    registry.instance_eval(&config)

    Dionysus.inject_routing!(registry)

    configure do |configuration|
      configuration.registry = registry
    end
  end

  def self.reset!
    return if registry.nil?

    registry.registrations.values.flat_map(&:consumers).each do |consumer_class|
      Dionysus.send(:remove_const, consumer_class.name.demodulize.to_sym) if consumer_class.name
    end
    @configuration = nil
    Dionysus.inject_routing!(nil)
  end
end
