lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'vmpooler-provider-vcd/version'

Gem::Specification.new do |s|
  s.name          = 'vmpooler-provider-vcd'
  s.version       = VmpoolerProviderVcd::VERSION
  s.authors       = ['Puppet']
  s.email         = ['support@puppet.com']

  s.summary       = 'Vcloud director provider for VMPooler'
  s.homepage      = 'https://github.com/cjswart/vmpooler-provider-vcd'
  s.license       = 'Apache-2.0'
  s.required_ruby_version = Gem::Requirement.new('>= 2.3.0')

  s.files         = Dir[ "lib/**/*" ]
  s.require_paths = ["lib"]
  s.add_dependency 'rbvmomi2', '>= 3.1', '< 4.0'
  s.add_dependency 'vmpooler', '~> 3.0'

  # Testing dependencies
  s.add_development_dependency 'climate_control', '>= 0.2.0'
  s.add_development_dependency 'mock_redis', '>= 0.17.0'
  s.add_development_dependency 'pry'
  s.add_development_dependency 'rack-test', '>= 0.6'
  s.add_development_dependency 'rspec', '>= 3.2'
  s.add_development_dependency 'rubocop', '~> 1.28.2'
  s.add_development_dependency 'simplecov', '>= 0.11.2'
  s.add_development_dependency 'thor', '~> 1.0', '>= 1.0.1'
  s.add_development_dependency 'yarjuf', '>= 2.0'
end
