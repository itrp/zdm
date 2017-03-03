$LOAD_PATH.push File.expand_path('../lib', __FILE__)
require 'version'

Gem::Specification.new do |s|
  s.name          = 'zdm'
  s.version       = Zdm::VERSION
  s.authors       = ['ITRP Institute, Inc.']
  s.email         = ['support@itrp.com']
  s.description   = %q{Zero Downtime Migrator of mysql compatible databases}
  s.summary       = %q{Zero Downtime Migrator for mysql in ruby}
  s.homepage      = 'https://github.com/itrp/zdm'
  s.license       = 'MIT'

  s.files         = `git ls-files`.split($/)
  s.executables   = s.files.grep(%r{^bin/}) { |f| File.basename(f) }
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  s.require_paths = ['lib']

  s.add_dependency 'activerecord', '>= 4.0'

  s.add_development_dependency 'bundler', '~> 1'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'mysql2'
  s.add_development_dependency 'appraisal'
end
