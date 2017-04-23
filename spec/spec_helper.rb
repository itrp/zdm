$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
$LOAD_PATH.unshift File.dirname(__FILE__)

$stderr.puts("Running Specs using Ruby v#{RUBY_VERSION}")

require 'rspec'
require 'logger'
require 'zdm'
require 'active_record'
require 'yaml'
require 'erb'

# require 'rspec/support'
# RSpec::Support.require_rspec_support "object_formatter"
# RSpec::Support::ObjectFormatter.default_instance.max_formatted_output_length = nil

config = YAML::load(ERB.new(IO.read(File.dirname(__FILE__) + '/database.yml')).result)
ActiveRecord::Base.establish_connection(config['test'])

ActiveRecord::Schema.define version: 0 do
  create_table :people, force: true do |t|
    t.integer :account_id
    t.string :name, limit: 30
    t.string :code
    t.datetime :created_at
  end
  add_index(:people, :name, unique: true)
  add_index(:people, [:account_id, :code], length: {account_id: nil, code: 191})
  add_index(:people, :created_at)

  create_table :people_teams, id: false, force: true do |t|
    t.integer :team_id, null: false
    t.integer :person_id, null: false
  end
end

ActiveRecord::Base.connection.execute(%[INSERT INTO people(account_id, name, code, created_at) VALUES (10,'foo','bar','2017-03-01 23:59:59')])
ActiveRecord::Base.connection.execute(%[INSERT INTO people(account_id, name, code, created_at) VALUES (20,'foo2','bar2','2017-03-02 23:59:59')])

# ActiveRecord::Base.logger = Logger.new($stdout)

