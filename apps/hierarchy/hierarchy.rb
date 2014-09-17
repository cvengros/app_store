# encoding: utf-8

require 'open-uri'
require 'csv'
require 'gooddata'
require 'user_hierarchies'

module GoodData::Bricks
  class HierarchyMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse.json')
      @config_namespace = 'preprocessing__hierarchy'
      super(options)
    end
    def default_loaded_call(params)
      # prepare whatever is needed
      config = params['config']['preprocessing']['hierarchy']
      output_fields = params['output_fields'] || []
      symbolized_config = config['setup'].symbolize_keys

      params['hierarchy'] = {
        'symbolized_config' => symbolized_config,
        'output_fields' => output_fields
      }
      @app.call(params)
    end
  end

  class ExecuteTuplesHierarchyBrick
    def call(params)
      user_hierarchy = GoodData::UserHierarchies::UserHierarchy.read_from_csv(
        params['config']['preprocessing']['hierarchy']['filepath'],
        params['hierarchy']['symbolized_config']
      )
      user_hierarchy.subordinates_closure_tuples_to_file(
        params['hierarchy']['output_fields'],
        'hierarchy_out')
    end
  end
end
