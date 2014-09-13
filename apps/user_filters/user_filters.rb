# utf-8
require 'open-uri'
require 'csv'
require 'gooddata'
require_relative 'user_filter_builder'

module GoodData::Bricks
  class UserFiltersMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse.json')
      @config_namespace = 'visualization__gd'
      super(options)
    end

    def default_loaded_call(params)
      config = params['config']['visualization']['gd']
      domain_name = config['domain']
      domain = GoodData::Domain[domain_name] if domain_name

      filters_filepath = config['filters_filepath']
      config = config['filters_setup']
      symbolized_config = config.deep_dup
      symbolized_config.symbolize_keys!
      symbolized_config[:labels].each {|l| l.symbolize_keys!}

      params['domain'] = domain
      params['user_filters'] = {
        'symbolized_config' => symbolized_config,
        'filters_filepath' => filters_filepath,
      }
      @app.call(params)
    end
  end


  class ExecuteUserFiltersBrick
    def call(params)
      GoodData::UserFilterBuilder.build(
        params['user_filters']['filters_filepath'],
        params['user_filters']['symbolized_config'],
        params['domain']
      )
    end
  end
end
