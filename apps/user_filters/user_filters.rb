# utf-8
require 'open-uri'
require 'csv'
require 'gooddata'
require_relative 'user_filter_builder'

module GoodData::Bricks
  class UserFiltersMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse.json')
      @config_namespace = 'visualization__gd__user_filters'
      super(options)
    end

    def default_loaded_call(params)
      # prepare whatever is needed - config, if config given, if not do nothing
      config = params['config']['visualization']['gd']['user_filters']
      if config
        domain_name = config['domain']
        domain = GoodData::Domain[domain_name] if domain_name

        filters_filepath = config['filepath']
        config = config['setup']
        symbolized_config = config.deep_dup
        symbolized_config.symbolize_keys!
        symbolized_config[:labels].each {|l| l.symbolize_keys!}

        params['domain'] = domain
        params['user_filters'] = {
          'symbolized_config' => symbolized_config,
          'filepath' => filters_filepath,
        }
      end
      @app.call(params)
    end
  end


  class ExecuteUserFiltersBrick
    def call(params)
      GoodData::UserFilterBuilder.build(
        params['user_filters']['filepath'],
        params['user_filters']['symbolized_config'],
        params['domain']
      ) if params['user_filters']
    end
  end
end
