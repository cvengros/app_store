# utf-8
require 'open-uri'
require 'csv'
require 'gooddata'

module GoodData::Bricks
  class UserSyncMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse.json')
      @config_namespace = 'visualization__gd__user_sync'
      super(options)
    end

    def default_loaded_call(params)
      @app.call(params)
    end
  end

  class ExecuteUserSyncBrick
    def call(params)
      self.class.temp_call(params)
    end
    def self.temp_call(params)
      GoodData.logging_on

      domain_name = params['config']['visualization']['gd']['domain']
      config = params['config']['visualization']['gd']['user_sync']

      csv_path = config['filepath']
      # ignore_logins can be an array, make it a regexp
      il = config['ignore_logins']
      il = [il] if il && (! il.is_a? Array)
      ignore_logins = il ? il.map{|login| Regexp.new(login)} : nil

      project = params['gdc_project']

      import_results = project.import_users_csv(csv_path, {
        :domain_name => domain_name,
        :whitelists => ignore_logins

        # merge with symbolized config
      }.merge(Hash[config.map{|(k, v)| [k.to_sym,v]}]))

    end
  end
end
