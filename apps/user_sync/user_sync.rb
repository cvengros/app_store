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

      project = GoodData::Project[params['GDC_PROJECT_ID']]
      csv_path = config['filepath']
      only_domain = config['add_only_to_domain']
      whitelists = config['whitelists']

      # Check mandatory columns and parameters

      domain = GoodData::Domain[domain_name]

      first_name_column   = config['first_name_column'] || 'first_name'
      last_name_column    = config['last_name_column'] || 'last_name'
      login_column        = config['login_column'] || 'login'
      password_column     = config['password_column'] || 'password'
      email_column        = config['email_column'] || 'email'
      role_column         = config['role_column'] || 'role'
      sso_provider_column = config['sso_provider_column'] || 'sso_provider'


      sso_provider = config['sso_provider']
      ignore_failures = config['ignore_failures']

      new_users = []

      CSV.foreach(File.open(csv_path, 'r:UTF-8'), :headers => true, :return_headers => false, encoding:'utf-8') do |row|

        json = {
          'user' => {
            'content' => {
              'firstname' => row[first_name_column],
              'lastname' => row[last_name_column],
              'login' => row[login_column],
              'password' => row[password_column],
              'email' => row[email_column] || row[login_column],
              'role' => row[role_column],
              'domain' => domain_name,
              'sso_provider' => sso_provider || row[sso_provider_column]
            },
            'meta' => {}
          }
        }
        new_users << GoodData::Membership.new(json)
      end
      # only add users to domain that aren't there
      domain_users = Set.new(domain.users.map {|u| u.login})
      new_domain_users = new_users.select {|u| ! domain_users.member?(u.login)}
      domain.users_create(new_domain_users)

      project.users_import(new_users, domain)
    end
  end
end
