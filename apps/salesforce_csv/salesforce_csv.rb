require 'gooddata'
require 'json'
require 'gooddata_connectors_downloader_salesforce'


include GoodData::Bricks

module GoodData::Bricks
  # Downloading from SFDC
  class SalesforceBulkDownloaderMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = 'config/gse.json'
      @config_namespace = 'downloader__salesforce'
      super(options)
    end

    def default_loaded_call(params)
      # add the object as middleware
      params["salesforce_downloader"] = GoodData::Connectors::Downloader::SalesforceDownloader.new(nil, params)
      @app.call(params)

    end
  end

  class ExecuteSalesforceCSVBrick
    def call(params)
      downloaded_info = params["salesforce_downloader"].run
      params["GDC_LOGGER"].info "Download finished. This is the info: #{JSON.pretty_generate({'local_files' => {'sfdc' => downloaded_info}})}" if params["GDC_LOGGER"]
      return downloaded_info
    end
  end
end