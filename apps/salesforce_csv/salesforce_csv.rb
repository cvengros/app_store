require 'gooddata'
require 'json'
require 'gooddata_connectors_downloader_salesforce'


include GoodData::Bricks

module GoodData::Bricks
  # Downloading from SFDC
  class SalesforceBulkDownloaderMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse.json')
      @config_namespace = 'downloader__salesforce'
      super(options)
    end

    def default_loaded_call(params)
      # add the object as middleware
      ents = params['config']['downloader']['salesforce']['entities']
      params['config']['downloader']['salesforce']['entities'] = JSON.parse(ents) unless ents.is_a? Array

      lims = params['config']['downloader']['salesforce']['limit_entity_fields']
      params['config']['downloader']['salesforce']['limit_entity_fields'] = JSON.parse lims if lims && (! lims.is_a? Hash)


      params["salesforce_downloader"] = GoodData::Connectors::Downloader::SalesforceDownloader.new(nil, params)
      @app.call(params)

    end
  end

  class ExecuteSalesforceCSVBrick
    def call(params)
      downloaded_info = params["salesforce_downloader"].run
      params["GDC_LOGGER"].info "Download finished. This is the info: #{JSON.pretty_generate(downloaded_info)}" if params["GDC_LOGGER"]
      if params['config']['downloader']['salesforce']['upload_to_webdav']
        # pass the param with what to upload to the webdav middleware
        params['gdc_files_to_upload'] = downloaded_info['local_files']['salesforce']['objects'].map{|_,v| v["filenames"]}.flatten.map{|f| {path: f, webdav_directory: params['config']['downloader']['salesforce']['webdav_directory']}}
      end
      return downloaded_info
    end
  end
end