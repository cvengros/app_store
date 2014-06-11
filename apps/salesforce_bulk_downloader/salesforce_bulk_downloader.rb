require 'gooddata'
require 'json'

require '../salesforce_history_downloader/downloader'

include GoodData::Bricks

module GoodData::Bricks

  # Downloading from SFDC
  class SalesforceBulkDownloaderMiddleware < GoodData::Bricks::Middleware
    def call(params)
      downloaded_info = GoodData::Bricks::SalesForceHistoryDownloader.new(params).run
      params["GDC_LOGGER"].info "Download finished. This is the info (use in params if you want to upload later: #{JSON.pretty_generate({:salesforce_downloaded_info => downloaded_info})}" if params["GDC_LOGGER"]
      @app.call(params.merge(:salesforce_downloaded_info => downloaded_info))
    end
  end

  # Downloading from SFDC
  class SalesforceBulkDownloaderBrick < GoodData::Bricks::Middleware
    def call(params)
      downloaded_info = GoodData::Bricks::SalesForceHistoryDownloader.new(params).run
      params["GDC_LOGGER"].info "Download finished. This is the info (use in params if you want to upload later: #{JSON.pretty_generate({:salesforce_downloaded_info => downloaded_info})}" if params["GDC_LOGGER"]
    end
  end
end