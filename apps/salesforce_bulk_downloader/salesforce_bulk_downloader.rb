require 'gooddata'
require 'json'
require_relative 'downloader'


include GoodData::Bricks

module GoodData::Bricks
  module CallDownloader
    def call_downloader(params)
      downloaded_info = GoodData::Bricks::SalesForceHistoryDownloader.new(params).run
      params["GDC_LOGGER"].info "Download finished. This is the info (use in params if you want to upload later: #{JSON.pretty_generate({'local_files' => {'sfdc' => downloaded_info}})}" if params["GDC_LOGGER"]
      return downloaded_info
    end
  end


  # Downloading from SFDC
  class SalesforceBulkDownloaderMiddleware < GoodData::Bricks::Middleware
    include CallDownloader
    def call(params)
      downloaded_info = call_downloader(params)
      @app.call(params.merge({
        'local_files' => {
          'sfdc' => downloaded_info
        }
      }))
    end
  end

  # Downloading from SFDC
  class SalesforceBulkDownloaderBrick
    include CallDownloader
    def call(params)
      call_downloader(params)
    end
  end
end