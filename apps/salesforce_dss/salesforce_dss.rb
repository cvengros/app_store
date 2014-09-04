require 'gooddata'
require 'json'
require 'gooddata_connectors_downloader_salesforce'


include GoodData::Bricks

module GoodData::Bricks
  class ExecuteSalesforceDssBrick
    def call(params)
      downloaded_info = params["salesforce_downloader"].run
      params["GDC_LOGGER"].info "Download finished. This is the info: #{JSON.pretty_generate({'local_files' => {'sfdc' => downloaded_info}})}" if params["GDC_LOGGER"]
      params["dss"].save_full(downloaded_info)
    end
  end
end