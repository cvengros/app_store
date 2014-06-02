require 'gooddata'
require 'restforce'
require '../dss_save/main'
require '../salesforce_bulk_downloader/main'

include GoodData::Bricks

Restforce.configure do |config|
  config.api_version = '29.0'
  config.timeout = 30
end

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  RestForceMiddleware,
  BulkSalesforceMiddleware,
  SalesforceBulkDownloaderMiddleware,
  DssSaveBrick
])

p.call($SCRIPT_PARAMS)