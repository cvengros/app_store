require 'gooddata'
require 'restforce'
require './salesforce_csv'

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
  ExecuteSalesforceCSVBrick
])

p.call($SCRIPT_PARAMS)