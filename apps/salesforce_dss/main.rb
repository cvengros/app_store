require 'gooddata'
require 'restforce'
require './salesforce_dss'
require '../salesforce_csv/salesforce_csv'
require '../csv_dss/csv_dss'


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
  CSVDSSMiddleware,
  ExecuteSalesforceDssBrick
])

p.call($SCRIPT_PARAMS)