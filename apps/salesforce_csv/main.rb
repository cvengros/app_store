require 'gooddata'
require 'restforce'
require './salesforce_csv'

include GoodData::Bricks

Restforce.configure do |config|
  config.api_version = '29.0'
  config.timeout = 30
end

p = GoodData::Bricks::Pipeline.prepare([
  UndotParamsMiddleware,
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  RestForceMiddleware,
  BulkSalesforceMiddleware,
  SalesforceBulkDownloaderMiddleware,
  FsProjectUploadMiddleware.new(:destination => :staging),
  ExecuteSalesforceCSVBrick
])

p.call($SCRIPT_PARAMS)