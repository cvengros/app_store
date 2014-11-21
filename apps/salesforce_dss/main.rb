require 'gooddata'
require 'restforce'
require './salesforce_dss'
require '../salesforce_csv/salesforce_csv'
require '../csv_dss/csv_dss'
require '../hierarchy/hierarchy'


include GoodData::Bricks

Restforce.configure do |config|
  config.api_version = '29.0'
  config.timeout = 30
end

p = GoodData::Bricks::Pipeline.prepare([
  UndotParamsMiddleware,
  LoggerMiddleware,
  BenchMiddleware,
  RestForceMiddleware,
  BulkSalesforceMiddleware,
  SalesforceBulkDownloaderMiddleware,
  HierarchyMiddleware,
  CSVDSSMiddleware,
  ExecuteSalesforceDssBrick
])

p.call($SCRIPT_PARAMS)