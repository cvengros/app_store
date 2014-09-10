require 'gooddata'
require 'restforce'
require '../dss_csv/dss_csv'
require '../csv_gd/csv_gd'
require './dss_gd'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DSSCSVMiddleware,
  GoodDataMiddleware,
  GoodDataModelMiddleware,
  ExecuteDSSGDBrick
])

p.call($SCRIPT_PARAMS)