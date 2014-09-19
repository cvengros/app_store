require 'gooddata'
require 'restforce'
require '../dss_csv/dss_csv'
require '../csv_gd/csv_gd'
require './dss_gd'
require '../user_filters/user_filters'
require '../user_sync/user_sync'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DSSCSVMiddleware,
  GoodDataMiddleware,
  GoodDataModelMiddleware,
  UserSyncMiddleware,
  UserFiltersMiddleware,
  ExecuteDSSGDBrick
])

p.call($SCRIPT_PARAMS)