require 'gooddata'
require './csv_gd'
require '../user_filters/user_filters'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  GoodDataModelMiddleware,
  UserFiltersMiddleware,
  ExecuteCSVGDBrick
])

p.call($SCRIPT_PARAMS)