require 'gooddata'
require './csv_gd'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  GoodDataModelMiddleware,
  ExecuteCSVGDBrick
])

p.call($SCRIPT_PARAMS)