require 'gooddata'
require 'restforce'
require './csv_dss'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  CSVDSSMiddleware,
  ExecuteCSVDSSBrick
])

p.call($SCRIPT_PARAMS)