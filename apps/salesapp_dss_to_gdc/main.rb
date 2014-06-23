require 'gooddata'
require '../dss_extract/dss_extract'
require '../gd_load/gd_load'
include GoodData::Bricks


p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DssExtractMiddleware,
  GoodDataMiddleware,
  GDLoadBrick
])

p.call($SCRIPT_PARAMS)
