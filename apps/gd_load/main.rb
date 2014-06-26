require 'gooddata'

require './gd_load'

include GoodData::Bricks


p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  GDLoadBrick
])

p.call($SCRIPT_PARAMS)