require 'gooddata'
require './user_filters'
require './user_filters_brick'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  UserFiltersBrick])

p.call($SCRIPT_PARAMS)
