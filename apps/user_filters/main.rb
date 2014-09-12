require 'gooddata'
require './user_filters'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  UserFiltersMiddleware,
  ExecuteUserFiltersBrick])

p.call($SCRIPT_PARAMS)
