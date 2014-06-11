require 'gooddata'

require './dss_save'


include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DssSaveBrick
])

p.call($SCRIPT_PARAMS)