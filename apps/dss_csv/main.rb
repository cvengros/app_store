require 'gooddata'
require 'restforce'

require './dss_csv'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DSSCSVMiddleware,
  ExecuteDSSCSVBrick
])

p.call($SCRIPT_PARAMS)