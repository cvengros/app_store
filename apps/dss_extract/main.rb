require 'gooddata'

require './dss_extract'

include GoodData::Bricks


p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DssExtractBrick
])

p.call($SCRIPT_PARAMS)