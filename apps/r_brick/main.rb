# encoding: utf-8
require 'gooddata'

require './r_brick'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  DecodeParamsMiddleware,
  LoggerMiddleware,
  BenchMiddleware,
  RBrick,
])

p.call($SCRIPT_PARAMS.to_hash)
