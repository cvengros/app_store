# encoding: utf-8

require 'gooddata'
require './hierarchy'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  HierarchyMiddleware,
  ExecuteHierarchyBrick
])

p.call($SCRIPT_PARAMS)
