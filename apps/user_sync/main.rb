# encoding: utf-8

require 'gooddata'
require './user_sync'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  GoodDataMiddleware,
  UserSyncMiddleware,
  ExecuteUserSyncBrick])

p.call($SCRIPT_PARAMS)
