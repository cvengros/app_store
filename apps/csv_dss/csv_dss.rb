require 'gooddata'
require 'gooddata_connectors_dss'

include GoodData::Bricks

module GoodData::Bricks
  class CSVDSSMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = 'config/gse.json'
      @config_namespace = 'storage__dss'
      super(options)
    end

    def default_loaded_call(params)
      # add the object as middleware
      params["dss"] = GoodData::Connectors::Storage::Dss.new(nil, params)
      @app.call(params)

    end
  end

  class ExecuteCSVDSSBrick
    def call(params)
      info = params["dss"].save_full(params['config'])
      params["GDC_LOGGER"].info "Upload to dss finished" if params["GDC_LOGGER"]
      return info
    end
  end
end