require 'gooddata'
require 'gooddata_connectors_dss'

include GoodData::Bricks

module GoodData::Bricks
  class DSSCSVMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse/gse.json')
      @config_namespace = 'storage__dss'
      super(options)
    end

    def default_loaded_call(params)
      # make the paths to sqls absolute
      base_dir =  File.dirname(@config)
      params['config']['storage']['dss']['dataset_mapping'].each  do |s, objs|
        objs.each do |obj, info|
          if info["extract_sql"]
            info["extract_sql"] = File.join(base_dir, info["extract_sql"])
          end
        end
      end

      # add the object as middleware
      params["dss"] = GoodData::Connectors::Storage::Dss.new(nil, params)
      @app.call(params)

    end
  end

  class ExecuteDSSCSVBrick
    def call(params)
      # tady naky extract
      info = params["dss"].extract
      params["GDC_LOGGER"].info "Upload to dss finished #{info}" if params["GDC_LOGGER"]
      return info
    end
  end
end