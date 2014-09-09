require 'gooddata'

include GoodData::Bricks

module GoodData::Bricks
  class GoodDataModelMiddleware < GoodData::Bricks::Middleware
    def initialize(options={})
      @config = File.join(File.dirname(__FILE__), 'config/gse.json')
      @config_namespace = 'visualization__gd'
      super(options)
    end

    def default_loaded_call(params)
      # add the object as middleware
      json = params["config"]["visualization"]["gd"].to_json
      params["model_blueprint"] = GoodData::Model::ProjectBlueprint.from_json(json)
      @app.call(params)
    end
  end

  class ExecuteCSVGDBrick
    def call(params)

      model = params["model_blueprint"]
      # for each defined dataset
      params["config"]["visualization"]["gd"]["dataset_mapping"].each do |dataset, ds_structure|
        # get it from the model and load it
        ds = model.find_dataset(dataset)
        ds.upload(ds_structure["csv_filename"])
      end
    end
  end
end