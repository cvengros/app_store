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
      json = params["config"]["visualization"]["gd"]["model"].to_json
      params["model_blueprint"] = GoodData::Model::ProjectBlueprint.from_json(json)
      @app.call(params)
    end
  end

  class ExecuteCSVGDBrick
    def call(params)

      # load data
      model = params["model_blueprint"]
      # for each defined dataset
      params["config"]["visualization"]["gd"]["dataset_mapping"].each do |dataset, ds_structure|
        # get it from the model and load it
        ds = model.find_dataset(dataset)
        ds.upload(ds_structure["csv_filename"])
      end

      # if userFilters given, apply them
      if params["user_filters"]
        params["GDC_LOGGER"].info "Applying user data permissions" if params["GDC_LOGGER"]

        GoodData::UserFilterBuilder.build(
          params['user_filters']['filepath'],
          params['user_filters']['symbolized_config'],
          params['domain']
        )
      end
    end
  end
end
