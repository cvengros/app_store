require 'gooddata'
require '../dss_execute/executor'

include GoodData::Bricks

module GoodData::Bricks
  # takes stuff from dss and puts it into a csv
  class DssExtractMiddleware < GoodData::Bricks::Middleware
    def call(params)
      executor = GoodData::Bricks::DssExecutor.new(params)

      extended_datasets = executor.extract_data(params["dataset_mapping"])
      @app.call(params.merge({"dataset_mapping" => extended_datasets}))
    end
  end

  # takes csvs and loads them to gd
  class GoodDataLoadBrick
    def call(params)
      if ! params["gooddata_model_url"]
        raise "missing gooddata_model_url in params"
      end
      json = RestClient.get(params["gooddata_model_url"])
      model = GoodData::Model::ProjectBlueprint.from_json(json)

      # for each defined dataset
      params["dataset_mapping"].each do |dataset, ds_structure|
        # get it from the model and load it
        ds = model.find_dataset(dataset)
        ds.upload(ds_structure["csv_filename"])
      end
    end
  end
end

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  DssExtractMiddleware,
  GoodDataMiddleware,
  GoodDataLoadBrick
])

p.call($SCRIPT_PARAMS)
