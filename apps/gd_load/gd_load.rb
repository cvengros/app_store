require 'gooddata'

include GoodData::Bricks

module GoodData::Bricks

  # takes csvs and loads them to gd
  class GDLoadBrick
    def call(params)
      if ! params["gooddata_model_url"]
        raise "missing gooddata_model_url in params"
      end
      json = RestClient.get(params["gooddata_model_url"])
      model = GoodData::Model::ProjectBlueprint.from_json(json)

      # for each defined dataset
      params["dataset_mapping"].each do |datasource, datasets|
        datasets.each do |dataset, ds_structure|
          # get it from the model and load it
          ds = model.find_dataset(dataset)
          ds.upload(ds_structure["csv_filename"])
        end
      end
    end
  end
end