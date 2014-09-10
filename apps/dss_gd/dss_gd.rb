require 'gooddata'

include GoodData::Bricks

module GoodData::Bricks

  class ExecuteDSSGDBrick
    def call(params)
      # download from dss
      extract_info = params["dss"].extract

      model = params["model_blueprint"]
      # for each defined dataset
      extract_info["dataset_mapping"].each do |dataset, ds_structure|
        # get it from the model and load it
        ds = model.find_dataset(dataset)
        ds.upload(ds_structure["csv_filename"])
      end
    end
  end
end