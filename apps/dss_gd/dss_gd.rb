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

      # if user sync should be done, do it.


      # if userFilters given, apply them
      if params["user_filters"]
        params["GDC_LOGGER"].info "Applying user data permissions" if params["GDC_LOGGER"]
        GoodData::UserFilterBuilder.build(
          extract_info['dataset_mapping']['gooddata_user']['csv_filename'],
          params['user_filters']['symbolized_config'],
          params['domain']
        )
      end
    end
  end
end