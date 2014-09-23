require 'gooddata'

require '../user_sync/user_sync'


include GoodData::Bricks

module GoodData::Bricks

  class ExecuteDSSGDBrick
    def call(params)
      # download from dss
      extract_info = params["dss"].extract

      model = params["model_blueprint"]

      # for each defined dataset
      # extract_info["dataset_mapping"].each do |dataset, ds_structure|
      #   if model.dataset?(dataset)
      #     # get it from the model and load it
      #     GoodData::Model.upload_data(ds_structure["csv_filename"], model, dataset)
      #   end
      # end

      # if user sync should be done, do it.
      # if params['config']['visualization']['gd']['user_sync']
      #   params["GDC_LOGGER"].info "Syncing user and project users" if params["GDC_LOGGER"]
      #   params['config']['visualization']['gd']['user_sync']['filepath'] = extract_info['dataset_mapping']['gooddata_user_details']['csv_filename']
      #   ExecuteUserSyncBrick.temp_call(params)
      # end

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
