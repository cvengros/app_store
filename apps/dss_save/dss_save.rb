require 'gooddata'

require 'json'
require '../dss_execute/executor'
include GoodData::Bricks
module GoodData::Bricks

  # Saving to DSS
  class DssSaveBrick
    def call(params)
      executor = GoodData::Bricks::DssExecutor.new(params)
      downloaded_info = params[:salesforce_downloaded_info] || params['salesforce_downloaded_info']
      # create dss tables
      executor.create_tables(downloaded_info[:objects] || downloaded_info['objects'])

      # load the data as is
      executor.load_data(downloaded_info)

      # reshuffle the data to the historization tables
      executor.load_historization_data(downloaded_info, params['salesforce_historized_objects'])
    end
  end
end