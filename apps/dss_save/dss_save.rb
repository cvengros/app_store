require 'gooddata'

require 'json'
require '../dss_execute/executor'
include GoodData::Bricks
module GoodData::Bricks

  # Saving to DSS
  class DssSaveBrick
    def call(params)
      executor = GoodData::Bricks::DssExecutor.new(params)
      downloaded_info = params['local_files']

      downloaded_info.each do |source, info|
        # create dss tables
        executor.create_tables(source, info, params['dss_historized_objects'])

        # load the data as is
        executor.load_data(source, info)

        # reshuffle the data to the historization tables
        executor.load_historization_data(source, info, params['dss_historized_objects'])
      end
    end
  end
end