require 'gooddata'
require 'json'
require '../dss_execute/executor'

include GoodData::Bricks

module GoodData::Bricks

  module CallExecutor
    def call_executor(params)
      executor = GoodData::Bricks::DssExecutor.new(params)
      extended_datasets = executor.extract_data(params["dataset_mapping"])

      params["GDC_LOGGER"].info "Extract finished. This is the info (use in params if you want to upload to GD later: #{JSON.pretty_generate({'dataset_mapping' => extended_datasets})}" if params["GDC_LOGGER"]
      return extended_datasets
    end
  end
  # takes stuff from dss and puts it into a csv
  class DssExtractMiddleware < GoodData::Bricks::Middleware
    include CallExecutor

    def call(params)
      extended_datasets = call_executor(params)

      @app.call(params.merge({"dataset_mapping" => extended_datasets}))
    end
  end

  # takes stuff from dss and puts it into a csv
  class DssExtractBrick
    include CallExecutor

    def call(params)
      call_executor(params)
    end
  end

end