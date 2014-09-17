require 'gooddata'
require 'json'
require 'gooddata_connectors_downloader_salesforce'


include GoodData::Bricks

module GoodData::Bricks
  class ExecuteSalesforceDssBrick
    def call(params)
      # download
      downloaded_info = params["salesforce_downloader"].run
      params["GDC_LOGGER"].info "Download finished. This is the info: #{JSON.pretty_generate({'local_files' => {'sfdc' => downloaded_info}})}" if params["GDC_LOGGER"]

      # if UserRole downloaded
      if downloaded_info['local_files']['salesforce']['objects']['UserRole']
        # generate subordinates from userrole for each downloaded file
        subordinate_files = []
        file_info = nil
        downloaded_info['local_files']['salesforce']['objects']['UserRole']['filenames'].each do |filename|
          user_hierarchy = GoodData::UserHierarchies::UserHierarchy.read_from_csv(
            filename,
            params['hierarchy']['symbolized_config']
          )
          file_info = user_hierarchy.subordinates_closure_tuples_to_file(
            params['hierarchy']['output_fields'],
            params['salesforce_downloader'].data_directory
          )
          subordinate_files += file_info["UserRoleHierarchy"]["filenames"]
        end
        file_info["UserRoleHierarchy"]["filenames"] = subordinate_files
        downloaded_info['local_files']['salesforce']['objects'].merge!(file_info)
      end

      # load to dss
      params["dss"].save_full(downloaded_info)
    end
  end
end