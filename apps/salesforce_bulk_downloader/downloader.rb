require 'set'

module GoodData::Bricks
  class SalesForceHistoryDownloader < BaseDownloader

    # TZ = ActiveSupport::TimeZone.new('UTC')
    # DATE_FROM = DateTime.now.advance(:days => -29).in_time_zone(TZ).iso8601
    # DATE_TO = DateTime.now.in_time_zone(TZ).iso8601

    # objects = ["Account", "Opportunity", "User", "Contact", "Lead", "Case", "Contract", "Product2", "Task", "Event"]
    DIRNAME = "tmp"

    # returns a hash objects -> what fields have been downloaded
    def download
      downloaded_info = {:objects => {}}
      client = @params["salesforce_client"]
      bulk_client = @params["salesforce_bulk_client"]
      objects = @params["salesforce_objects"]
      created_from = @params["salesforce_created_from"]
      created_to = @params["salesforce_created_to"]
      history_objects = @params["salesforce_history_objects"] || []
      historized_objects = Set.new(@params["salesforce_historized_objects"] ? @params["salesforce_historized_objects"].keys : [])

      # save the information about download - sfdc server
      instance = bulk_client.instance_url

      # TODO would be nice to take from describe call
      # 1.9.3-p484 :008 > h["urls"]["uiDetailTemplate"]
      # => "https://na12.salesforce.com/{ID}"
      downloaded_info[:salesforce_server] = instance

      # create the directory if it doesn't exist
      Dir.mkdir(DIRNAME) if ! File.directory?(DIRNAME)

      (objects + history_objects).each do |obj|
        name = "tmp/#{obj}-#{DateTime.now.to_i.to_s}.csv"

        obj_fields = get_fields(client, obj)

        main_data = download_main_dataset(client, bulk_client, obj, obj_fields)

        # if it's already in files, just write downloaded_info
        if main_data[:in_files]
          downloaded_info[:objects][obj] = {
            :fields => obj_fields,
            :filenames => main_data[:filenames].map {|f| File.absolute_path(f)},
            :to_be_historized => historized_objects.member?(obj)
          }
        else
          # otherwise write it to csv
          CSV.open(name, 'w', :force_quotes => true) do |csv|
            # get the list of fields and write them as a header
            csv << obj_fields.map {|f| f[:name]}
            downloaded_info[:objects][obj] = {
              :fields => obj_fields,
              :filenames => [File.absolute_path(name)],
              :to_be_historized => historized_objects.member?(obj)
            }

            # write the stuff to the csv
            main_data[:data].map do |row_hash|
              # get rid of the weird stuff coming from the api
              csv_line = row_hash.values_at(*obj_fields.map {|f| f[:name]}).map do |m|
                if m.kind_of?(Array)
                  m[0] == {"xsi:nil"=>"true"} ? nil : m[0]
                else
                  m
                end
              end
              csv << csv_line
            end
          end
        end

      end
      return downloaded_info
    end

    private

    def download_main_dataset(client, bulk_client, obj, fields)
      created_from = @params["salesforce_created_from"]
      created_to = @params["salesforce_created_to"]
      single_batch = @params["salesforce_single_batch"]

      q = construct_query(obj, fields)
      logger = @params["GDC_LOGGER"]
      logger.info "Executing soql: #{q}" if logger

      begin
        filename_prefix = @params[:dss_table_prefix] ? @params[:dss_table_prefix] + '_' : nil

        # try it with the bulk
        result = bulk_client.query(obj, q,
          :directory_path => DIRNAME,
          :filename_prefix => filename_prefix,
          :created_from => created_from,
          :created_to => created_to,
          :single_batch => single_batch
        )

        return {
          :in_files => true,
          :filenames => result[:filenames]
        }
      rescue => e
        logger.error "#{e.message}\n\n#{e.backtrace.join("\n")}"
        logger.warn "Batch download failed. Now downloading through REST api instead" if logger
        # if not, try the normal api
        # recreate the query so that it contains the from and to dates
        q = construct_query(obj, fields, created_from, created_to)
        data = client.query(q)
        return {
          :in_files => false,
          :data => data
        }
      end
    end

    # get the list of fields for an object
    def get_fields(client, obj)
      description = client.describe(obj)
      # return the names of the fields
      description.fields.map {|f| {:name => f.name, :type => f.type}}
    end

    def construct_query(obj, fields, created_from=nil, created_to=nil)
      base_query = "SELECT #{fields.map {|f| f[:name]}.join(', ')} FROM #{obj}"
      if created_from && created_to
        return base_query + " WHERE CreatedDate >= #{created_from} AND CreatedDate < #{created_to}"
      end

      if created_from
        return base_query + " WHERE CreatedDate >= #{created_from}"
      end

      if created_to
        return base_query + " WHERE CreatedDate < #{created_to}"
      end

      return base_query
    end

  end
end
