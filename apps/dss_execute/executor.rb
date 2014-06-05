require 'rubygems'
require 'sequel'
require 'jdbc/dss'

module GoodData::Bricks
  class DssExecutor
    def initialize(params)
      if (!params["dss_GDC_USERNAME"]) || (!params["dss_GDC_PASSWORD"])
        # use the standard ones
        params["dss_GDC_USERNAME"] = params["GDC_USERNAME"]
        params["dss_GDC_PASSWORD"] = params["GDC_PASSWORD"]
      end
      @params = params
      @logger = @params["GDC_LOGGER"]


      Jdbc::DSS.load_driver
      Java.com.gooddata.dss.jdbc.driver.DssDriver
    end

    # expecting hash:
    # table name ->
    #   :fields -> list of columns
    def create_tables(table_hash)
      # create the tables one by one
      table_hash.each do |table, table_meta|
        sql = get_create_sql(table, table_meta[:fields])
        execute(sql)

        # if it should be historized create one more
        if table_meta[:to_be_historized]
          sql = get_create_sql(table, table_meta[:fields], true)
          execute(sql)
        end
      end
    end

    # expecting hash:
    # table name ->
    #   :fields -> list of columns
    #   :filename -> name of the csv file
    def load_data(downloaded_info)

      # save the info and load the tables
      load_id = save_download_info(downloaded_info)

      table_hash = downloaded_info[:objects]

      # load the data for each table and each file to be loaded there
      table_hash.each do |table, table_meta|
        table_meta[:filenames].each do |filename|
          sql = get_upload_sql(table, table_meta[:fields], filename, load_id)
          execute(sql)

          # if there's something in the reject/except, raise an error
          if File.size?(get_except_filename(filename)) || File.size?(get_reject_filename(filename))
            raise "Some of the records were rejected: see #{filename}"
          end
        end
      end

      @data_loaded = true
    end

    def load_historization_data(downloaded_info, historized_objects_params)
      if ! @data_loaded
        raise "No data loaded, nothing to shuffle to historized datasets. First load data with load_data."
      end

      first_load = first_load?
      historized_objects_params.each do |object, hist_info|
        # if we're doing the first load, load data from history table
        if first_load
          load_from_params = historized_objects_params[object]["load_history_from"]
          # if there's a table to load the history from
          if load_from_params
            @logger.info "First load and load_history_from given, so we're loading data from history object for #{object}" if @logger

            # load data from history tables i.e. OpportunityHistory
            load_from_params = historized_objects_params[object]["load_history_from"]

            # for fields use the history object fields
            if ! downloaded_info[:objects][load_from_params["name"]]
              raise "The source for historized object #{load_from_params["name"]} is missing in the downloaded info: #{downloaded_info[:objects]}"
            end
            fields = downloaded_info[:objects][load_from_params["name"]][:fields]

            load_hist_sql = get_history_loading_sql(
              object,
              load_from_params,
              fields
            )
            execute(load_hist_sql)
          end
        else
          @logger.info "Deleting old loads for #{object}" if @logger

          # if it's not the first load only keep the latest load in the merge_from table
          delete_sql = get_delete_but_last_load_sql(historized_objects_params[object]["merge_from"]["name"])
          execute(delete_sql)
        end
        @logger.info "Merging data into history table for #{object}"
        # merge the data from object tables last load i.e. Opportunity
        merge_sql = get_historization_merge_sql(
          object,
          historized_objects_params[object]["merge_from"],
          downloaded_info[:objects][object][:fields]
        )
        execute(merge_sql)
      end
    end

    def get_delete_but_last_load_sql(object)
      table_name = sql_table_name(object)
      return "DELETE FROM #{table_name} WHERE _LOAD_ID <> (SELECT MAX(_LOAD_ID) FROM #{sql_table_name(LOAD_INFO_TABLE_NAME)})"
    end


    def get_history_loading_sql(object, load_history_from_params, object_fields)
      if !@load_id
        raise "load the data first! load_id is empty"
      end
      snapshot_table_name = sql_table_name(object, true)
      history_table_name = sql_table_name(load_history_from_params["name"])
      dest_fields = load_history_from_params["column_mapping"].keys
      src_fields = load_history_from_params["column_mapping"].values

      all_fields = object_fields.map {|o| o[:name]}

      common_fields = (Set.new(all_fields) - Set.new(dest_fields) - Set.new(src_fields)).to_a

      dest_meta = ["_LOAD_ID", "_HASH"]
      src_meta = [@load_id, get_hash_expression(src_fields + common_fields)]

      field_list_dest = (dest_fields + common_fields + dest_meta).join(", ")
      field_list_src = (src_fields + common_fields + src_meta).join(", ")

      sql = "INSERT INTO #{snapshot_table_name} (#{field_list_dest})\n"
      sql += "SELECT #{field_list_src} \n"

      sql += "FROM #{history_table_name} WHERE _LOAD_ID = (SELECT MAX(_LOAD_ID) FROM #{sql_table_name(LOAD_INFO_TABLE_NAME)})"
      return sql
    end

    def get_historization_merge_sql(object, merge_from_params, object_fields)

      # get all the params to be used
      snapshot_table_name = sql_table_name(object, true)
      object_table_name = sql_table_name(merge_from_params["name"])
      fields = object_fields.map {|o| o[:name]}

      sql = "MERGE  INTO #{snapshot_table_name} s USING #{object_table_name} o\n"

      # finding matches in all fields - in the hash
      # string like s.Id = o.Id AND os.StageName = o.StageName

      sql += "ON (s._HASH = o._HASH)\n"

      # when we find a match just update from the column mapping, plus the load id
      col_mapping = merge_from_params["column_mapping"].merge({
        "_LOAD_ID" => "_LOAD_ID",
        "_HASH" => "_HASH"
      })
      set_string = col_mapping.map {|dest, src| "#{dest} = o.#{src}"}.join(", ")
      sql += "WHEN MATCHED THEN UPDATE SET #{set_string}\n"

      # if not insert a new row from the (staging) Opportunity table
      dest_string = (col_mapping.keys + fields).join(", ")
      src_string = (col_mapping.values + fields).map{|f| "o.#{f}"}.join(", ")
      sql += "WHEN NOT MATCHED THEN INSERT (#{dest_string}) VALUES (#{src_string})"

      return sql
    end

    def first_load?
      if @first_load.nil?
        raise "you need to load something first to find out if it was the first load"
      end

      return @first_load
    end

    # .each{|t| puts "DROP TABLE dss_#{t};"}

    LOAD_INFO_TABLE_NAME = 'meta_loads'

    LOAD_COUNT_SQL = 'SELECT COUNT(*) FROM dss_meta_loads'

    # save the info about the download
    # return the load id
    def save_download_info(downloaded_info)
      # generate load id
      load_id = Time.now.to_i

      # create the load table if it doesn't exist yet
      create_sql = get_create_sql(LOAD_INFO_TABLE_NAME, [{:name => 'Salesforce_Server'}])
      execute(create_sql)

      # check out if it's a new load
      count = execute_select(LOAD_COUNT_SQL, nil, true)
      @first_load = (count == 0)

      # insert it there
      insert_sql = get_insert_sql(
        sql_table_name(LOAD_INFO_TABLE_NAME),
        {
          "Salesforce_Server" => downloaded_info[:salesforce_server],
          "_LOAD_ID" => load_id
        }
      )
      execute(insert_sql)

      # save it for later
      @load_id = load_id

      return load_id
    end

    DIRNAME = "tmp"

    # extracts data to be filled in to datasets,
    # writes them to a csv file
    def extract_data(datasets)
      # create the directory if it doesn't exist
      Dir.mkdir(DIRNAME) if ! File.directory?(DIRNAME)

      # extract load info and put it my own params
      @params[:salesforce_downloaded_info] = get_load_info

      # extract each dataset from vertica
      datasets.each do |dataset, ds_structure|

        # if custom sql given
        if ds_structure["extract_sql"]
          # get the sql from the file
          sql = File.open(ds_structure["extract_sql"], 'rb') { |f| f.read }
          columns_gd = nil
        else
          # get the columns and generate the sql
          columns = get_columns(ds_structure)
          columns_gd = columns[:gd]
          sql = get_extract_sql(
            ds_structure["source_table"],
            columns[:sql]
          )
        end

        name = "tmp/#{dataset}-#{DateTime.now.to_i.to_s}.csv"

        # columns of the sql query result
        sql_columns = nil

        # open a file to write select results to it
        CSV.open(name, 'w', :force_quotes => true) do |csv|

          fetch_handler = lambda do |f|
            sql_columns = f.columns
            # write the columns to the csv file as a header
            csv << sql_columns
          end

          # execute the select and write row by row
          execute_select(sql, fetch_handler) do |row|
            row_array = sql_columns.map {|col| row[col]}
            csv << row_array
          end

          if columns_gd && (sql_columns != columns_gd.map {|c| c.to_sym})
            raise "something is weird, the columns of the sql '#{sql_columns}' aren't the same as the given cols '#{columns_gd}' "
          end
        end

        absolute_path = File.absolute_path(name)
        ds_structure["csv_filename"] = absolute_path
        @logger.info("Written results to file #{absolute_path}") if @logger
      end
      return datasets
    end

    def table_has_column(table, column)
      count = nil
      execute_select("SELECT COUNT(column_name) FROM columns WHERE table_name = '#{table}' and column_name = '#{column}'") do |row|

        count = row[:count]
      end
      return count > 0
    end

    # get columns to be part of the SELECT query .. only when sql needs to be generated
    def get_columns(ds_structure)
      columns_sql = []
      columns_gd = []

      if ds_structure["extract_sql"]
        raise "something is wrong, generating colums for sql when custom sql given"
      end

      columns = ds_structure["columns"]

      # go through all the fields of the dataset
      columns.each do |csv_column_name, s|
        # push the gd short_identifier to list of csv columns
        columns_gd.push(csv_column_name)

        # if it's optional and it's not in the table, return empty
        if s["optional"]
          source_column = s["source_column"]
          if ! source_column
            raise "source column must be given for optional: #{f}"
          end

          if ! table_has_column(ds_structure["source_table"], source_column)
            columns_sql.push("'' AS #{csv_column_name}")
            next
          end
        end

        if !s
          raise "no source given for field: #{f}"
        end

        # if column name given, push it there directly
        if s["source_column"]
          columns_sql.push("#{s['source_column']} AS #{csv_column_name}")
          next
        end

        # same if source_column_expression given
        if s["source_column_expression"]
          columns_sql.push("#{s['source_column_expression']} AS #{csv_column_name}")
          next
        end

        # if there's something to be evaluated, do it
        if s["source_column_concat"]
          # through the stuff to be concated
          concat_strings = s["source_column_concat"].map do |c|
            # if it's a symbol get it from the load params
            if c[0] == ":"
              "'#{@params[:salesforce_downloaded_info][c[1..-1].to_sym]}'"
            else
              # take the value as it is, including apostrophes if any
              c
            end
          end
          columns_sql.push("(#{concat_strings.join(' || ')}) AS #{csv_column_name}")
          next
        end
        raise "column or source_column_concat must be given for #{f}"
      end
      return {
        :sql => columns_sql,
        :gd => columns_gd
      }
    end

    def get_load_info
      # get information from the meta table latest row
      # return it in form source_column name -> value
      select_sql = get_extract_load_info_sql
      info = {}
      execute_select(select_sql) do |row|
        info.merge!(row)
      end
      return info
    end

    # connect and pass execution to a block
    def connect
      Sequel.connect @params["dss_jdbc_url"],
        :username => @params["dss_GDC_USERNAME"],
        :password => @params["dss_GDC_PASSWORD"] do |connection|
          yield(connection)
      end
    end

    # executes sql (select), for each row, passes execution to block
    def execute_select(sql, fetch_handler=nil, count=false)
      connect do |connection|
        # do the query
        f = connection.fetch(sql)

        @logger.info("Executing sql: #{sql}") if @logger
        # if handler was passed call it
        if fetch_handler
          fetch_handler.call(f)
        end

        if count
          return f.first[:count]
        end

        # go throug the rows returned and call the block
        return f.each do |row|
          yield(row)
        end
      end
    end

    # execute sql, return nothing
    def execute(sql_strings)
      if ! sql_strings.kind_of?(Array)
        sql_strings = [sql_strings]
      end
      connect do |connection|
          sql_strings.each do |sql|
            @logger.info("Executing sql: #{sql}") if @logger
            connection.run(sql)
          end
      end
    end

    private

    def sql_table_name(obj, historization=false)
      pr = @params["dss_table_prefix"]
      user_prefix = pr ? "#{pr}_" : ""
      hist_postfix = historization ? "_snapshot" : ""
      return "dss_#{user_prefix}#{obj}#{hist_postfix}"
    end

    def obj_name(sql_table)
      return sql_table[4..-1]
    end

    ID_COLUMN = {"_oid" => "IDENTITY PRIMARY KEY"}

    META_COLUMNS = [
      {"_HASH" => "VARCHAR(1023)"},
      {"_LOAD_ID" => "VARCHAR(255)"},
      {"_INSERTED_AT" => "TIMESTAMP NOT NULL DEFAULT now()"},
      {"_IS_DELETED" => "boolean NOT NULL DEFAULT FALSE"},
    ]

    HISTORIZATION_COLUMNS = [
      {"_SNAPSHOT_AT" => "TIMESTAMP"}
    ]

    TYPE_MAPPING = {
      "date" => "DATE",
      "datetime" => "TIMESTAMP",
      "string" => "VARCHAR(255)",
      "double" => "DOUBLE PRECISION",
      "int" => "INTEGER",
      # the vertica currency doesn't work well with parsing sfdc values
      "currency" => "DECIMAL",
      "boolean" => "BOOLEAN",
      "textarea" => "VARCHAR(32769)"
    }

    DEFAULT_TYPE = "VARCHAR(255)"

    def get_col_strings(col_list)
      return col_list.map {|col| "#{col.keys[0]} #{col.values[0]}"}.join(", ")
    end

    def get_create_sql(table, fields, historization=false)
      fields_string = fields.map{|f| "#{f[:name]} #{TYPE_MAPPING[f[:type]] || DEFAULT_TYPE}"}.join(", ")
      meta_cols = get_col_strings(META_COLUMNS)
      hist_cols = historization ? ", #{get_col_strings(HISTORIZATION_COLUMNS)}" : ""

      # id column isn't used for historization tables as merge to tables with id doesn't work.
      # it can be somehow faked using sequences
      id_col = historization ? "" : "#{ID_COLUMN.keys[0]} #{ID_COLUMN.values[0]},"

      return "CREATE TABLE IF NOT EXISTS #{sql_table_name(table, historization)}
      (#{id_col} #{fields_string}, #{meta_cols} #{hist_cols})"
    end

    def get_except_filename(filename)
      return "#{filename}.except.log"
    end

    def get_reject_filename(filename)
      return "#{filename}.reject.log"
    end

    HASH_LIMIT = 32

    # fields is a list of strings
    def get_hash_expression(fields)
      field_list = fields.join(', ')

      if fields.length <= HASH_LIMIT
        return "HASH(#{field_list})"
      end

      hashes = fields.each_slice(HASH_LIMIT).map{|a| "HASH(#{a.join(', ')})"}
      return hashes[1, hashes.length-1].reduce(hashes[0]) do |product, hsh|
        "CONCAT(#{hsh}, #{product})"
      end
    end

    # filename is absolute
    def get_upload_sql(table, fields, filename, load_id)
      field_string_list = fields.map {|f| f[:name]}
      field_list = field_string_list.join(', ')

      hash_exp = get_hash_expression(field_string_list)

      return %Q{COPY #{sql_table_name(table)} (#{field_list}, _LOAD_ID AS '#{load_id}', _HASH AS #{hash_exp})
      FROM LOCAL '#{filename}' WITH PARSER GdcCsvParser()
      ESCAPE AS '"'
       SKIP 1
      EXCEPTIONS '#{get_except_filename(filename)}'
      REJECTED DATA '#{get_reject_filename(filename)}' }
    end

    def get_extract_sql(table, columns)
      # TODO last snapshot
      return "SELECT #{columns.join(',')} FROM #{table} WHERE _LOAD_ID = (SELECT MAX(_LOAD_ID) FROM #{sql_table_name(LOAD_INFO_TABLE_NAME)})"
    end

    def get_extract_load_info_sql
      table_name = sql_table_name(LOAD_INFO_TABLE_NAME)
      return "SELECT * FROM #{table_name} WHERE _INSERTED_AT = (SELECT MAX(_INSERTED_AT) FROM #{table_name})"
    end

    def get_insert_sql(table, column_values)
      columns = column_values.keys
      values = column_values.values_at(*columns) + [get_hash_expression(columns)]
      columns.push("_HASH")
      values_string = values.map {|e| "'#{e}'"}.join(', ')

      return "INSERT INTO #{table} (#{columns.join(',')}) VALUES (#{values_string})"
    end
  end
end
