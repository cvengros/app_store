# encoding: utf-8
require 'rinruby'
require 'gooddata'
require 'gooddata_datawarehouse'
require 'tempfile'

module GoodData::Bricks
  class RBrick < GoodData::Bricks::Brick
    def version
      "0.0.1"
    end

    def get_dwh(ads_username, ads_password, ads_id)
      return @dwh if @dwh
      if ads_username.nil? || ads_password.nil? || ads_id.nil?
        fail "You need to provide ADS_USERNAME, ADS_PASSWORD, ADS_ID. Some of them are nil."
      end
      @dwh = GoodData::Datawarehouse.new(ads_username, ads_password, ads_id)
    end

    def create_temp_file(table_name)
      # create a new tempfile and make it last
      # f = Tempfile.new("#{table_name}.csv")
      # ObjectSpace.undefine_finalizer(f)
      # f
      Dir::Tmpname.create("#{table_name}.csv") { |path| path }
    end

    def call(params)
      # get params from params
      config = params['config']
      r_script_dir = config['R_SCRIPT_DIR'] or fail 'R_SCRIPT_DIR is empty. You need to provide path where the R script is'
      r_script_filename = config['R_SCRIPT_FILENAME'] or fail 'R_SCRIPT_FILENAME is empty. You need to provide the filename of the R script'
      output_tables = config['OUTPUT_TABLES']
      input_tables = config['INPUT_TABLES']
      ads_id = config['ADS_ID']
      ads_username = config['ADS_USERNAME']
      ads_password = config['ADS_PASSWORD']

      # if the files are at s3, get them
      if /s3n:/ =~ r_script_dir
        # download it from there
        # r_script_dir = where it was downloaded
      end

      # if input tables given export stuff from ADS to CSV
      unless input_tables.nil? || input_tables.empty?
        unless input_tables.is_a?(Array)
          input_tables = [input_tables]
        end
        dwh = get_dwh(ads_username, ads_password, ads_id)
        input_filenames = []
        # export the tables to csvs
        input_tables.each do |table_name|
          f = create_temp_file(table_name)

          dwh.export_table(table_name, f)
          input_filenames << f
        end
        # pass it to the script
        R.input_filenames = input_filenames
      end

      # if output tables given, create temporary files for them
      unless output_tables.nil? || output_tables.empty?
        unless output_tables.is_a?(Array)
          output_tables = [output_tables]
        end
        output_filenames = []
        output_tables.each do |table_name|
          f = create_temp_file(table_name)
          output_filenames << f
        end

        # pass it to the script
        R.output_filenames = output_filenames
      end

      # run the script
      R.eval(File.read(File.join(r_script_dir, r_script_filename)))
      # if output_tables given load it to ads
      unless output_tables.nil? || output_tables.empty?
        dwh = get_dwh(ads_username, ads_password, ads_id)

        # for each output file given find the table where it should go and load it there
        output_filenames.each do |f|
          table = output_tables.select{|t| f.include?(t)}
          if table.size != 1
            puts "WARNING: No output table for output file #{f}, or too many output tables: #{table}"
            next
          end
          dwh.load_data_from_csv(table[0], f)
        end
      end
      params
    end
  end
end