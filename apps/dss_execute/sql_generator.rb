module GoodData::Bricks
  class SQLGenerator
    class << self

      def insert_load(column_values)
        columns = column_values.keys
        values = column_values.values_at(*columns) + [hash_expression(columns)]
        columns.push("_HASH")
        values_string = values.map {|e| "'#{e}'"}.join(', ')

        return "INSERT INTO #{sql_table_name(LOAD_INFO_TABLE_NAME)} (#{columns.join(',')}) VALUES (#{values_string})"
      end

      def extract(table, columns)
        # TODO last snapshot
        return "SELECT #{columns.join(', ')} FROM #{table}"
      end

      def create_last_snapshot_view(object, fields)
        field_names = fields.map {|f| f[:name]}.join(', ')
        return "CREATE OR REPLACE VIEW #{sql_view_name(object, :last_snapshot => true)} AS SELECT #{field_names}, _LOAD_ID, _INSERTED_AT FROM #{sql_table_name(object)} WHERE _LOAD_ID = (SELECT MAX(_LOAD_ID) FROM #{sql_table_name(LOAD_INFO_TABLE_NAME)})"
      end

      def extract_load_info
        table_name = sql_table_name(LOAD_INFO_TABLE_NAME)
        return "SELECT * FROM #{table_name} WHERE _INSERTED_AT = (SELECT MAX(_INSERTED_AT) FROM #{table_name})"
      end

      # filename is absolute
      def upload(table, fields, filename, load_id)
        field_string_list = fields.map {|f| f[:name]}
        field_list = field_string_list.join(', ')

        hash_exp = hash_expression(field_string_list)

        return %Q{COPY #{sql_table_name(table)} (#{field_list}, _LOAD_ID AS '#{load_id}', _HASH AS #{hash_exp})
        FROM LOCAL '#{filename}' WITH PARSER GdcCsvParser()
        ESCAPE AS '"'
         SKIP 1
        EXCEPTIONS '#{except_filename(filename)}'
        REJECTED DATA '#{reject_filename(filename)}' }
      end

      def history_loading(object, load_history_from_params, object_fields, load_id)
        if !load_id
          raise "load the data first! load_id is empty"
        end
        snapshot_table_name = sql_table_name(object, true)
        history_table_name = sql_table_name(load_history_from_params["name"])
        dest_fields = load_history_from_params["column_mapping"].keys
        src_fields = load_history_from_params["column_mapping"].values

        all_fields = object_fields.map {|o| o[:name]}

        common_fields = (Set.new(all_fields) - Set.new(dest_fields) - Set.new(src_fields)).to_a

        dest_meta = ["_LOAD_ID", "_HASH"]
        src_meta = [load_id, hash_expression(src_fields + common_fields)]

        field_list_dest = (dest_fields + common_fields + dest_meta).join(", ")
        field_list_src = (src_fields + common_fields + src_meta).join(", ")

        sql = "INSERT INTO #{snapshot_table_name} (#{field_list_dest})\n"
        sql += "SELECT #{field_list_src} \n"

        sql += "FROM #{history_table_name} WHERE _LOAD_ID = (SELECT MAX(_LOAD_ID) FROM #{sql_table_name(LOAD_INFO_TABLE_NAME)})"
        return sql
      end

      def create_loads
        return create(LOAD_INFO_TABLE_NAME, [{:name => 'Salesforce_Server'}])
      end

      def load_count
        return 'SELECT COUNT(*) FROM dss_meta_loads'
      end

      def create(table, fields, historization=false)
        fields_string = fields.map{|f| "#{f[:name]} #{TYPE_MAPPING[f[:type]] || DEFAULT_TYPE}"}.join(", ")
        meta_cols = col_strings(META_COLUMNS)
        hist_cols = historization ? ", #{col_strings(HISTORIZATION_COLUMNS)}" : ""

        # id column isn't used for historization tables as merge to tables with id doesn't work.
        # it can be somehow faked using sequences
        id_col = historization ? "" : "#{ID_COLUMN.keys[0]} #{ID_COLUMN.values[0]},"

        return "CREATE TABLE IF NOT EXISTS #{sql_table_name(table, historization)}
        (#{id_col} #{fields_string}, #{meta_cols} #{hist_cols})"
      end

      def column_count(table, column)
        "SELECT COUNT(column_name) FROM columns WHERE table_name = '#{table}' and column_name = '#{column}'"
      end

      def delete_but_last_load(object)
        table_name = sql_table_name(object)
        return "DELETE FROM #{table_name} WHERE _LOAD_ID <> (SELECT MAX(_LOAD_ID) FROM #{sql_table_name(LOAD_INFO_TABLE_NAME)})"
      end

      def historization_merge(object, merge_from_params, object_fields)

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

      def except_filename(filename)
        return "#{filename}.except.log"
      end

      def reject_filename(filename)
        return "#{filename}.reject.log"
      end

      private

      LOAD_INFO_TABLE_NAME = 'meta_loads'

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

      def col_strings(col_list)
        return col_list.map {|col| "#{col.keys[0]} #{col.values[0]}"}.join(", ")
      end

      HASH_LIMIT = 32

      # fields is a list of strings
      def hash_expression(fields)
        field_list = fields.join(', ')

        if fields.length <= HASH_LIMIT
          return "HASH(#{field_list})"
        end

        hashes = fields.each_slice(HASH_LIMIT).map{|a| "HASH(#{a.join(', ')})"}
        return hashes[1, hashes.length-1].reduce(hashes[0]) do |product, hsh|
          "CONCAT(#{hsh}, #{product})"
        end
      end

      def sql_table_name(obj, historization=false)
        hist_postfix = historization ? "_snapshot" : ""
        return "dss_#{obj}#{hist_postfix}"
      end

      def sql_view_name(obj, options={})
        last_snapshot_postfix = options[:last_snapshot] ? "_last_snapshot": ''
        return "dss_#{obj}#{last_snapshot_postfix}"
      end

      def obj_name(sql_table)
        return sql_table[4..-1]
      end
    end
  end
end