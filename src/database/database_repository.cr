require "db"

module Repositories
  module Database
    class DatabaseRepository
      def initialize(@database : DB::Database)
      end

      def get_placeholder(index)
        "$#{index}"
      end

      def select_one(from table, fields, where query, as type : Class)
        statement = build_select_statement(table, fields, query, limit: 1)
        args = get_final_values query
        @database.query_one? statement, *args, as: type
      end

      def select_many(from table, fields, where query, as type : Class)
        statement = build_select_statement(table, fields, query)
        args = get_final_values query
        @database.query_all statement, *args, as: type
      end

      def exists?(from table, where query, *args)
        statement = build_select_statement(table, ["true"], query, limit: 1)
        args = get_final_values query
        return_value = false
        exists = @database.query_one? statement, *args, &.read(Bool)
        exists || false
      end

      def insert(into table, query, returning : String?=nil, as type : Class = Int32)
        statement = build_insert_statement(table, query.keys, returning)
        return_value = nil
        @database.query statement, *query.values do |rs|
          if returning
            rs.each do
              return_value = rs.read(type)
            end
          end
        end
        return_value
      end

      def update(table, set values, where query, returning : String?=nil, as type : Class = Int32)
        statement = build_update_statement(table, values.keys, query, returning)
        return_value = nil
        @database.query statement, *query.values do |rs|
          if returning
            rs.each do
              return_value = rs.read(type)
            end
          end
        end
        return_value
      end

      def delete(from table, query)
        statement = build_delete_statement(table, query)
        @database.exec statement, *query.values
      end

      def build_select_statement(table, fields, query, limit : Int64?=nil)
        String.build do |io|
          io << "SELECT "
          fields.each_with_index do |field, i|
            sep = i < fields.size - 1 ? "," : ""
            io << field << sep
          end
          io << " FROM " << table
          if query
            io << " WHERE " << get_query_string query
          end
          if limit
            io << " LIMIT " << limit
          end
        end
      end

      def build_insert_statement(table, keys, returning : String?=nil)
        String.build do |io|
          io << "INSERT INTO " << table
          io << '(' << keys.join(",") << ')'
          placeholders = keys.map_with_index(1) { |_, index| get_placeholder(index) }
          io << " VALUES (" << placeholders.join(",") << ')'
          if returning
            io << " RETURNING " << returning
          end
        end
      end

      def build_update_statement(table, value_keys, query, returning : String?=nil)
        String.build do |io|
          io << "UPDATE " << table
          io << " SET " << get_assign_string value_keys
          io << " WHERE " << get_query_string(query, offset: value_keys.size + 1)
          if returning
            io << " RETURNING " << returning
          end
        end
      end

      def build_delete_statement(table, query)
        String.build do |io|
          io << "DELETE FROM " << table
          io << " WHERE " << get_query_string query
        end
      end

      def get_final_values(query)
        query.values.map &.value
      end

      private def get_assign_string(keys, offset=1)
        result = keys.map_with_index(offset) do |key, index|
          placeholder = get_placeholder(index)
          "#{key}=#{placeholder}"
        end
        result.join(",")
      end

      private def get_query_string(query, offset=1)
        index = offset
        result = query.map do |key, func|
          indexes = index.step(to: index + func.size - 1).to_a
          placeholders = indexes.map { |index| get_placeholder(index) }
          index += indexes.size + 1
          func.to_sql(key, placeholders)
        end
        result.join(",")
      end
    end
  end
end