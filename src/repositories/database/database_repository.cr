require "db"

module Repositories
  module Database
    class DatabaseRepository
      def initialize(@database : DB::Database)
        @table = ""
        @fields = {
          :all => [] of String
        }
      end

      def select_one?(query, *args, as type : Class, field_set = :all)
        statement = build_select_statement(@table, @fields[field_set], query, limit: 1)
        @database.query_one?(statement, *args, as: type)
      end

      def select_many(query, *args, as type : Class, field_set = :all)
        statement = build_select_statement(@table, @fields[field_set], query)
        @database.query_all(statement, *args, as: type)
      end

      def exists?(query, *args)
        statement = build_select_statement(@table, [1], query)
        exists = @database.query_one?(statement, *args, &.read(Bool))
        exists || false
      end

      def insert(query, returning : String?=nil, as type : Class = Int32)
        statement = build_insert_statement(@table, query.keys.to_a, returning)
        if returning
          return @database.query(statement, *query.values, &.read(type))
        end
        @database.exec(statement, *query.values)
      end

      def update(set values, where query, *args, returning : String?=nil, as type : Class = Int32)
        statement = build_update_statement(@table, values.keys.to_a, query, returning)
        if returning
          return @database.query(statement, *{*values.values, *args}, &.read(type))
        end
        @database.exec(statement, *{*values.values, *args})
      end

      def update_model(id, query, id_field = "id")
        statement = String.build do |str|
          str << "UPDATE " << @table << " SET "
          params = query.keys.map_with_index(2) do |key, index|
            "#{key}=COALESCE($#{index}, #{key})"
          end
          str << params.join(",")
          str << " WHERE #{id_field}=$1"
        end
        @database.exec(statement, *{id, *query.values})
      end

      def delete(query, *args)
        statement = build_delete_statement(@table, query)
        @database.exec(statement, *args)
      end

      def build_select_statement(table, fields, query : String?=nil, order_by : String?=nil, limit : Int32?=nil)
        String.build do |str|
          str << "SELECT "
          str << fields.join(",")
          str << " FROM " << table
          if query
            str << " WHERE " << query
          end
          if order_by
            str << " ORDER BY " << order_by
          end
          if limit
            str << " LIMIT " << limit
          end
          str << ';'
        end
      end

      def build_insert_statement(table, keys, returning : String?=nil)
        String.build do |str|
          str << "INSERT INTO " << table
          str << '(' << keys.join(",") << ')'
          placeholders = keys.map_with_index(1) { |_, index| placeholder_of(index) }
          str << " VALUES (" << placeholders.join(",") << ')'
          if returning
            str << " RETURNING " << returning
          end
          str << ';'
        end
      end

      def build_update_statement(table, value_keys, query, returning : String?=nil)
        String.build do |str|
          str << "UPDATE " << table
          str << " SET " << get_assign_string value_keys
          str << " WHERE " << query
          if returning
            str << " RETURNING " << returning
          end
          str << ';'
        end
      end

      def build_delete_statement(table, query)
        String.build do |str|
          str << "DELETE FROM " << table
          str << " WHERE " << query
          str << ';'
        end
      end

      protected def placeholder_of(index)
        "$#{index}"
      end

      protected def get_assign_string(keys, offset=1)
        pairs = keys.map_with_index(offset) do |key, index|
          "#{key}=#{placeholder_of(index)}"
        end
        pairs.join(",")
      end
    end
  end
end
