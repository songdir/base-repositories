module Repositories
  module Database
    abstract struct SQLFunc(T)
      def initialize(@value : T)
      end

      getter value

      def size
        {% if T <= Array %}
          @value.size
        {% else %}
          1
        {% end %}
      end

      def to_sql(key, overwrite=nil)
        case overwrite
        when Array
          value = overwrite.join(",")
        when String
          value = overwrite
        when nil
          value = @value.to_s
        else
          value = overwrite.to_s
        end
        get_expression(key, value)
      end
    end

    struct In(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} IN (#{value})"
      end
    end

    struct Gt(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} > #{value}"
      end
    end

    struct Lt(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} < #{value}"
      end
    end

    struct Gte(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} >= #{value}"
      end
    end

    struct Lte(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} <= #{value}"
      end
    end

    struct Eq(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} = #{value}"
      end
    end

    struct Neq(T) < SQLFunc(T)
      def get_expression(key, value)
        "#{key} <> #{value}"
      end
    end

    def in?(value)
      In.new(value)
    end

    def gt?(value)
      Gt.new(value)
    end

    def lt?(value)
      Lt.new(value)
    end

    def gte?(value)
      Gte.new(value)
    end

    def lte?(value)
      Lte.new(value)
    end

    def eq?(value)
      Eq.new(value)
    end

    def neq?(value)
      Neq.new(value)
    end
  end
end