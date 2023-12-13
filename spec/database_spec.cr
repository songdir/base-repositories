require "./spec_helper"

module DB
  class Database
    def initialize(@connection_options=Connection::Options.new)
      @setup_connection = ->(conn : Connection) {}
      @pool = uninitialized Pool(Connection)
    end

    def query_one?(query, *args, as type : Class)
      if type == Int32
        1
      elsif type == Float32
        1.0
      elsif type == Bool
        false
      elsif type == Char
        'a'
      else
        "result of #{args[0]}"
      end
    end

    def query_one?(query, *args, &block)
      nil
    end

    def query_all(query, *args, as type : Class)
      if type == Int32
        [1, 2]
      elsif type == Float32
        [1.0, 2.0]
      elsif type == Bool
        [true, false]
      elsif type == Char
        ['a', 'b']
      else
        ["A", "B"]
      end
    end

    def query(query, *args, &block)
    end

    def exec(query, *args)
    end
  end
end

include Repositories::Database

db = DB::Database.new

describe DatabaseRepository do
  describe "#select_one?" do
    it "must retrieve one result from the database" do
      repo = DatabaseRepository.new(db)
      username = "john"
      result = repo.select_one?("username=$1", username, as: String)
      result.should be_a String?
      result.should eq "result of #{username}"
    end
  end

  describe "#query_all" do
    it "must retrieve an array of strings" do
      repo = DatabaseRepository.new(db)
      result = repo.select_many("admin=$1", true, as: String)
      result.should be_a Array(String)
      result.should eq ["A", "B"]
    end

    it "must retrieve an array of ints" do
      repo = DatabaseRepository.new(db)
      result = repo.select_many("admin=$1", true, as: Int32)
      result.should be_a Array(Int32)
      result.should eq [1, 2]
    end
  end

  describe "#exists?" do
    it "must retrieve false" do
      repo = DatabaseRepository.new(db)
      result = repo.exists?("username=$1", "carl")
      result.should be_a Bool
      result.should be_false
    end
  end

  describe "#build_select_statement" do
    it "must create a basic select statement" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("albums", ["name", "year"])
      statement.should eq "SELECT name,year FROM albums"
    end

    it "must create a select statement with WHERE" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("payments", ["reference"], "status='active'")
      statement.should eq "SELECT reference FROM payments WHERE status='active'"
    end

    it "must create a select statement with IN operator" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("payments", ["reference"], "id IN (123, 234)")
      statement.should eq "SELECT reference FROM payments WHERE id IN (123, 234)"
    end

    it "must create a select statement with LIMIT" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("payments", ["reference"], limit: 3)
      statement.should eq "SELECT reference FROM payments LIMIT 3"
    end
  end

  describe "#build_insert_statement" do
    it "must create a basic insert statement" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_insert_statement("properties", {"number", "name"})
      statement.should eq "INSERT INTO properties(number,name) VALUES ($1,$2)"
    end

    it "must create an insert statement with RETURNING" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_insert_statement("properties", {"number", "name"}, returning: "id")
      statement.should eq "INSERT INTO properties(number,name) VALUES ($1,$2) RETURNING id"
    end
  end

  describe "#build_update_statement" do
    it "must create a basic update statement" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_update_statement("items", {"name", "value"}, "id=100")
      statement.should eq "UPDATE items SET name=$1,value=$2 WHERE id=100"
    end

    it "must create an update statement with RETURNING" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_update_statement("items", {"name", "value"}, "id=100", returning: "id")
      statement.should eq "UPDATE items SET name=$1,value=$2 WHERE id=100 RETURNING id"
    end
  end

  describe "#build_delete_statement" do
    it "must create a basic delete statement" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_delete_statement("engines", "type=$1")
      statement.should eq "DELETE FROM engines WHERE type=$1"
    end

    it "must create a basic delete statement with comparison" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_delete_statement("engines", "power < 100")
      statement.should eq "DELETE FROM engines WHERE power < 100"
    end
  end
end
