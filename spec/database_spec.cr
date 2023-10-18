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
  describe "#select_one" do
    it "must retrieve one result from the database" do
      repo = DatabaseRepository.new(db)
      result = repo.select_one? "users", ["email"], where: {username: eq? "john"}, as: String
      result.should be_a String?
      result.should eq "result of john"
    end
  end

  describe "#query_all" do
    it "must retrieve an array of strings" do
      repo = DatabaseRepository.new(db)
      result = repo.select_many "users", ["email"], where: {admin: eq? true}, as: String
      result.should be_a Array(String)
      result.should eq ["A", "B"]
    end

    it "must retrieve an array of ints" do
      repo = DatabaseRepository.new(db)
      result = repo.select_many "users", ["level"], where: {admin: eq? true}, as: Int32
      result.should be_a Array(Int32)
      result.should eq [1, 2]
    end
  end

  describe "#exists?" do
    it "must retrieve false" do
      repo = DatabaseRepository.new(db)
      result = repo.exists? "users", where: {username: eq? "john"}
      result.should be_a Bool
      result.should be_false
    end
  end

  describe "#build_select_statement" do
    it "must create a basic select statement" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("albums", ["name", "year"], nil)
      statement.should eq "SELECT name,year FROM albums"
    end

    it "must create a select statement with WHERE" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("payments", ["reference"], {status: eq? "active"})
      statement.should eq "SELECT reference FROM payments WHERE status = $1"
    end

    it "must create a select statement with IN operator" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("payments", ["reference"], {id: in? ["123", "234"]})
      statement.should eq "SELECT reference FROM payments WHERE id IN ($1,$2)"
    end

    it "must create a select statement with LIMIT" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_select_statement("payments", ["reference"], nil, limit: 3)
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
      statement = repo.build_update_statement("items", {"name", "value"}, {id: eq? 100})
      statement.should eq "UPDATE items SET name=$1,value=$2 WHERE id = $3"
    end

    it "must create an update statement with RETURNING" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_update_statement("items", {"name", "value"}, {id: eq? 100}, returning: "id")
      statement.should eq "UPDATE items SET name=$1,value=$2 WHERE id = $3 RETURNING id"
    end
  end

  describe "#build_delete_statement" do
    it "must create a basic delete statement" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_delete_statement("engines", {type: eq? "ABC"})
      statement.should eq "DELETE FROM engines WHERE type = $1"
    end

    it "must create a basic delete statement with comparison" do
      repo = DatabaseRepository.new(db)
      statement = repo.build_delete_statement("engines", {power: lt? 100})
      statement.should eq "DELETE FROM engines WHERE power < $1"
    end
  end

  describe "#get_final_values" do
    it "must return a tuple with expected values" do
      repo = DatabaseRepository.new(db)
      values = repo.get_final_values({
        in: In.new([1, 2]),
        gt: Gt.new(1),
        lt: Lt.new(30),
        gte: Gte.new(20),
        lte: Lte.new(10.5),
        eq: Eq.new(true),
        neq: Neq.new('b')
      })
      values.should eq({[1, 2], 1, 30, 20, 10.5, true, 'b'})
    end
  end
end
