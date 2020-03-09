# DatabaseWrapper

[![][nuget-img]][nuget]

[nuget]:     https://www.nuget.org/packages/DatabaseWrapper/
[nuget-img]: https://badge.fury.io/nu/Object.svg

Simple database wrapper for Microsoft SQL Server, MySQL, PostgreSQL, and Sqlite written in C#, targeting both .NET Core and .NET Framework.

For a sample app exercising this library, refer to the test project contained within the solution.

## Description

DatabaseWrapper is a simple database wrapper for Microsoft SQL Server, MySQL, PostgreSQL, and Sqlite databases written in C#.   

Core features:

- Dynamic query building using expression objects
- Support for nested queries within expressions
- Support for SQL server native vs Windows authentication
- Support for SELECT, INSERT, UPDATE, DELETE, TRUNCATE, CREATE, DROP or raw queries
- Programmatic table creation and removal (drop)
- Built-in sanitization

## New in v2.0.0

- Support for Sqlite (.NET Framework 4.6.1 and Sqlite seems to have issues, but .NET Core works well)
  - For Microsoft SQL Server, MySQL, and PostgreSQL, use the original full constructors
  - For Sqlite, use the simplified constructor ```DatabaseClient(string filename)```
- Update dependencies (and update minimum .NET Framework required to .NET Framework 4.6.1)

## A Note on Sanitization

Use of parameterized queries vs building queries dynamically is a sensitive subject.  Proponents of parameterized queries have data on their side - that parameterization does the right thing to prevent SQL injection and other issues.  *I do not disagree with them*.  However, it is worth noting that with proper care, you CAN build systems that allow you to dynamically build queries, and you SHOULD do so as long as you build in the appropriate safeguards.

If you find an injection attack that will defeat the sanitization layer built into this project, please let me know!

## Simple Example

Refer to the test project for a more complete example with sample table setup scripts.
```
using DatabaseWrapper;
// SQL Server, MySQL, or PostgreSQL

DatabaseClient client = new DatabaseClient(DbTypes.MsSql, "localhost", 0, null, null, "SQLEXPRESS", "test");
// Sqlite
DatabaseClient client = new DatabaseClient("filename");

// some variables we'll be using
Dictionary<string, object> d;
Expression e;
List<string> fields;
DataTable result;

// add a record
d = new Dictionary<string, object>();
d.Add("firstName", "Joel");
d.Add("lastName", "Christner");
d.Add("notes", "Author");
result = client.Insert("person", d);

// update a record
d = new Dictionary<string, object>();
d.Add("notes", "The author :)");
e = new Expression("firstName", Operators.Equals, "Joel"); 
result = client.Update("person", d, e);

// retrieve 10 records
fields = new List<string> { "firstName", "lastName" }; // leave null for *
e = new Expression("lastName", Operators.Equals, "Christner"); 
result = client.Select("person", 0, 10, fields, e, "ORDER BY personId ASC");

// delete a record
e = new Expression("firstName", Operators.Equals, "Joel"); 
result = client.Delete("person", e);

// execute a raw query
result = client.RawQuery("SELECT customer_id FROM customer WHERE customer_id > 10");
```

## Sample Compound Expression

Expressions can be nested in either the LeftTerm or RightTerm.  Conversion from Expression to a WHERE clause uses recursion, so you should have a good degree of flexibility in building your expressions in terms of depth.
```
Expression e = new Expression {
	LeftTerm = new Expression("age", Operators.GreaterThan, 30),
	Operator = Operators.And,
	RightTerm = new Expression("height", Operators.LessThan, 74)
};
```

## Select with Pagination

Use indexStart, maxResults, and orderByClause to retrieve paginated results.  The query will retrieve maxResults records starting at row number indexStart using an ordering based on orderByClause.  See the example in the DatabaseWrapperTest project.

IMPORTANT: When doing pagination, you MUST specify an ```orderByClause```.
```
DataTable result = client.Select("person", 5, 10, null, e, "ORDER BY age DESC");
```

## Need a Timestamp?

We added a simple static method for this which you can use when building expressions (or elsewhere).  An object method exists as well.
```
string mssql1 = DatabaseClient.DbTimestamp(DbTypes.MsSql, DateTime.Now));
string mssql2 = client.Timestamp(DateTime.Now);
// 08/23/2016 05:34:32.4349034 PM

string mysql1 = DatabaseClient.DbTimestamp(DbTypes.MySql, DateTime.Now));
string mysql2 = client.Timestamp(DateTime.Now);
// 2016-08-23 17:34:32.446913 
```

## Other Notes

### MySQL

- MySQL does not like to return updated rows.  Sorry about that.  I thought about making the UPDATE clause require that you supply the ID field and the ID value so that I could retrieve it after the fact, but that approach is just too limiting.

### PostgreSQL

- Cleansing of strings in PostgreSQL uses the dollar-quote style.  Fieldnames are always encapsulated in double-quotes for PostgreSQL.

### Sqlite

- Sqlite seems to work well with .NET Core, but has image format exception issues with .NET Framework 4.6.1.  If anyone has a fix for this, please submit a PR!

## Version history

Refer to CHANGELOG.md.
