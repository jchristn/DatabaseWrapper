
## ⚠️ NOTICE ⚠️

**Efforts on this repository have been frozen in favor of [Durable](https://github.com/jchristn/durable). This repository has been made into a public archive.**

# DatabaseWrapper

| Library | Version | Downloads |
|---|---|---|
| DatabaseWrapper (all supported database types) | [![NuGet Version](https://img.shields.io/nuget/v/DatabaseWrapper.svg?style=flat)](https://www.nuget.org/packages/DatabaseWrapper/)  | [![NuGet](https://img.shields.io/nuget/dt/DatabaseWrapper.svg)](https://www.nuget.org/packages/DatabaseWrapper) |
| DatabaseWrapper.Mysql | [![NuGet Version](https://img.shields.io/nuget/v/DatabaseWrapper.Mysql.svg?style=flat)](https://www.nuget.org/packages/DatabaseWrapper.Mysql/)  | [![NuGet](https://img.shields.io/nuget/dt/DatabaseWrapper.Mysql.svg)](https://www.nuget.org/packages/DatabaseWrapper.Mysql) |
| DatabaseWrapper.Postgresql | [![NuGet Version](https://img.shields.io/nuget/v/DatabaseWrapper.Postgresql.svg?style=flat)](https://www.nuget.org/packages/DatabaseWrapper.Postgresql/)  | [![NuGet](https://img.shields.io/nuget/dt/DatabaseWrapper.Postgresql.svg)](https://www.nuget.org/packages/DatabaseWrapper.Postgresql) |
| DatabaseWrapper.Sqlite | [![NuGet Version](https://img.shields.io/nuget/v/DatabaseWrapper.Sqlite.svg?style=flat)](https://www.nuget.org/packages/DatabaseWrapper.Sqlite/)  | [![NuGet](https://img.shields.io/nuget/dt/DatabaseWrapper.Sqlite.svg)](https://www.nuget.org/packages/DatabaseWrapper.Sqlite) |
| DatabaseWrapper.SqlServer | [![NuGet Version](https://img.shields.io/nuget/v/DatabaseWrapper.SqlServer.svg?style=flat)](https://www.nuget.org/packages/DatabaseWrapper.SqlServer/)  | [![NuGet](https://img.shields.io/nuget/dt/DatabaseWrapper.SqlServer.svg)](https://www.nuget.org/packages/DatabaseWrapper.SqlServer) |
| DatabaseWrapper.Core | [![NuGet Version](https://img.shields.io/nuget/v/DatabaseWrapper.Core.svg?style=flat)](https://www.nuget.org/packages/DatabaseWrapper.Core/)  | [![NuGet](https://img.shields.io/nuget/dt/DatabaseWrapper.Core.svg)](https://www.nuget.org/packages/DatabaseWrapper.Core) |
| ExpressionTree | [![NuGet Version](https://img.shields.io/nuget/v/ExpressionTree.svg?style=flat)](https://www.nuget.org/packages/ExpressionTree/)  | [![NuGet](https://img.shields.io/nuget/dt/ExpressionTree.svg)](https://www.nuget.org/packages/ExpressionTree) |

DatabaseWrapper is the EASIEST and FASTEST way to get a data-driven application up and running using SQL Server, MySQL, PostgreSQL, or Sqlite.

For a sample app exercising this library, refer to the numerous ```Test``` projects contained within the solution.

Core features:

- Dynamic query building
- Hierarchical Boolean logic using Expression objects
- Support for SQL server native vs Windows authentication
- Support for SELECT, INSERT, UPDATE, DELETE, TRUNCATE, CREATE, DROP or raw queries
- Programmatic table creation and removal (drop)
- Built-in sanitization
- Support for .NET Standard, .NET Core, and .NET Framework
- Support for SQL Server, Sqlite, PostgreSQL, MySQL, MariaDB, Oracle, both on-premises and in the cloud
- Both synchronous and asynchronous APIs

## New in v6.x

- Oracle support (thank you @Skimmenthal13)
- Asynchronous APIs
- Updated dependencies
- Bugfixes
- Minor breaking changes (API name changes)

## Special Thanks

Special thanks to those who have helped contribute or otherwise improve the library!  

@shawty @constantinje @thedarklort @l-404-l @igrgurina @Vaelek @treyhay31 @daoye @twobytescy @rinkusahu1 @Skimmenthal13

## A Note on Sanitization

Use of parameterized queries vs building queries dynamically is a sensitive subject.  Proponents of parameterized queries have data on their side - that parameterization does the right thing to prevent SQL injection and other issues.  *I do not disagree with them*.  However, it is worth noting that with proper care, you CAN build systems that allow you to dynamically build queries, and you SHOULD do so as long as you build in the appropriate safeguards.

If you find an injection attack that will defeat the sanitization layer built into this project, please let me know!

## Simple Example

Refer to the test project for a more complete example with sample table setup scripts.
```csharp
using DatabaseWrapper;
using DatabaseWrapper.Core;
using ExpressionTree;

DatabaseClient client = null;

// Sqlite
client = new DatabaseClient("[databasefilename]");

// SQL Server, MySQL, or PostgreSQL
client = new DatabaseClient(DbTypes.SqlServer,  "[hostname]", [port], "[user]", "[password]", "[databasename]");
client = new DatabaseClient(DbTypes.Mysql,      "[hostname]", [port], "[user]", "[password]", "[databasename]");
client = new DatabaseClient(DbTypes.Postgresql, "[hostname]", [port], "[user]", "[password]", "[databasename]");

// SQL Express
client = new DatabaseClient(DbTypes.SqlServer,  "[hostname]", [port], "[user]", "[password]", "[instance]", "[databasename]");

// some variables we'll be using
Dictionary<string, object> d;
Expr e;
List<string> fields;
DataTable result;

// add a record
d = new Dictionary<string, object>();
d.Add("firstName", "Joel");
d.Add("lastName", "Christner");
d.Add("notes", "Author");
result = client.Insert("person", d);
result = await client.InsertAsync("person", d);

// update a record
d = new Dictionary<string, object>();
d.Add("notes", "The author :)");
e = new Expr("firstName", OperatorEnum.Equals, "Joel"); 
client.Update("person", d, e);
await client.UpdateAsync("person", d, e);

// retrieve 10 records
fields = new List<string> { "firstName", "lastName" }; // leave null for *
e = new Expr("lastName", OperatorEnum.Equals, "Christner"); 
ResultOrder[] order = new ResultOrder[1];
order = new ResultOrder("firstName", OrderDirectionEnum.Ascending)
result = client.Select("person", 0, 10, fields, e, order);
result = await client.SelectAsync("person", 0, 10, fields, e, order);

// delete a record
e = new Expr("firstName", Operators.Equals, "Joel"); 
client.Delete("person", e);
await client.DeleteAsync("person", e);

// execute a raw query
result = client.Query("SELECT customer_id FROM customer WHERE customer_id > 10");
result = await client.QueryAsync("SELECT customer_id FROM customer WHERE customer_id > 10");
```

## Sample Compound Expression

Expressions, i.e. the ```Expr``` class from ```ExpressionTree```, can be nested in either the ```Left``` or ```Right``` properties.  Conversion from ```Expr``` to a WHERE clause uses recursion, so you have a good degree of flexibility in building your expressions in terms of depth.

```csharp
Expr e = new Expr {
	Left = new Expr("age", OperatorEnum.GreaterThan, 30),
	Operator = Operators.And,
	Right = new Expr("height", OperatorEnum.LessThan, 74)
};
```

## Select with Pagination

Use ```IndexStart```, ```MaxResults```, and ```ResultOrder[]``` to retrieve paginated results.  The query will retrieve maxResults records starting at row number indexStart using an ordering based on orderByClause.  See the example in the DatabaseWrapperTest project.

IMPORTANT: When doing pagination with SQL Server, you MUST specify an ```ResultOrder[]```.

```csharp
ResultOrder[] order = new ResultOrder[1];
order = new ResultOrder("firstName", OrderDirectionEnum.Ascending);
DataTable result = client.Select("person", 5, 10, null, e, order);
```

## Need a Timestamp?

We added a simple static method for this which you can use when building expressions (or elsewhere).  An object method exists as well.

```csharp
string ts = client.Timestamp(DateTime.Now);
// 08/23/2016 05:34:32.4349034 PM

string tso = client.Timestamp(DateTime.Now);
// 2016-08-23 17:34:32.446913 
```

## Other Notes

### General

When using database-specific classes ```DatabaseWrapper.Mysql```, ```DatabaseWrapper.Postgresql```, ```DatabaseWrapper.SqlServer```, or ```DatabaseWrapper.Sqlite```, the constructor is simplified from what is shown above.

For Sqlite:

```csharp
DatabaseClient client = new DatabaseClient("[databasefilename]");
```

For SQL Server, MySQL, or PostgreSQL:

```csharp
DatabaseClient client = new DatabaseClient(DbTypes.SqlServer,  "[hostname]", [port], "[user]", "[password]", "[databasename]");
DatabaseClient client = new DatabaseClient(DbTypes.Mysql,      "[hostname]", [port], "[user]", "[password]", "[databasename]");
DatabaseClient client = new DatabaseClient(DbTypes.Postgresql, "[hostname]", [port], "[user]", "[password]", "[databasename]");
```

For SQL Server Express:

```csharp
DatabaseClient client = new DatabaseClient(DbTypes.SqlServer, "[hostname]", [port], "[user]", "[password]", "[instance]", "[databasename]");
```

### MySQL

- MySQL does not like to return updated rows.  I thought about making the UPDATE clause require that you supply the ID field and the ID value so that I could retrieve it after the fact, but that approach is just too limiting.

### MariaDB

- Use the MySQL constructor.  MySQL constraints remain.

### PostgreSQL

- Cleansing of strings in PostgreSQL uses the dollar-quote style.  Fieldnames are always encapsulated in double-quotes for PostgreSQL.

### SQL Server

- Pagination where ```IndexStart``` and ```MaxResults``` are supplied demands use of ```ResultOrder[]```.

### Sqlite

- Sqlite may not work out of the box with .NET Framework.  In order to use Sqlite with .NET Framework, you'll need to manually copy the ```runtimes``` folder into your project output directory.  This directory is automatically created when building for .NET Core.  To get this folder, build the ```Test.Sqlite``` project and navigate to the ```bin/debug/netcoreapp*``` directory.  Then copy the ```runtimes``` folder into the project output directory of your .NET Framework application. 

## Version history

Refer to CHANGELOG.md.
