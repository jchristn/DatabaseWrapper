# Change Log

## Current Version

v3.1.0.

- Breaking change: removal of .NET Framework and upgrade to .NET standard 2.1
- DatabaseSettings class
- New constructors

## Previous Versions

v3.0.1.2

- Migrate to Microsoft.Data.Sqlite for DatabaseWrapper.Sqlite

v3.0.1

- Support for 'Between' operator (thank you @twobytescy)

v3.0.0

- Major refactor, minor breaking changes
- Broke apart DatabaseWrapper into several database-specific projects; DatabaseWrapper itself can still target any

v2.0.5

- Added COLLATE NOCASE to Sqlite TEXT column creation

v2.0.4

- Truncate table fix

v2.0.3

- String names for serialized enum members

v2.0.2

- Fix Sqlite field name encapsulation character

v2.0.1

- Change Sqlite timestamp fields from NUMERIC to TEXT

v2.0.0

- Support for Sqlite (.NET Framework 4.6.1 and Sqlite seems to have issues, but .NET Core works well)
  - For Microsoft SQL Server, MySQL, and PostgreSQL, use the original full constructors
  - For Sqlite, use the simplified constructor ```DatabaseClient(string filename)```
- Update dependencies (and update minimum .NET Framework required to .NET Framework 4.6.1)

v1.5.1

- INSERT fix for MySQL

v1.5.0

- Breaking changes; improved logging setup

v1.4.7

- Fix bug with describing a table returning redundant rows

v1.4.5

- XML documentation

v1.4.x

- Expose ```Type``` property in ```DatabaseClient```
- Encapsulate table names in queries with the appropriate characters
  - Microsoft SQL: ``` [tablename] ```
  - MySQL: ``` `tablename` ```
  - PostgreSQL: ``` "tablename" ```
- Add support for CreateTable and DropTable operations, please note the following constraints:
  - For PostgreSQL, automatically uses ```SERIAL PRIMARY KEY``` for primary keys
  - For Microsoft SQL, automatically creates a constraint and assumes primary key type is ```int```
  - For Microsoft SQL, DateTime types are created as ```datetime2```
  - For MySQL, automatically applies ```AUTO_INCREMENT``` to primary keys
  - For MySQL, assumes ```Engine=InnoDB``` and ```AUTO_INCREMENT=1```
  - For a full list of supported data types and how they are cast, please refer to:
    - ```DataType.cs```, and 
    - ```DataTypeFromString``` method in ```DatabaseClient.cs```

v1.3.x

- Rework of MSSQL SELECT with pagination, now requires ORDER BY clause to be set (breaking change)
- Long-lived connections (rather than re-opening per query)
- IDisposable support

v1.2.x

- Retarget to support both .NET Core 2.0 and .NET Framework 4.5.2.
- Exposed SanitizeString through DatabaseClient
- New signatures for PrependAnd and PrependOr to make use easier
- PostgreSQL support
- Minor refactor

v1.1.x

- Added Trunate API
- Simplified (new) constructor for Expression
- Additional Helper static methods to convert DataTable to useful objects (List<Dictionary>, Dictionary, List<dynamic>, dynamic)
- Instance method to create timestamp for the given database type.
- Support for string for database type in timestamp and where clause builders
- New constructor using string for dbtype instead of enum
- Raw query support
- Pagination support in SELECT queries: use indexStart, maxResults, and orderByClause (all are required)
- Numerous bugfixes
