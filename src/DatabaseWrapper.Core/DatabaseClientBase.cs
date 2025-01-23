namespace DatabaseWrapper.Core
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Data;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DatabaseWrapper.Core;
    using ExpressionTree;
    using System.Threading;

    /// <summary>
    /// Database client base.
    /// </summary>
    public abstract class DatabaseClientBase
    {
        #region Public-Members

        /// <summary>
        /// The connection string used to connect to the database.
        /// </summary>
        public string ConnectionString;

        /// <summary>
        /// Timestamp format.
        /// </summary>
        public string TimestampFormat;

        /// <summary>
        /// Timestamp format with offset.
        /// </summary>
        public string TimestampOffsetFormat;

        /// <summary>
        /// Maximum supported statement length.
        /// </summary>
        public int MaxStatementLength;

        /// <summary>
        /// Database settings.
        /// </summary>
        public DatabaseSettings Settings;

        /// <summary>
        /// Event to fire when a query is handled.
        /// </summary>
        public EventHandler<DatabaseQueryEvent> QueryEvent;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// List all tables in the database.
        /// </summary>
        /// <returns>List of strings, each being a table name.</returns>
        public abstract List<string> ListTables();

        /// <summary>
        /// List all tables in the database.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>List of strings, each being a table name.</returns>
        public abstract Task<List<string>> ListTablesAsync(CancellationToken token = default);

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if exists.</returns>
        public abstract bool TableExists(string tableName);

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True if exists.</returns>
        public abstract Task<bool> TableExistsAsync(string tableName, CancellationToken token = default);

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <returns>A list of column objects.</returns>
        public abstract List<Column> DescribeTable(string tableName);

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A list of column objects.</returns>
        public abstract Task<List<Column>> DescribeTableAsync(string tableName, CancellationToken token = default);

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <returns>Dictionary where Key is table name, value is List of Column objects.</returns>
        public abstract Dictionary<string, List<Column>> DescribeDatabase();

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Dictionary where Key is table name, value is List of Column objects.</returns>
        public abstract Task<Dictionary<string, List<Column>>> DescribeDatabaseAsync(CancellationToken token = default);

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        public abstract void CreateTable(string tableName, List<Column> columns);

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="columns">Columns.</param>
        public abstract Task CreateTableAsync(string tableName, List<Column> columns, CancellationToken token = default);

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        public abstract void DropTable(string tableName);

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        /// <param name="token">Cancellation token.</param>
        public abstract Task DropTableAsync(string tableName, CancellationToken token = default);

        /// <summary>
        /// Retrieve the name of the primary key column from a specific table.
        /// </summary>
        /// <param name="tableName">The table of which you want the primary key.</param>
        /// <returns>A string containing the column name.</returns>
        public abstract string GetPrimaryKeyColumn(string tableName);

        /// <summary>
        /// Retrieve a list of the names of columns from within a specific table.
        /// </summary>
        /// <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
        /// <returns>A list of strings containing the column names.</returns>
        public abstract List<string> GetColumnNames(string tableName);

        /// <summary>
        /// Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="columnName">The column containing key or unique fields where a match is desired.</param>
        /// <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
        /// <returns>A DataTable containing at most one row.</returns>
        public abstract DataTable GetUniqueObjectById(string tableName, string columnName, object value);

        /// <summary>
        /// Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="columnName">The column containing key or unique fields where a match is desired.</param>
        /// <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing at most one row.</returns>
        public abstract Task<DataTable> GetUniqueObjectByIdAsync(string tableName, string columnName, object value, CancellationToken token = default);

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter);

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract Task<DataTable> SelectAsync(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, CancellationToken token = default);

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <param name="resultOrder">Specify on which columns and in which direction results should be ordered.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder);

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <param name="resultOrder">Specify on which columns and in which direction results should be ordered.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract Task<DataTable> SelectAsync(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder, CancellationToken token = default);

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs);

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract Task<DataTable> InsertAsync(string tableName, Dictionary<string, object> keyValuePairs, CancellationToken token = default);

        /// <summary>
        /// Execute an INSERT query with multiple values within a transaction.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        public abstract void InsertMultiple(string tableName, List<Dictionary<string, object>> keyValuePairList);

        /// <summary>
        /// Execute an INSERT query with multiple values within a transaction.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <param name="token">Cancellation token.</param>
        public abstract Task InsertMultipleAsync(string tableName, List<Dictionary<string, object>> keyValuePairList, CancellationToken token = default);

        /// <summary>
        /// Execute an UPDATE query. 
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
        public abstract void Update(string tableName, Dictionary<string, object> keyValuePairs, Expr filter);

        /// <summary>
        /// Execute an UPDATE query. 
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
        /// <param name="token">Cancellation token.</param>
        public abstract Task UpdateAsync(string tableName, Dictionary<string, object> keyValuePairs, Expr filter, CancellationToken token = default);

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        public abstract void Delete(string tableName, Expr filter);

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        /// <param name="token">Cancellation token.</param>
        public abstract Task DeleteAsync(string tableName, Expr filter, CancellationToken token = default);

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        public abstract void Truncate(string tableName);

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        /// <param name="token">Cancellation token.</param>
        public abstract Task TruncateAsync(string tableName, CancellationToken token = default);

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract DataTable Query(string query);

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing the results.</returns>
        public abstract Task<DataTable> QueryAsync(string query, CancellationToken token = default);

        /// <summary>
        /// Determine if records exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>True if records exist.</returns>
        public abstract bool Exists(string tableName, Expr filter);

        /// <summary>
        /// Determine if records exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True if records exist.</returns>
        public abstract Task<bool> ExistsAsync(string tableName, Expr filter, CancellationToken token = default);

        /// <summary>
        /// Determine the number of records that exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The number of records.</returns>
        public abstract long Count(string tableName, Expr filter);

        /// <summary>
        /// Determine the number of records that exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of records.</returns>
        public abstract Task<long> CountAsync(string tableName, Expr filter, CancellationToken token = default);

        /// <summary>
        /// Determine the sum of a column for records that match the supplied filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="fieldName">The name of the field.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The sum of the specified column from the matching rows.</returns>
        public abstract decimal Sum(string tableName, string fieldName, Expr filter);

        /// <summary>
        /// Determine the sum of a column for records that match the supplied filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="fieldName">The name of the field.</param>
        /// <param name="filter">Expression.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The sum of the specified column from the matching rows.</returns>
        public abstract Task<decimal> SumAsync(string tableName, string fieldName, Expr filter, CancellationToken token = default);

        /// <summary>
        /// Create a string timestamp from the given DateTime.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>A string with formatted timestamp.</returns>
        public abstract string Timestamp(DateTime ts);

        /// <summary>
        /// Create a string timestamp with offset from the given DateTimeOffset.
        /// </summary>
        /// <param name="ts">DateTimeOffset.</param>
        /// <returns>A string with formatted timestamp.</returns>
        public abstract string TimestampOffset(DateTimeOffset ts);

        /// <summary>
        /// Sanitize an input string.
        /// </summary>
        /// <param name="s">The value to sanitize.</param>
        /// <returns>A sanitized string.</returns>
        public abstract string SanitizeString(string s);

        #endregion
    }
}
