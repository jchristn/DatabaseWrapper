using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data; 
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks; 
using DatabaseWrapper.Core;

namespace DatabaseWrapper.Sqlite
{
    /// <summary>
    /// Database client for MSSQL, Mysql, and PostgreSQL.
    /// </summary>
    public class DatabaseClient : IDisposable
    {
        #region Public-Members
         
        /// <summary>
        /// The connection string used to connect to the database.
        /// </summary>
        public string ConnectionString 
        { 
            get
            {
                return _ConnectionString;
            }
            private set
            {
                _ConnectionString = value;
            }
        }

        /// <summary>
        /// Enable or disable logging of queries using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogQueries = false;

        /// <summary>
        /// Enable or disable logging of query results using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogResults = false;

        /// <summary>
        /// Method to invoke when sending a log message.
        /// </summary>
        public Action<string> Logger = null;

        #endregion

        #region Private-Members

        private bool _Disposed = false;
        private string _Header = "[DatabaseWrapper.Sqlite] ";
        private string _ConnectionString = null;
        private string _SqliteFilename = null;   
        private Random _Random = new Random();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Create an instance of the database client.
        /// </summary>
        /// <param name="filename">Sqlite database filename.</param>
        public DatabaseClient(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename)); 
            _SqliteFilename = filename;
            _ConnectionString = SqliteHelper.ConnectionString(_SqliteFilename);
        }
         
        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the client and dispose of resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// List all tables in the database.
        /// </summary>
        /// <returns>List of strings, each being a table name.</returns>
        public List<string> ListTables()
        { 
            List<string> tableNames = new List<string>();
            DataTable result = Query(SqliteHelper.LoadTableNamesQuery());

            if (result != null && result.Rows.Count > 0)
            { 
                foreach (DataRow curr in result.Rows)
                {
                    tableNames.Add(curr["TABLE_NAME"].ToString());
                } 
            }

            return tableNames;
        }

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if exists.</returns>
        public bool TableExists(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            return ListTables().Contains(tableName);
        }

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <returns>A list of column objects.</returns>
        public List<Column> DescribeTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
              
            List<Column> columns = new List<Column>(); 
             
            DataTable result = Query(SqliteHelper.LoadTableColumnsQuery(tableName));
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow currColumn in result.Rows)
                {
                    #region Process-Each-Column

                    /*
                    public bool PrimaryKey;
                    public string Name;
                    public string DataType;
                    public int? MaxLength;
                    public bool Nullable;
                    */

                    Column tempColumn = new Column();
                    
                    tempColumn.Name = currColumn["COLUMN_NAME"].ToString();

                    tempColumn.MaxLength = null;
                    if (currColumn.Table.Columns.Contains("CHARACTER_MAXIMUM_LENGTH"))
                    {
                        int maxLength = 0;
                        if (Int32.TryParse(currColumn["CHARACTER_MAXIMUM_LENGTH"].ToString(), out maxLength))
                        {
                            tempColumn.MaxLength = maxLength;
                        }
                    }

                    tempColumn.Type = Helper.DataTypeFromString(currColumn["DATA_TYPE"].ToString());

                    if (currColumn.Table.Columns.Contains("IS_NULLABLE"))
                    {
                        if (String.Compare(currColumn["IS_NULLABLE"].ToString(), "YES") == 0) tempColumn.Nullable = true;
                        else tempColumn.Nullable = false;
                    }
                    else if (currColumn.Table.Columns.Contains("IS_NOT_NULLABLE"))
                    {
                        tempColumn.Nullable = !(Convert.ToBoolean(currColumn["IS_NOT_NULLABLE"]));
                    }
                     
                    if (currColumn["IS_PRIMARY_KEY"] != null
                        && currColumn["IS_PRIMARY_KEY"] != DBNull.Value
                        && !String.IsNullOrEmpty(currColumn["IS_PRIMARY_KEY"].ToString()))
                    {
                        tempColumn.PrimaryKey = Convert.ToBoolean(currColumn["IS_PRIMARY_KEY"]);
                    } 
                     
                    if (!columns.Exists(c => c.Name.Equals(tempColumn.Name)))
                    {
                        columns.Add(tempColumn);
                    }

                    #endregion
                } 
            }

            return columns; 
        }

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <returns>Dictionary where Key is table name, value is List of Column objects.</returns>
        public Dictionary<string, List<Column>> DescribeDatabase()
        { 
            DataTable result = new DataTable();
            Dictionary<string, List<Column>> ret = new Dictionary<string, List<Column>>();
            List<string> tableNames = ListTables();

            if (tableNames != null && tableNames.Count > 0)
            {
                foreach (string tableName in tableNames)
                {
                    ret.Add(tableName, DescribeTable(tableName));
                }
            }

            return ret; 
        }

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        public void CreateTable(string tableName, List<Column> columns)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (columns == null || columns.Count < 1) throw new ArgumentNullException(nameof(columns)); 
            Query(SqliteHelper.CreateTableQuery(tableName, columns)); 
        }

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        public void DropTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            Query(SqliteHelper.DropTableQuery(tableName)); 
        }

        /// <summary>
        /// Retrieve the name of the primary key column from a specific table.
        /// </summary>
        /// <param name="tableName">The table of which you want the primary key.</param>
        /// <returns>A string containing the column name.</returns>
        public string GetPrimaryKeyColumn(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details = DescribeTable(tableName);
            if (details != null && details.Count > 0)
            {
                foreach (Column c in details)
                {
                    if (c.PrimaryKey) return c.Name;
                }
            }

            return null;
        }

        /// <summary>
        /// Retrieve a list of the names of columns from within a specific table.
        /// </summary>
        /// <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
        /// <returns>A list of strings containing the column names.</returns>
        public List<string> GetColumnNames(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details = DescribeTable(tableName);
            List<string> columnNames = new List<string>();

            if (details != null && details.Count > 0)
            {
                foreach (Column c in details)
                {
                    columnNames.Add(c.Name);
                }
            }

            return columnNames;
        }

        /// <summary>
        /// Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="columnName">The column containing key or unique fields where a match is desired.</param>
        /// <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
        /// <returns>A DataTable containing at most one row.</returns>
        public DataTable GetUniqueObjectById(string tableName, string columnName, object value)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Expression e = new Expression
            {
                LeftTerm = columnName,
                Operator = Operators.Equals,
                RightTerm = value.ToString()
            };

            return Select(tableName, null, 1, null, e, null);
        }

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval; used for pagination in conjunction with maxResults and orderByClause.  orderByClause example: ORDER BY created DESC.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <param name="orderByClause">Specify an ORDER BY clause if desired.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName)); 
            return Query(SqliteHelper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, orderByClause));
        }

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            #region Variables

            string keys = "";
            string values = ""; 
            int insertedId = 0;
            string retrievalQuery = "";
            DataTable result;

            #endregion

            #region Build-Key-Value-Pairs

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue; 

                if (added == 0)
                {
                    #region First

                    keys += SqliteHelper.PreparedFieldname(curr.Key);
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            values += "'" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            values += curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                values += SqliteHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                values += SqliteHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        values += "null";
                    }

                    #endregion
                }
                else
                {
                    #region Subsequent

                    keys += "," + SqliteHelper.PreparedFieldname(curr.Key);
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            values += ",'" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            values += "," + curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                values += "," + SqliteHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                values += "," + SqliteHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }

                    }
                    else
                    {
                        values += ",null";
                    }

                    #endregion
                }

                added++;
            }

            #endregion

            #region Build-INSERT-Query-and-Submit
             
            result = Query(SqliteHelper.InsertQuery(tableName, keys, values));

            #endregion

            #region Post-Retrieval
             
            if (!Helper.DataTableIsNullOrEmpty(result))
            {
                bool idFound = false;

                string primaryKeyColumn = GetPrimaryKeyColumn(tableName);

                foreach (DataRow curr in result.Rows)
                {
                    if (Int32.TryParse(curr["id"].ToString(), out insertedId))
                    {
                        idFound = true;
                        break;
                    }
                }

                if (!idFound)
                {
                    result = null;
                }
                else
                {
                    retrievalQuery = "SELECT * FROM `" + tableName + "` WHERE " + primaryKeyColumn + "=" + insertedId;
                    result = Query(retrievalQuery);
                }
            } 

            #endregion

            return result;
        }

        /// <summary>
        /// Execute an UPDATE query. 
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
        public void Update(string tableName, Dictionary<string, object> keyValuePairs, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            #region Variables
             
            string keyValueClause = ""; 

            #endregion

            #region Build-Key-Value-Clause

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue; 

                if (added == 0)
                {
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            keyValueClause += SqliteHelper.PreparedFieldname(curr.Key) + "='" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            keyValueClause += SqliteHelper.PreparedFieldname(curr.Key) + "=" + curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                keyValueClause += SqliteHelper.PreparedFieldname(curr.Key) + "=" + SqliteHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                keyValueClause += SqliteHelper.PreparedFieldname(curr.Key) + "=" + SqliteHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        keyValueClause += SqliteHelper.PreparedFieldname(curr.Key) + "= null";
                    }
                }
                else
                {
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            keyValueClause += "," + SqliteHelper.PreparedFieldname(curr.Key) + "='" + DbTimestamp((DateTime)curr.Value) + "'";
                        }
                        else if (curr.Value is int || curr.Value is long || curr.Value is decimal)
                        {
                            keyValueClause += "," + SqliteHelper.PreparedFieldname(curr.Key) + "=" + curr.Value.ToString();
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                keyValueClause += "," + SqliteHelper.PreparedFieldname(curr.Key) + "=" + SqliteHelper.PreparedUnicodeValue(curr.Value.ToString());
                            }
                            else
                            {
                                keyValueClause += "," + SqliteHelper.PreparedFieldname(curr.Key) + "=" + SqliteHelper.PreparedStringValue(curr.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        keyValueClause += "," + SqliteHelper.PreparedFieldname(curr.Key) + "= null";
                    }
                }
                added++;
            }

            #endregion

            #region Build-UPDATE-Query-and-Submit
             
            Query(SqliteHelper.UpdateQuery(tableName, keyValueClause, filter));

            #endregion
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        public void Delete(string tableName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (filter == null) throw new ArgumentNullException(nameof(filter)); 
            Query(SqliteHelper.DeleteQuery(tableName, filter));
        }

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        public void Truncate(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            Query(SqliteHelper.TruncateQuery(tableName));
        }

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Query(string query)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(query);
            DataTable result = new DataTable();

            if (LogQueries && Logger != null) Logger(_Header + "query: " + query);
             
            using (SQLiteConnection conn = new SQLiteConnection(_ConnectionString))
            {
                conn.Open();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                using (SQLiteCommand cmd = new SQLiteCommand(query, conn))
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                {
                    using (SQLiteDataReader rdr = cmd.ExecuteReader())
                    {
                        result.Load(rdr);
                    }
                }

                conn.Close();
            } 

            if (LogResults && Logger != null)
            {
                if (result != null)
                {
                    Logger(_Header + "result: " + result.Rows.Count + " rows");
                }
                else
                {
                    Logger(_Header + "result: null");
                }
            }

            return result;
        }

        /// <summary>
        /// Create a string timestamp from the given DateTime.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>A string with formatted timestamp.</returns>
        public string Timestamp(DateTime ts)
        {
            return SqliteHelper.DbTimestamp(ts);
        }

        /// <summary>
        /// Sanitize an input string.
        /// </summary>
        /// <param name="s">The value to sanitize.</param>
        /// <returns>A sanitized string.</returns>
        public string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return s; 
            return SqliteHelper.SanitizeString(s); 
        }
         
        #endregion

        #region Private-Methods

        /// <summary>
        /// Dispose of the object.
        /// </summary>
        /// <param name="disposing">Disposing of resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            { 
                // placeholder
            }

            _Disposed = true;
        }
          
        #endregion

        #region Public-Static-Methods

        /// <summary>
        /// Convert a DateTime to a formatted string.
        /// </summary> 
        /// <param name="ts">The timestamp.</param>
        /// <returns>A string formatted for use with the specified database.</returns>
        public static string DbTimestamp(DateTime ts)
        {
            return SqliteHelper.DbTimestamp(ts);
        }
         
        #endregion
    }
}
