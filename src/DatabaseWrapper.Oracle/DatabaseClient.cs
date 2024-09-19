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
using Oracle.ManagedDataAccess.Client;

namespace DatabaseWrapper.Oracle
{
    /// <summary>
    /// Database client for Oracle.
    /// </summary>
    public class DatabaseClient : DatabaseClientBase, IDisposable
    {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

        #region Public-Members

        /// <summary>
        /// The connection string used to connect to the database.
        /// </summary>
        public new string ConnectionString
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
        /// Timestamp format.
        /// Default is yyyy-MM-dd HH:mm:ss.ffffff.
        /// </summary>
        public new string TimestampFormat
        {
            get
            {
                return TimestampFormat;
            }
            set
            {
                if (String.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(TimestampFormat));
                TimestampFormat = value;
            }
        }

        /// <summary>
        /// Timestamp format with offset.
        /// Default is yyyy-MM-dd HH:mm:ss.ffffffzzz.
        /// </summary>
        public new string TimestampOffsetFormat
        {
            get
            {
                return TimestampOffsetFormat;
            }
            set
            {
                if (String.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(TimestampOffsetFormat));
                TimestampOffsetFormat = value;
            }
        }

        /// <summary>
        /// Maximum supported statement length.
        /// </summary>
        public new int MaxStatementLength
        {
            get
            {
                return _MaxStatementLength;
            }
        }

        /// <summary>
        /// Database settings.
        /// </summary>
        public new DatabaseSettings Settings
        {
            get
            {
                return _Settings;
            }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(Settings));
                _Settings = value;
            }
        }

        /// <summary>
        /// Event to fire when a query is handled.
        /// </summary>
        public new event EventHandler<DatabaseQueryEvent> QueryEvent = delegate { };

        #endregion

        #region Private-Members

        private bool _Disposed = false;
        private string _Header = "[DatabaseWrapper.Oracle] ";
        private DatabaseSettings _Settings = null;
        private string _ConnectionString = null;
        private int _MaxStatementLength = 4194304;

        private Random _Random = new Random();

        private string _CountColumnName = "oocountoo";
        private string _SumColumnName = "oosumoo";
        private OracleHelper _Helper = new OracleHelper();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Create an instance of the database client.
        /// </summary>
        /// <param name="settings">Database settings.</param>
        public DatabaseClient(DatabaseSettings settings)
        {
            _Settings = settings ?? throw new ArgumentNullException(nameof(settings));
            if (_Settings.Type != DbTypeEnum.Oracle) throw new ArgumentException("Database settings must be of type 'Oracle'.");
            _ConnectionString = _Helper.GenerateConnectionString(_Settings);

            SetMaxStatementLength();
        }

        /// <summary>
        /// Create an instance of the database client.
        /// </summary> 
        /// <param name="serverIp">The IP address or hostname of the database server.</param>
        /// <param name="serverPort">The TCP port of the database server.</param>
        /// <param name="username">The username to use when authenticating with the database server.</param>
        /// <param name="password">The password to use when authenticating with the database server.</param> 
        /// <param name="database">The name of the database with which to connect.</param>
        public DatabaseClient(
            string serverIp,
            int serverPort,
            string username,
            string password,
            string database)
        {
            if (String.IsNullOrEmpty(serverIp)) throw new ArgumentNullException(nameof(serverIp));
            if (serverPort < 0) throw new ArgumentOutOfRangeException(nameof(serverPort));
            if (String.IsNullOrEmpty(database)) throw new ArgumentNullException(nameof(database));

            _Settings = new DatabaseSettings(DbTypeEnum.Oracle, serverIp, serverPort, username, password, database);
            _ConnectionString = _Helper.GenerateConnectionString(_Settings);

            SetMaxStatementLength();
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
        public override List<string> ListTables()
        {
            List<string> tableNames = new List<string>();
            DataTable result = Query(_Helper.RetrieveTableNamesQuery(null));

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
        /// Retrieve the list of sequences.
        /// </summary>
        /// <returns>List of sequences.</returns>
        public List<string> ListSequences()
        {
            List<string> tableNames = new List<string>();
            DataTable result = Query(_Helper.RetrieveSequencesQuery(null));

            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow curr in result.Rows)
                {
                    tableNames.Add(curr["SEQUENCE_NAME"].ToString());
                }
            }

            return tableNames;
        }

        /// <summary>
        /// List all tables in the database.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>List of strings, each being a table name.</returns>
        public override async Task<List<string>> ListTablesAsync(CancellationToken token = default)
        {
            List<string> tableNames = new List<string>();
            DataTable result = await QueryAsync(_Helper.RetrieveTableNamesQuery(null), token).ConfigureAwait(false);

            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow curr in result.Rows)
                {
                    tableNames.Add(curr["Tables_in_" + _Settings.DatabaseName].ToString());
                }
            }

            return tableNames;
        }

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if exists.</returns>
        public override bool TableExists(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return ListTables().Contains(tableName.ToUpper());
        }

        /// <summary>
        /// Check if a sequence exists.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>True if a sequence exists.</returns>
        public bool SequenceExists(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return ListSequences().Contains(tableName.ToUpper() + "_SEQ");
        }

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True if exists.</returns>
        public override async Task<bool> TableExistsAsync(string tableName, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            List<string> tables = await ListTablesAsync(token).ConfigureAwait(false);
            return tables.Contains(tableName);
        }

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <returns>A list of column objects.</returns>
        public override List<Column> DescribeTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> columns = new List<Column>();
            DataTable result = Query(_Helper.RetrieveTableColumnsQuery(_Settings.DatabaseName, tableName));
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow currColumn in result.Rows)
                {
                    #region Process-Each-Column

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

                    if (currColumn.Table.Columns.Contains("NULLABLE"))
                    {
                        if (String.Compare(currColumn["NULLABLE"].ToString(), "Y") == 0) tempColumn.Nullable = true;
                        else tempColumn.Nullable = false;
                    }
                    else if (currColumn.Table.Columns.Contains("IS_NOT_NULLABLE"))
                    {
                        tempColumn.Nullable = !(Convert.ToBoolean(currColumn["IS_NOT_NULLABLE"]));
                    }

                    tempColumn.PrimaryKey = IsPrimary(_Settings.DatabaseName, tableName, tempColumn.Name);


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
        /// Determine if a column is the primary key for a given database table.
        /// </summary>
        /// <param name="databaseName">Database.</param>
        /// <param name="tableName">Table.</param>
        /// <param name="column">Column.</param>
        /// <returns>True if primary key.</returns>
        public bool IsPrimary(string databaseName, string tableName, string column)
        {
            DataTable result = Query(_Helper.IsPrimaryKeyQuery(_Settings.DatabaseName, tableName, column));
            if (result.Rows.Count > 0) return true;
            return false;
        }

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A list of column objects.</returns>
        public override async Task<List<Column>> DescribeTableAsync(string tableName, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> columns = new List<Column>();
            DataTable result = await QueryAsync(_Helper.RetrieveTableColumnsQuery(_Settings.DatabaseName, tableName), token).ConfigureAwait(false);
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow currColumn in result.Rows)
                {
                    #region Process-Each-Column

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

                    if (currColumn["COLUMN_KEY"] != null
                        && currColumn["COLUMN_KEY"] != DBNull.Value
                        && !String.IsNullOrEmpty(currColumn["COLUMN_KEY"].ToString()))
                    {
                        if (currColumn["COLUMN_KEY"].ToString().ToLower().Equals("pri")) tempColumn.PrimaryKey = true;
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
        public override Dictionary<string, List<Column>> DescribeDatabase()
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
        /// Describe each of the tables in the database.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Dictionary where Key is table name, value is List of Column objects.</returns>
        public override async Task<Dictionary<string, List<Column>>> DescribeDatabaseAsync(CancellationToken token = default)
        {
            DataTable result = new DataTable();
            Dictionary<string, List<Column>> ret = new Dictionary<string, List<Column>>();
            List<string> tableNames = await ListTablesAsync(token).ConfigureAwait(false);

            if (tableNames != null && tableNames.Count > 0)
            {
                foreach (string tableName in tableNames)
                {
                    ret.Add(tableName, await DescribeTableAsync(tableName, token).ConfigureAwait(false));
                }
            }

            return ret;
        }

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        public override void CreateTable(string tableName, List<Column> columns)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (columns == null || columns.Count < 1) throw new ArgumentNullException(nameof(columns));

            string primaryKeyCol = null;
            foreach (Column col in columns)
                if (col.PrimaryKey) primaryKeyCol = col.Name;

            string query = _Helper.CreateTableQuery(tableName, columns);
            Query(query);
            if (query.Contains("PRIMARY KEY"))
            {
                Query(_Helper.CreateSequnce(tableName));
                Query(_Helper.CreateTrigger(tableName, primaryKeyCol));
            }
        }

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        /// <param name="token">Cancellation token.</param>
        public override async Task CreateTableAsync(string tableName, List<Column> columns, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (columns == null || columns.Count < 1) throw new ArgumentNullException(nameof(columns));
            await QueryAsync(_Helper.CreateTableQuery(tableName, columns), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        public override void DropTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (TableExists(tableName))
                Query(_Helper.DropTableQuery(tableName));
            if (SequenceExists(tableName))
            {
                Query(_Helper.DropSequenceQuery(tableName));
            }
        }

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        /// <param name="token">Cancellation token.</param>
        public override async Task DropTableAsync(string tableName, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            await QueryAsync(_Helper.DropTableQuery(tableName), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieve the name of the primary key column from a specific table.
        /// </summary>
        /// <param name="tableName">The table of which you want the primary key.</param>
        /// <returns>A string containing the column name.</returns>
        public override string GetPrimaryKeyColumn(string tableName)
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
        public override List<string> GetColumnNames(string tableName)
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
        public override DataTable GetUniqueObjectById(string tableName, string columnName, object value)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Expr e = new Expr
            {
                Left = columnName,
                Operator = OperatorEnum.Equals,
                Right = value.ToString()
            };

            return Select(tableName, null, 1, null, e, null);
        }

        /// <summary>
        /// Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="columnName">The column containing key or unique fields where a match is desired.</param>
        /// <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing at most one row.</returns>
        public override async Task<DataTable> GetUniqueObjectByIdAsync(string tableName, string columnName, object value, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Expr e = new Expr
            {
                Left = columnName,
                Operator = OperatorEnum.Equals,
                Right = value.ToString()
            };

            return await SelectAsync(tableName, null, 1, null, e, null, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <returns>A DataTable containing the results.</returns>
        public override DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return Query(_Helper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, null));
        }

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
        public override async Task<DataTable> SelectAsync(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return await QueryAsync(_Helper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, null), token).ConfigureAwait(false);
        }

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
        public override DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return Query(_Helper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, resultOrder));
        }

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
        public override async Task<DataTable> SelectAsync(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            return await QueryAsync(_Helper.SelectQuery(tableName, indexStart, maxResults, returnFields, filter, resultOrder), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>A DataTable containing the results.</returns>
        public override DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            DataTable result = QueryParams(_Helper.InsertQueryParams(tableName, keyValuePairs), keyValuePairs);

            if (!Helper.DataTableIsNullOrEmpty(result))
            {
                bool idFound = false;

                string primaryKeyColumn = GetPrimaryKeyColumn(tableName);
                int insertedId = 0;

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
                    string retrievalQuery = "SELECT * FROM `" + tableName + "` WHERE " + primaryKeyColumn + "=" + insertedId;
                    result = Query(retrievalQuery);
                }
            }

            return result;
        }

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing the results.</returns>
        public override async Task<DataTable> InsertAsync(string tableName, Dictionary<string, object> keyValuePairs, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            DataTable result = await QueryAsync(_Helper.InsertQuery(tableName, keyValuePairs), token).ConfigureAwait(false);

            if (!Helper.DataTableIsNullOrEmpty(result))
            {
                bool idFound = false;

                string primaryKeyColumn = GetPrimaryKeyColumn(tableName);
                int insertedId = 0;

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
                    string retrievalQuery = "SELECT * FROM `" + tableName + "` WHERE " + primaryKeyColumn + "=" + insertedId;
                    result = await QueryAsync(retrievalQuery, token).ConfigureAwait(false);
                }
            }

            return result;
        }

        /// <summary>
        /// Execute an INSERT query with multiple values within a transaction.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        public override void InsertMultiple(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairList == null || keyValuePairList.Count < 1) throw new ArgumentNullException(nameof(keyValuePairList));
            foreach (Dictionary<string, object> keyValuePair in keyValuePairList)
            {
                QueryParams(_Helper.InsertQueryParams(tableName, keyValuePair), keyValuePair);
            }

        }

        /// <summary>
        /// Execute an INSERT query with multiple values within a transaction.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <param name="token">Cancellation token.</param>
        public override async Task InsertMultipleAsync(string tableName, List<Dictionary<string, object>> keyValuePairList, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairList == null || keyValuePairList.Count < 1) throw new ArgumentNullException(nameof(keyValuePairList));
            await QueryAsync(_Helper.InsertMultipleQuery(tableName, keyValuePairList), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute an UPDATE query. 
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
        public override void Update(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));
            Query(_Helper.UpdateQuery(tableName, keyValuePairs, filter));
        }

        /// <summary>
        /// Execute an UPDATE query. 
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param> 
        /// <param name="token">Cancellation token.</param>
        public override async Task UpdateAsync(string tableName, Dictionary<string, object> keyValuePairs, Expr filter, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));
            await QueryAsync(_Helper.UpdateQuery(tableName, keyValuePairs, filter), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        public override void Delete(string tableName, Expr filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            Query(_Helper.DeleteQuery(tableName, filter));
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        /// <param name="token">Cancellation token.</param>
        public override async Task DeleteAsync(string tableName, Expr filter, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            await QueryAsync(_Helper.DeleteQuery(tableName, filter), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        public override void Truncate(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            Query(_Helper.TruncateQuery(tableName));
        }

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        /// <param name="token">Cancellation token.</param>
        public override async Task TruncateAsync(string tableName, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            await QueryAsync(_Helper.TruncateQuery(tableName), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <returns>A DataTable containing the results.</returns>
        public override DataTable Query(string query)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(query);
            if (query.Length > MaxStatementLength) throw new ArgumentException("Query exceeds maximum statement length of " + MaxStatementLength + " characters.");

            DateTime startTime = DateTime.Now;
            DataTable result = new DataTable();
            Exception ex = null;

            if (_Settings.Debug.EnableForQueries && _Settings.Debug.Logger != null)
                _Settings.Debug.Logger(_Header + "query: " + query);

            try
            {
                using (OracleConnection conn = new OracleConnection(_ConnectionString))
                {
                    conn.Open();

                    using (OracleCommand cmd = new OracleCommand())
                    {
                        cmd.Connection = conn;

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        cmd.CommandText = query;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        using (OracleDataAdapter sda = new OracleDataAdapter(cmd))
                        {
                            DataSet ds = new DataSet();
                            sda.Fill(ds);
                            if (ds != null)
                            {
                                if (ds.Tables != null)
                                {
                                    if (ds.Tables.Count > 0)
                                    {
                                        result = ds.Tables[0];
                                    }
                                }
                            }
                        }
                    }

                    conn.Close();
                }

                if (_Settings.Debug.EnableForResults && _Settings.Debug.Logger != null)
                {
                    if (result != null)
                    {
                        _Settings.Debug.Logger(_Header + "result: " + result.Rows.Count + " rows");
                    }
                    else
                    {
                        _Settings.Debug.Logger(_Header + "result: null");
                    }
                }

                return result;
            }
            catch (Exception e)
            {
                e.Data.Add("Query", query);
                ex = e;
                throw;
            }
            finally
            {
                double totalMs = (DateTime.Now - startTime).TotalMilliseconds;
                QueryEvent?.Invoke(this, new DatabaseQueryEvent(query, totalMs, result, ex));
            }
        }

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A DataTable containing the results.</returns>
        public override async Task<DataTable> QueryAsync(string query, CancellationToken token = default)
        {
            throw new NotImplementedException();
            //TODO Oracle managed data access doesn't have FillAsync Method
            //            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(query);
            //            if (query.Length > MaxStatementLength) throw new ArgumentException("Query exceeds maximum statement length of " + MaxStatementLength + " characters.");

            //            DateTime startTime = DateTime.Now;
            //            DataTable result = new DataTable();
            //            Exception ex = null;

            //            if (_Settings.Debug.EnableForQueries && _Settings.Debug.Logger != null)
            //                _Settings.Debug.Logger(_Header + "query: " + query);

            //            try
            //            {
            //                using (OracleConnection conn = new OracleConnection(_ConnectionString))
            //                {
            //                    await conn.OpenAsync(token).ConfigureAwait(false);

            //                    using (OracleCommand cmd = new OracleCommand())
            //                    {
            //                        cmd.Connection = conn;

            //#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
            //                        cmd.CommandText = query;
            //#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

            //                        using (OracleDataAdapter sda = new OracleDataAdapter(cmd))
            //                        {
            //                            DataSet ds = new DataSet();

            //                            await sda.FillAsync(ds, token).ConfigureAwait(false);
            //                            if (ds != null)
            //                            {
            //                                if (ds.Tables != null)
            //                                {
            //                                    if (ds.Tables.Count > 0)
            //                                    {
            //                                        result = ds.Tables[0];
            //                                    }
            //                                }
            //                            }
            //                        }
            //                    }

            //                    await conn.CloseAsync().ConfigureAwait(false);
            //                }

            //                if (_Settings.Debug.EnableForResults && _Settings.Debug.Logger != null)
            //                {
            //                    if (result != null)
            //                    {
            //                        _Settings.Debug.Logger(_Header + "result: " + result.Rows.Count + " rows");
            //                    }
            //                    else
            //                    {
            //                        _Settings.Debug.Logger(_Header + "result: null");
            //                    }
            //                }

            //                return result;
            //            }
            //            catch (Exception e)
            //            {
            //                e.Data.Add("Query", query);
            //                ex = e;
            //                throw;
            //            }
            //            finally
            //            {
            //                double totalMs = (DateTime.Now - startTime).TotalMilliseconds;
            //                QueryEvent?.Invoke(this, new DatabaseQueryEvent(query, totalMs, result, ex));
            //            }
        }

        /// <summary>
        /// Determine if records exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>True if records exist.</returns>
        public override bool Exists(string tableName, Expr filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            DataTable result = Query(_Helper.ExistsQuery(tableName, filter));
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        /// <summary>
        /// Determine if records exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True if records exist.</returns>
        public override async Task<bool> ExistsAsync(string tableName, Expr filter, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            DataTable result = await QueryAsync(_Helper.ExistsQuery(tableName, filter), token).ConfigureAwait(false);
            if (result != null && result.Rows.Count > 0) return true;
            return false;
        }

        /// <summary>
        /// Determine the number of records that exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The number of records.</returns>
        public override long Count(string tableName, Expr filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            DataTable result = Query(_Helper.CountQuery(tableName, _CountColumnName, filter));
            if (result != null
                && result.Rows.Count > 0
                && result.Rows[0].Table.Columns.Contains(_CountColumnName)
                && result.Rows[0][_CountColumnName] != null
                && result.Rows[0][_CountColumnName] != DBNull.Value)
            {
                return Convert.ToInt64(result.Rows[0][_CountColumnName]);
            }
            return 0;
        }

        /// <summary>
        /// Determine the number of records that exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of records.</returns>
        public override async Task<long> CountAsync(string tableName, Expr filter, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            DataTable result = await QueryAsync(_Helper.CountQuery(tableName, _CountColumnName, filter), token).ConfigureAwait(false);
            if (result != null
                && result.Rows.Count > 0
                && result.Rows[0].Table.Columns.Contains(_CountColumnName)
                && result.Rows[0][_CountColumnName] != null
                && result.Rows[0][_CountColumnName] != DBNull.Value)
            {
                return Convert.ToInt64(result.Rows[0][_CountColumnName]);
            }
            return 0;
        }

        /// <summary>
        /// Determine the sum of a column for records that match the supplied filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="fieldName">The name of the field.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The sum of the specified column from the matching rows.</returns>
        public override decimal Sum(string tableName, string fieldName, Expr filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(fieldName)) throw new ArgumentNullException(nameof(fieldName));
            DataTable result = Query(_Helper.SumQuery(tableName, fieldName, _SumColumnName, filter));
            if (result != null
                && result.Rows.Count > 0
                && result.Rows[0].Table.Columns.Contains(_SumColumnName)
                && result.Rows[0][_SumColumnName] != null
                && result.Rows[0][_SumColumnName] != DBNull.Value)
            {
                return Convert.ToDecimal(result.Rows[0][_SumColumnName]);
            }
            return 0m;
        }

        /// <summary>
        /// Determine the sum of a column for records that match the supplied filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="fieldName">The name of the field.</param>
        /// <param name="filter">Expression.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The sum of the specified column from the matching rows.</returns>
        public override async Task<decimal> SumAsync(string tableName, string fieldName, Expr filter, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(fieldName)) throw new ArgumentNullException(nameof(fieldName));
            DataTable result = await QueryAsync(_Helper.SumQuery(tableName, fieldName, _SumColumnName, filter), token).ConfigureAwait(false);
            if (result != null
                && result.Rows.Count > 0
                && result.Rows[0].Table.Columns.Contains(_SumColumnName)
                && result.Rows[0][_SumColumnName] != null
                && result.Rows[0][_SumColumnName] != DBNull.Value)
            {
                return Convert.ToDecimal(result.Rows[0][_SumColumnName]);
            }
            return 0m;
        }

        /// <summary>
        /// Create a string timestamp from the given DateTime.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>A string with formatted timestamp.</returns>
        public override string Timestamp(DateTime ts)
        {
            return ts.ToString(_Helper.TimestampFormat);
        }

        /// <summary>
        /// Create a string timestamp with offset from the given DateTimeOffset.
        /// </summary>
        /// <param name="ts">DateTimeOffset.</param>
        /// <returns>A string with formatted timestamp.</returns>
        public override string TimestampOffset(DateTimeOffset ts)
        {
            return ts.ToString(_Helper.TimestampOffsetFormat);
        }

        /// <summary>
        /// Sanitize an input string.
        /// </summary>
        /// <param name="s">The value to sanitize.</param>
        /// <returns>A sanitized string.</returns>
        public override string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return s;
            return SanitizeString(s);
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

        private void SetMaxStatementLength()
        {
            //TODO Finding an equivalent for oracle
            //DataTable dt = Query("SHOW VARIABLES LIKE 'max_allowed_packet'");
            //if (dt != null
            //    && dt.Rows.Count == 1
            //    && dt.Columns.Contains("Value"))
            //{
            //    _MaxStatementLength = Convert.ToInt32(dt.Rows[0]["Value"]);
            //}
        }

        private DataTable QueryParams(string query, Dictionary<string, object> keyValuePairs)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(query);
            if (query.Length > MaxStatementLength) throw new ArgumentException("Query exceeds maximum statement length of " + MaxStatementLength + " characters.");

            DateTime startTime = DateTime.Now;
            DataTable result = new DataTable();
            Exception ex = null;

            if (_Settings.Debug.EnableForQueries && _Settings.Debug.Logger != null)
                _Settings.Debug.Logger(_Header + "query: " + query);

            try
            {
                using (OracleConnection conn = new OracleConnection(_ConnectionString))
                {
                    conn.Open();

                    using (OracleCommand cmd = new OracleCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Parameters.Clear();
                        List<OracleParameter> opc = GetParametersFromkeyValuePairs(keyValuePairs);
                        foreach (OracleParameter param in opc)
                        {
                            cmd.Parameters.Add(param);
                        }
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        cmd.CommandText = query;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        using (OracleDataAdapter sda = new OracleDataAdapter(cmd))
                        {
                            DataSet ds = new DataSet();
                            sda.Fill(ds);
                            if (ds != null)
                            {
                                if (ds.Tables != null)
                                {
                                    if (ds.Tables.Count > 0)
                                    {
                                        result = ds.Tables[0];
                                    }
                                }
                            }
                        }
                    }

                    conn.Close();
                }

                if (_Settings.Debug.EnableForResults && _Settings.Debug.Logger != null)
                {
                    if (result != null)
                    {
                        _Settings.Debug.Logger(_Header + "result: " + result.Rows.Count + " rows");
                    }
                    else
                    {
                        _Settings.Debug.Logger(_Header + "result: null");
                    }
                }

                return result;
            }
            catch (Exception e)
            {
                e.Data.Add("Query", query);
                ex = e;
                throw;
            }
            finally
            {
                double totalMs = (DateTime.Now - startTime).TotalMilliseconds;
                QueryEvent?.Invoke(this, new DatabaseQueryEvent(query, totalMs, result, ex));
            }
        }

        private List<OracleParameter> GetParametersFromkeyValuePairs(Dictionary<string, object> keyValuePairs)
        {
            List<OracleParameter> prm = new List<OracleParameter>();
            foreach (KeyValuePair<string, object> currKvp in keyValuePairs)
            {
                if (currKvp.Value is DateTime
                    || currKvp.Value is DateTime?)
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value;
                    prm.Add(para);
                }
                else if (currKvp.Value is DateTimeOffset
                    || currKvp.Value is DateTimeOffset?)
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value;
                    prm.Add(para);
                }
                else if (currKvp.Value is int
                    || currKvp.Value is long
                    || currKvp.Value is decimal)
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value;
                    prm.Add(para);
                }
                else if (currKvp.Value is bool)
                {
                    string val = ((bool)currKvp.Value ? "1" : "0");
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = val;
                    prm.Add(para);
                }
                else if (currKvp.Value is byte[])
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value;
                    prm.Add(para);
                }
                else if (currKvp.Value is string)
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value;
                    prm.Add(para);
                }
                else if (currKvp.Value is Guid)
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value.ToString();
                    prm.Add(para);
                }
                else
                {
                    OracleParameter para = new OracleParameter();
                    para.ParameterName = ":" + currKvp.Key;
                    para.Direction = ParameterDirection.Input;
                    para.Value = currKvp.Value;
                    prm.Add(para);
                }
            }

            return prm;
        }

        #endregion

#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    }
}
