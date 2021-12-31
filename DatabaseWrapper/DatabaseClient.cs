using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data; 
using System.Linq;
using System.Text;
using System.Threading.Tasks; 
using DatabaseWrapper.Core;
using DatabaseWrapper.Mysql;
using DatabaseWrapper.Postgresql;
using DatabaseWrapper.Sqlite;
using DatabaseWrapper.SqlServer;
using ExpressionTree;

namespace DatabaseWrapper
{
    /// <summary>
    /// Database client for Microsoft SQL Server, Mysql, PostgreSQL, and Sqlite.
    /// </summary>
    public class DatabaseClient : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// The type of database.
        /// </summary>
        public DbTypes Type
        {
            get
            {
                return _Settings.Type;
            }
        }

        /// <summary>
        /// The connection string used to connect to the database server.
        /// </summary>
        public string ConnectionString 
        { 
            get
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        return _SqlServer.ConnectionString;
                    case DbTypes.Mysql:
                        return _Mysql.ConnectionString;
                    case DbTypes.Postgresql:
                        return _Postgresql.ConnectionString;
                    case DbTypes.Sqlite:
                        return _Sqlite.ConnectionString;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        /// <summary>
        /// Enable or disable logging of queries using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogQueries
        {
            get
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        return _SqlServer.LogQueries;
                    case DbTypes.Mysql:
                        return _Mysql.LogQueries;
                    case DbTypes.Postgresql:
                        return _Postgresql.LogQueries;
                    case DbTypes.Sqlite:
                        return _Sqlite.LogQueries;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
            set
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        _SqlServer.LogQueries = value;
                        break;
                    case DbTypes.Mysql:
                        _Mysql.LogQueries = value;
                        break;
                    case DbTypes.Postgresql:
                        _Postgresql.LogQueries = value;
                        break;
                    case DbTypes.Sqlite:
                        _Sqlite.LogQueries = value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        /// <summary>
        /// Enable or disable logging of query results using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogResults
        {
            get
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        return _SqlServer.LogResults;
                    case DbTypes.Mysql:
                        return _Mysql.LogResults;
                    case DbTypes.Postgresql:
                        return _Postgresql.LogResults;
                    case DbTypes.Sqlite:
                        return _Sqlite.LogResults;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
            set
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        _SqlServer.LogResults = value;
                        break;
                    case DbTypes.Mysql:
                        _Mysql.LogResults = value;
                        break;
                    case DbTypes.Postgresql:
                        _Postgresql.LogResults = value;
                        break;
                    case DbTypes.Sqlite:
                        _Sqlite.LogResults = value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        /// <summary>
        /// Method to invoke when sending a log message.
        /// </summary>
        public Action<string> Logger
        {
            get
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        return _SqlServer.Logger;
                    case DbTypes.Mysql:
                        return _Mysql.Logger;
                    case DbTypes.Postgresql:
                        return _Postgresql.Logger;
                    case DbTypes.Sqlite:
                        return _Sqlite.Logger;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
            set
            {
                switch (_Settings.Type)
                {
                    case DbTypes.SqlServer:
                        _SqlServer.Logger = value;
                        break;
                    case DbTypes.Mysql:
                        _Mysql.Logger = value;
                        break;
                    case DbTypes.Postgresql:
                        _Postgresql.Logger = value;
                        break;
                    case DbTypes.Sqlite:
                        _Sqlite.Logger = value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        #endregion

        #region Private-Members

        private bool _Disposed = false;
        private string _Header = "";
        private DatabaseSettings _Settings = null;
         
        private DatabaseWrapper.Mysql.DatabaseClient        _Mysql = null;
        private DatabaseWrapper.Postgresql.DatabaseClient   _Postgresql = null;
        private DatabaseWrapper.Sqlite.DatabaseClient       _Sqlite = null;
        private DatabaseWrapper.SqlServer.DatabaseClient    _SqlServer = null;
          
        private Random _Random = new Random();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Create an instance of the database client.
        /// </summary>
        /// <param name="settings">Database settings.</param>
        public DatabaseClient(DatabaseSettings settings)
        {
            _Settings = settings ?? throw new ArgumentNullException(nameof(settings));

            if (_Settings.Type == DbTypes.Sqlite && String.IsNullOrEmpty(_Settings.Filename))
                throw new ArgumentException("Filename must be populated in database settings of type 'Sqlite'.");

            if (_Settings.Type != DbTypes.SqlServer && !String.IsNullOrEmpty(_Settings.Instance))
                throw new ArgumentException("Instance can only be used in database settings of type 'SqlServer'.");

            _Header = "[DatabaseWrapper." + _Settings.Type.ToString() + "] ";

            switch (_Settings.Type)
            {
                case DbTypes.Sqlite:
                    _Sqlite = new Sqlite.DatabaseClient(_Settings);
                    break;
                case DbTypes.Mysql:
                    _Mysql = new Mysql.DatabaseClient(_Settings);
                    break;
                case DbTypes.Postgresql:
                    _Postgresql = new Postgresql.DatabaseClient(_Settings);
                    break;
                case DbTypes.SqlServer:
                    _SqlServer = new SqlServer.DatabaseClient(_Settings);
                    break;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Create an instance of the database client for a Sqlite database file.
        /// </summary>
        /// <param name="filename">Sqlite database.</param>
        public DatabaseClient(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            _Settings = new DatabaseSettings(filename);
            _Header = "[DatabaseWrapper." + _Settings.Type.ToString() + "] "; 
            _Sqlite = new Sqlite.DatabaseClient(filename);
        }

        /// <summary>
        /// Create an instance of the database client.
        /// </summary>
        /// <param name="dbType">The type of database.</param>
        /// <param name="serverIp">The IP address or hostname of the database server.</param>
        /// <param name="serverPort">The TCP port of the database server.</param>
        /// <param name="username">The username to use when authenticating with the database server.</param>
        /// <param name="password">The password to use when authenticating with the database server.</param>
        /// <param name="instance">The instance on the database server (for use with Microsoft SQL Server).</param>
        /// <param name="database">The name of the database with which to connect.</param>
        public DatabaseClient(
            DbTypes dbType,
            string serverIp,
            int serverPort,
            string username,
            string password,
            string instance,
            string database)
        {
            if (dbType == DbTypes.Sqlite) throw new ArgumentException("Use the filename constructor for Sqlite databases.");
            if (String.IsNullOrEmpty(serverIp)) throw new ArgumentNullException(nameof(serverIp));
            if (serverPort < 0) throw new ArgumentOutOfRangeException(nameof(serverPort));
            if (String.IsNullOrEmpty(database)) throw new ArgumentNullException(nameof(database));

            if (dbType == DbTypes.SqlServer)
            {
                _Settings = new DatabaseSettings(serverIp, serverPort, username, password, instance, database);
            }
            else
            {
                if (!String.IsNullOrEmpty(instance))
                    throw new ArgumentException("Instance can only be used in database settings of type 'SqlServer'.");

                _Settings = new DatabaseSettings(dbType, serverIp, serverPort, username, password, database);
            }
            
            _Header = "[DatabaseWrapper." + _Settings.Type.ToString() + "] ";

            switch (_Settings.Type)
            {
                case DbTypes.Sqlite:
                    throw new ArgumentException("Unable to use this constructor with 'DbTypes.Sqlite'.");
                case DbTypes.Mysql:
                    _Mysql = new Mysql.DatabaseClient(_Settings);
                    break;
                case DbTypes.Postgresql:
                    _Postgresql = new Postgresql.DatabaseClient(_Settings);
                    break;
                case DbTypes.SqlServer:
                    _SqlServer = new SqlServer.DatabaseClient(_Settings);
                    break;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Create an instance of the database client.
        /// </summary>
        /// <param name="dbType">The type of database.</param>
        /// <param name="serverIp">The IP address or hostname of the database server.</param>
        /// <param name="serverPort">The TCP port of the database server.</param>
        /// <param name="username">The username to use when authenticating with the database server.</param>
        /// <param name="password">The password to use when authenticating with the database server.</param> 
        /// <param name="database">The name of the database with which to connect.</param>
        public DatabaseClient(
            DbTypes dbType,
            string serverIp,
            int serverPort,
            string username,
            string password, 
            string database)
        {
            if (String.IsNullOrEmpty(serverIp)) throw new ArgumentNullException(nameof(serverIp));
            if (serverPort < 0) throw new ArgumentOutOfRangeException(nameof(serverPort));
            if (String.IsNullOrEmpty(database)) throw new ArgumentNullException(nameof(database));

            _Settings = new DatabaseSettings(dbType, serverIp, serverPort, username, password, database); 
            _Header = "[DatabaseWrapper." + _Settings.Type.ToString() + "] ";

            switch (_Settings.Type)
            {
                case DbTypes.Sqlite:
                    throw new ArgumentException("Unable to use this constructor with 'DbTypes.Sqlite'.");
                case DbTypes.Mysql:
                    _Mysql = new Mysql.DatabaseClient(_Settings);
                    break;
                case DbTypes.Postgresql:
                    _Postgresql = new Postgresql.DatabaseClient(_Settings);
                    break;
                case DbTypes.SqlServer:
                    _SqlServer = new SqlServer.DatabaseClient(_Settings);
                    break;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
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
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.ListTables();
                case DbTypes.Postgresql:
                    return _Postgresql.ListTables();
                case DbTypes.Sqlite:
                    return _Sqlite.ListTables();
                case DbTypes.SqlServer:
                    return _SqlServer.ListTables();
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'."); 
            } 
        }

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if exists.</returns>
        public bool TableExists(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.TableExists(tableName);
                case DbTypes.Postgresql:
                    return _Postgresql.TableExists(tableName);
                case DbTypes.Sqlite:
                    return _Sqlite.TableExists(tableName);
                case DbTypes.SqlServer:
                    return _SqlServer.TableExists(tableName);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <returns>A list of column objects.</returns>
        public List<Column> DescribeTable(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.DescribeTable(tableName);
                case DbTypes.Postgresql:
                    return _Postgresql.DescribeTable(tableName);
                case DbTypes.Sqlite:
                    return _Sqlite.DescribeTable(tableName);
                case DbTypes.SqlServer:
                    return _SqlServer.DescribeTable(tableName);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <returns>Dictionary.  Key is table name, value is List of Column objects.</returns>
        public Dictionary<string, List<Column>> DescribeDatabase()
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.DescribeDatabase();
                case DbTypes.Postgresql:
                    return _Postgresql.DescribeDatabase();
                case DbTypes.Sqlite:
                    return _Sqlite.DescribeDatabase();
                case DbTypes.SqlServer:
                    return _SqlServer.DescribeDatabase();
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Create a table with a specified name.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        public void CreateTable(string tableName, List<Column> columns)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    _Mysql.CreateTable(tableName, columns);
                    return;
                case DbTypes.Postgresql:
                    _Postgresql.CreateTable(tableName, columns);
                    return;
                case DbTypes.Sqlite:
                    _Sqlite.CreateTable(tableName, columns);
                    return;
                case DbTypes.SqlServer:
                    _SqlServer.CreateTable(tableName, columns);
                    return;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Drop the specified table.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        public void DropTable(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    _Mysql.DropTable(tableName);
                    return;
                case DbTypes.Postgresql:
                    _Postgresql.DropTable(tableName);
                    return;
                case DbTypes.Sqlite:
                    _Sqlite.DropTable(tableName);
                    return;
                case DbTypes.SqlServer:
                    _SqlServer.DropTable(tableName);
                    return;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Retrieve the name of the primary key column from a specific table.
        /// </summary>
        /// <param name="tableName">The table of which you want the primary key.</param>
        /// <returns>A string containing the column name.</returns>
        public string GetPrimaryKeyColumn(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.GetPrimaryKeyColumn(tableName);
                case DbTypes.Postgresql:
                    return _Postgresql.GetPrimaryKeyColumn(tableName);
                case DbTypes.Sqlite:
                    return _Sqlite.GetPrimaryKeyColumn(tableName);
                case DbTypes.SqlServer:
                    return _SqlServer.GetPrimaryKeyColumn(tableName);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Retrieve a list of the names of columns from within a specific table.
        /// </summary>
        /// <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
        /// <returns>A list of strings containing the column names.</returns>
        public List<string> GetColumnNames(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.GetColumnNames(tableName);
                case DbTypes.Postgresql:
                    return _Postgresql.GetColumnNames(tableName);
                case DbTypes.Sqlite:
                    return _Sqlite.GetColumnNames(tableName);
                case DbTypes.SqlServer:
                    return _SqlServer.GetColumnNames(tableName);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
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
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.GetUniqueObjectById(tableName, columnName, value);
                case DbTypes.Postgresql:
                    return _Postgresql.GetUniqueObjectById(tableName, columnName, value);
                case DbTypes.Sqlite:
                    return _Sqlite.GetUniqueObjectById(tableName, columnName, value);
                case DbTypes.SqlServer:
                    return _SqlServer.GetUniqueObjectById(tableName, columnName, value);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
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
        public DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                case DbTypes.Postgresql:
                    return _Postgresql.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                case DbTypes.Sqlite:
                    return _Sqlite.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                case DbTypes.SqlServer:
                    return _SqlServer.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
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
        public DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                case DbTypes.Postgresql:
                    return _Postgresql.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                case DbTypes.Sqlite:
                    return _Sqlite.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                case DbTypes.SqlServer:
                    return _SqlServer.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Insert(tableName, keyValuePairs);
                case DbTypes.Postgresql:
                    return _Postgresql.Insert(tableName, keyValuePairs);
                case DbTypes.Sqlite:
                    return _Sqlite.Insert(tableName, keyValuePairs);
                case DbTypes.SqlServer:
                    return _SqlServer.Insert(tableName, keyValuePairs);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Execute an INSERT query with multiple values within a transaction.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        public void InsertMultiple(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    _Mysql.InsertMultiple(tableName, keyValuePairList);
                    return;
                case DbTypes.Postgresql:
                    _Postgresql.InsertMultiple(tableName, keyValuePairList);
                    return;
                case DbTypes.Sqlite:
                    _Sqlite.InsertMultiple(tableName, keyValuePairList);
                    return;
                case DbTypes.SqlServer:
                    _SqlServer.InsertMultiple(tableName, keyValuePairList);
                    return;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Execute an UPDATE query.
        /// For Microsoft SQL Server and PostgreSQL, the updated rows are returned.
        /// For MySQL and Sqlite, nothing is returned.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        /// <returns>For Microsoft SQL Server and PostgreSQL, a DataTable containing the results.  For MySQL and Sqlite, null.</returns>
        public DataTable Update(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    _Mysql.Update(tableName, keyValuePairs, filter);
                    return null;
                case DbTypes.Postgresql:
                    return _Postgresql.Update(tableName, keyValuePairs, filter);
                case DbTypes.Sqlite:
                    _Sqlite.Update(tableName, keyValuePairs, filter);
                    return null;
                case DbTypes.SqlServer:
                    return _SqlServer.Update(tableName, keyValuePairs, filter);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        public void Delete(string tableName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    _Mysql.Delete(tableName, filter);
                    return;
                case DbTypes.Postgresql:
                    _Postgresql.Delete(tableName, filter);
                    return;
                case DbTypes.Sqlite:
                    _Sqlite.Delete(tableName, filter);
                    return;
                case DbTypes.SqlServer:
                    _SqlServer.Delete(tableName, filter);
                    return;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Empties a table completely.
        /// </summary>
        /// <param name="tableName">The table you wish to TRUNCATE.</param>
        public void Truncate(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    _Mysql.Truncate(tableName);
                    return;
                case DbTypes.Postgresql:
                    _Postgresql.Truncate(tableName);
                    return;
                case DbTypes.Sqlite:
                    _Sqlite.Truncate(tableName);
                    return;
                case DbTypes.SqlServer:
                    _SqlServer.Truncate(tableName);
                    return;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Execute a query.
        /// </summary>
        /// <param name="query">Database query defined outside of the database client.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Query(string query)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Query(query);
                case DbTypes.Postgresql:
                    return _Postgresql.Query(query);
                case DbTypes.Sqlite:
                    return _Sqlite.Query(query);
                case DbTypes.SqlServer:
                    return _SqlServer.Query(query);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Determine if records exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>True if records exist.</returns>
        public bool Exists(string tableName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Exists(tableName, filter);
                case DbTypes.Postgresql:
                    return _Postgresql.Exists(tableName, filter);
                case DbTypes.Sqlite:
                    return _Sqlite.Exists(tableName, filter);
                case DbTypes.SqlServer:
                    return _SqlServer.Exists(tableName, filter);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Determine the number of records that exist by filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The number of records.</returns>
        public long Count(string tableName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Count(tableName, filter);
                case DbTypes.Postgresql:
                    return _Postgresql.Count(tableName, filter);
                case DbTypes.Sqlite:
                    return _Sqlite.Count(tableName, filter);
                case DbTypes.SqlServer:
                    return _SqlServer.Count(tableName, filter);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Determine the sum of a column for records that match the supplied filter.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="fieldName">The name of the field.</param>
        /// <param name="filter">Expression.</param>
        /// <returns>The sum of the specified column from the matching rows.</returns>
        public decimal Sum(string tableName, string fieldName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Sum(tableName, fieldName, filter);
                case DbTypes.Postgresql:
                    return _Postgresql.Sum(tableName, fieldName, filter);
                case DbTypes.Sqlite:
                    return _Sqlite.Sum(tableName, fieldName, filter);
                case DbTypes.SqlServer:
                    return _SqlServer.Sum(tableName, fieldName, filter);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Create a string timestamp from the given DateTime for the database of the instance type.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>A string with timestamp formatted for the database of the instance type.</returns>
        public string Timestamp(DateTime ts)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.Timestamp(ts);
                case DbTypes.Postgresql:
                    return _Postgresql.Timestamp(ts);
                case DbTypes.Sqlite:
                    return _Sqlite.Timestamp(ts);
                case DbTypes.SqlServer:
                    return _SqlServer.Timestamp(ts);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Create a string timestamp with offset from the given DateTimeOffset for the database of the instance type.
        /// </summary>
        /// <param name="ts">DateTimeOffset.</param>
        /// <returns>A string with timestamp and offset formatted for the database of the instance type.</returns>
        public string TimestampOffset(DateTimeOffset ts)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.TimestampOffset(ts);
                case DbTypes.Postgresql:
                    return _Postgresql.TimestampOffset(ts);
                case DbTypes.Sqlite:
                    return _Sqlite.TimestampOffset(ts);
                case DbTypes.SqlServer:
                    return _SqlServer.TimestampOffset(ts);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Sanitize an input string.
        /// </summary>
        /// <param name="s">The value to sanitize.</param>
        /// <returns>A sanitized string.</returns>
        public string SanitizeString(string s)
        {
            switch (_Settings.Type)
            {
                case DbTypes.Mysql:
                    return _Mysql.SanitizeString(s);
                case DbTypes.Postgresql:
                    return _Postgresql.SanitizeString(s);
                case DbTypes.Sqlite:
                    return _Sqlite.SanitizeString(s);
                case DbTypes.SqlServer:
                    return _SqlServer.SanitizeString(s);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
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
                if (_Mysql != null) 
                    _Mysql.Dispose();
                else if (_Postgresql != null) 
                    _Postgresql.Dispose();
                else if (_Sqlite != null) 
                    _Sqlite.Dispose();
                else if (_SqlServer != null) 
                    _SqlServer.Dispose();
                else
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }

            _Disposed = true;
        }

        #endregion

        #region Public-Static-Methods

        /// <summary>
        /// Convert a DateTime to a string formatted for the specified database type.
        /// </summary>
        /// <param name="dbType">The type of database.</param>
        /// <param name="ts">The timestamp.</param>
        /// <returns>A string formatted for use with the specified database.</returns>
        public static string DbTimestamp(DbTypes dbType, DateTime ts)
        {
            switch (dbType)
            {
                case DbTypes.Mysql:
                    return Mysql.DatabaseClient.DbTimestamp(ts);
                case DbTypes.Postgresql:
                    return Postgresql.DatabaseClient.DbTimestamp(ts);
                case DbTypes.Sqlite:
                    return Sqlite.DatabaseClient.DbTimestamp(ts);
                case DbTypes.SqlServer:
                    return SqlServer.DatabaseClient.DbTimestamp(ts);
                default:
                    return null;
            }
        }

        /// <summary>
        /// Convert a DateTimeOffset to a string formatted for the specified database type.
        /// </summary>
        /// <param name="dbType">The type of database.</param>
        /// <param name="ts">The timestamp with offset.</param>
        /// <returns>A string formatted for use with the specified database.</returns>
        public static string DbTimestampOffset(DbTypes dbType, DateTimeOffset ts)
        {
            switch (dbType)
            {
                case DbTypes.Mysql:
                    return Mysql.DatabaseClient.DbTimestampOffset(ts);
                case DbTypes.Postgresql:
                    return Postgresql.DatabaseClient.DbTimestampOffset(ts);
                case DbTypes.Sqlite:
                    return Sqlite.DatabaseClient.DbTimestampOffset(ts);
                case DbTypes.SqlServer:
                    return SqlServer.DatabaseClient.DbTimestampOffset(ts);
                default:
                    return null;
            }
        }

        #endregion
    }
}
