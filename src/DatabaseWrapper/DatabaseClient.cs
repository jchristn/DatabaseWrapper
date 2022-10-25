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
    public class DatabaseClient : DatabaseClientBase, IDisposable
    {
        #region Public-Members

        /// <summary>
        /// The connection string used to connect to the database server.
        /// </summary>
        public new string ConnectionString 
        { 
            get
            {
                switch (_Settings.Type)
                {
                    case DbTypeEnum.SqlServer:
                        return _SqlServer.ConnectionString;
                    case DbTypeEnum.Mysql:
                        return _Mysql.ConnectionString;
                    case DbTypeEnum.Postgresql:
                        return _Postgresql.ConnectionString;
                    case DbTypeEnum.Sqlite:
                        return _Sqlite.ConnectionString;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
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
                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        return _Mysql.TimestampFormat;
                    case DbTypeEnum.Postgresql:
                        return _Postgresql.TimestampFormat;
                    case DbTypeEnum.Sqlite:
                        return _Sqlite.TimestampFormat;
                    case DbTypeEnum.SqlServer:
                        return _SqlServer.TimestampFormat;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
            set
            {
                if (String.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(TimestampFormat));
                
                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        _Mysql.TimestampFormat = value;
                        break;
                    case DbTypeEnum.Postgresql:
                        _Postgresql.TimestampFormat = value;
                        break;
                    case DbTypeEnum.Sqlite:
                        _Sqlite.TimestampFormat = value;
                        break;
                    case DbTypeEnum.SqlServer:
                        _SqlServer.TimestampFormat = value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
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
                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        return _Mysql.TimestampOffsetFormat;
                    case DbTypeEnum.Postgresql:
                        return _Postgresql.TimestampOffsetFormat;
                    case DbTypeEnum.Sqlite:
                        return _Sqlite.TimestampOffsetFormat;
                    case DbTypeEnum.SqlServer:
                        return _SqlServer.TimestampOffsetFormat;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
            set
            {
                if (String.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(TimestampOffsetFormat));

                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        _Mysql.TimestampOffsetFormat = value;
                        break;
                    case DbTypeEnum.Postgresql:
                        _Postgresql.TimestampOffsetFormat = value;
                        break;
                    case DbTypeEnum.Sqlite:
                        _Sqlite.TimestampOffsetFormat = value;
                        break;
                    case DbTypeEnum.SqlServer:
                        _SqlServer.TimestampOffsetFormat = value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        /// <summary>
        /// Maximum supported statement length.
        /// </summary>
        public new int MaxStatementLength
        {
            get
            {
                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        return _Mysql.MaxStatementLength;
                    case DbTypeEnum.Postgresql:
                        return _Postgresql.MaxStatementLength;
                    case DbTypeEnum.Sqlite:
                        return _Sqlite.MaxStatementLength;
                    case DbTypeEnum.SqlServer:
                        return _SqlServer.MaxStatementLength;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        /// <summary>
        /// Settings.
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

                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        _Mysql.Settings = value;
                        break;
                    case DbTypeEnum.Postgresql:
                        _Postgresql.Settings = value;
                        break;
                    case DbTypeEnum.Sqlite:
                        _Sqlite.Settings = value;
                        break;
                    case DbTypeEnum.SqlServer:
                        _SqlServer.Settings = value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
        }

        /// <summary>
        /// Event to fire when a query is handled.
        /// </summary>
        public new event EventHandler<DatabaseQueryEvent> QueryEvent
        {
            add
            {
                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        _Mysql.QueryEvent += value;
                        break;
                    case DbTypeEnum.Postgresql:
                        _Postgresql.QueryEvent += value;
                        break;
                    case DbTypeEnum.Sqlite:
                        _Sqlite.QueryEvent += value;
                        break;
                    case DbTypeEnum.SqlServer:
                        _SqlServer.QueryEvent += value;
                        break;
                    default:
                        throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }
            }
            remove
            {
                switch (_Settings.Type)
                {
                    case DbTypeEnum.Mysql:
                        _Mysql.QueryEvent -= value;
                        break;
                    case DbTypeEnum.Postgresql:
                        _Postgresql.QueryEvent -= value;
                        break;
                    case DbTypeEnum.Sqlite:
                        _Sqlite.QueryEvent -= value;
                        break;
                    case DbTypeEnum.SqlServer:
                        _SqlServer.QueryEvent -= value;
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

            if (_Settings.Type == DbTypeEnum.Sqlite && String.IsNullOrEmpty(_Settings.Filename))
                throw new ArgumentException("Filename must be populated in database settings of type 'Sqlite'.");

            if (_Settings.Type != DbTypeEnum.SqlServer && !String.IsNullOrEmpty(_Settings.Instance))
                throw new ArgumentException("Instance can only be used in database settings of type 'SqlServer'.");

            _Header = "[DatabaseWrapper." + _Settings.Type.ToString() + "] ";

            switch (_Settings.Type)
            {
                case DbTypeEnum.Sqlite:
                    _Sqlite = new Sqlite.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.Mysql:
                    _Mysql = new Mysql.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.Postgresql:
                    _Postgresql = new Postgresql.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.SqlServer:
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
            DbTypeEnum dbType,
            string serverIp,
            int serverPort,
            string username,
            string password,
            string instance,
            string database)
        {
            if (dbType == DbTypeEnum.Sqlite) throw new ArgumentException("Use the filename constructor for Sqlite databases.");
            if (String.IsNullOrEmpty(serverIp)) throw new ArgumentNullException(nameof(serverIp));
            if (serverPort < 0) throw new ArgumentOutOfRangeException(nameof(serverPort));
            if (String.IsNullOrEmpty(database)) throw new ArgumentNullException(nameof(database));

            if (dbType == DbTypeEnum.SqlServer)
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
                case DbTypeEnum.Sqlite:
                    throw new ArgumentException("Unable to use this constructor with 'DbTypeEnum.Sqlite'.");
                case DbTypeEnum.Mysql:
                    _Mysql = new Mysql.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.Postgresql:
                    _Postgresql = new Postgresql.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.SqlServer:
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
            DbTypeEnum dbType,
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
                case DbTypeEnum.Sqlite:
                    throw new ArgumentException("Unable to use this constructor with 'DbTypeEnum.Sqlite'.");
                case DbTypeEnum.Mysql:
                    _Mysql = new Mysql.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.Postgresql:
                    _Postgresql = new Postgresql.DatabaseClient(_Settings);
                    break;
                case DbTypeEnum.SqlServer:
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
        public override List<string> ListTables()
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.ListTables();
                case DbTypeEnum.Postgresql:
                    return _Postgresql.ListTables();
                case DbTypeEnum.Sqlite:
                    return _Sqlite.ListTables();
                case DbTypeEnum.SqlServer:
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
        public override bool TableExists(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.TableExists(tableName);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.TableExists(tableName);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.TableExists(tableName);
                case DbTypeEnum.SqlServer:
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
        public override List<Column> DescribeTable(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.DescribeTable(tableName);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.DescribeTable(tableName);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.DescribeTable(tableName);
                case DbTypeEnum.SqlServer:
                    return _SqlServer.DescribeTable(tableName);
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <returns>Dictionary.  Key is table name, value is List of Column objects.</returns>
        public override Dictionary<string, List<Column>> DescribeDatabase()
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.DescribeDatabase();
                case DbTypeEnum.Postgresql:
                    return _Postgresql.DescribeDatabase();
                case DbTypeEnum.Sqlite:
                    return _Sqlite.DescribeDatabase();
                case DbTypeEnum.SqlServer:
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
        public override void CreateTable(string tableName, List<Column> columns)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    _Mysql.CreateTable(tableName, columns);
                    return;
                case DbTypeEnum.Postgresql:
                    _Postgresql.CreateTable(tableName, columns);
                    return;
                case DbTypeEnum.Sqlite:
                    _Sqlite.CreateTable(tableName, columns);
                    return;
                case DbTypeEnum.SqlServer:
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
        public override void DropTable(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    _Mysql.DropTable(tableName);
                    return;
                case DbTypeEnum.Postgresql:
                    _Postgresql.DropTable(tableName);
                    return;
                case DbTypeEnum.Sqlite:
                    _Sqlite.DropTable(tableName);
                    return;
                case DbTypeEnum.SqlServer:
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
        public override string GetPrimaryKeyColumn(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.GetPrimaryKeyColumn(tableName);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.GetPrimaryKeyColumn(tableName);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.GetPrimaryKeyColumn(tableName);
                case DbTypeEnum.SqlServer:
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
        public override List<string> GetColumnNames(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.GetColumnNames(tableName);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.GetColumnNames(tableName);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.GetColumnNames(tableName);
                case DbTypeEnum.SqlServer:
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
        public override DataTable GetUniqueObjectById(string tableName, string columnName, object value)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.GetUniqueObjectById(tableName, columnName, value);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.GetUniqueObjectById(tableName, columnName, value);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.GetUniqueObjectById(tableName, columnName, value);
                case DbTypeEnum.SqlServer:
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
        public override DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Select(tableName, indexStart, maxResults, returnFields, filter, null);
                case DbTypeEnum.SqlServer:
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
        public override DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Select(tableName, indexStart, maxResults, returnFields, filter, resultOrder);
                case DbTypeEnum.SqlServer:
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
        public override DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Insert(tableName, keyValuePairs);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Insert(tableName, keyValuePairs);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Insert(tableName, keyValuePairs);
                case DbTypeEnum.SqlServer:
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
        public override void InsertMultiple(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    _Mysql.InsertMultiple(tableName, keyValuePairList);
                    return;
                case DbTypeEnum.Postgresql:
                    _Postgresql.InsertMultiple(tableName, keyValuePairList);
                    return;
                case DbTypeEnum.Sqlite:
                    _Sqlite.InsertMultiple(tableName, keyValuePairList);
                    return;
                case DbTypeEnum.SqlServer:
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
        public override void Update(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    _Mysql.Update(tableName, keyValuePairs, filter);
                    return;
                case DbTypeEnum.Postgresql:
                    _Postgresql.Update(tableName, keyValuePairs, filter);
                    return;
                case DbTypeEnum.Sqlite:
                    _Sqlite.Update(tableName, keyValuePairs, filter);
                    return;
                case DbTypeEnum.SqlServer:
                    _SqlServer.Update(tableName, keyValuePairs, filter);
                    return;
                default:
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
            }
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param> 
        public override void Delete(string tableName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    _Mysql.Delete(tableName, filter);
                    return;
                case DbTypeEnum.Postgresql:
                    _Postgresql.Delete(tableName, filter);
                    return;
                case DbTypeEnum.Sqlite:
                    _Sqlite.Delete(tableName, filter);
                    return;
                case DbTypeEnum.SqlServer:
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
        public override void Truncate(string tableName)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    _Mysql.Truncate(tableName);
                    return;
                case DbTypeEnum.Postgresql:
                    _Postgresql.Truncate(tableName);
                    return;
                case DbTypeEnum.Sqlite:
                    _Sqlite.Truncate(tableName);
                    return;
                case DbTypeEnum.SqlServer:
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
        public override DataTable Query(string query)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Query(query);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Query(query);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Query(query);
                case DbTypeEnum.SqlServer:
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
        public override bool Exists(string tableName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Exists(tableName, filter);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Exists(tableName, filter);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Exists(tableName, filter);
                case DbTypeEnum.SqlServer:
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
        public override long Count(string tableName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Count(tableName, filter);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Count(tableName, filter);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Count(tableName, filter);
                case DbTypeEnum.SqlServer:
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
        public override decimal Sum(string tableName, string fieldName, Expr filter)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Sum(tableName, fieldName, filter);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Sum(tableName, fieldName, filter);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Sum(tableName, fieldName, filter);
                case DbTypeEnum.SqlServer:
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
        public override string Timestamp(DateTime ts)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.Timestamp(ts);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.Timestamp(ts);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.Timestamp(ts);
                case DbTypeEnum.SqlServer:
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
        public override string TimestampOffset(DateTimeOffset ts)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.TimestampOffset(ts);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.TimestampOffset(ts);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.TimestampOffset(ts);
                case DbTypeEnum.SqlServer:
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
        public override string SanitizeString(string s)
        {
            switch (_Settings.Type)
            {
                case DbTypeEnum.Mysql:
                    return _Mysql.SanitizeString(s);
                case DbTypeEnum.Postgresql:
                    return _Postgresql.SanitizeString(s);
                case DbTypeEnum.Sqlite:
                    return _Sqlite.SanitizeString(s);
                case DbTypeEnum.SqlServer:
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
                if (_Mysql != null)
                {
                    _Mysql.Dispose();
                }
                else if (_Postgresql != null)
                {
                    _Postgresql.Dispose();
                }
                else if (_Sqlite != null)
                {
                    _Sqlite.Dispose();
                }
                else if (_SqlServer != null)
                {
                    _SqlServer.Dispose();
                }
                else
                {
                    throw new ArgumentException("Unknown database type '" + _Settings.Type.ToString() + "'.");
                }

                _Disposed = true;
            }
        }

        #endregion
    }
}
