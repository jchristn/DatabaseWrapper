using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql;
using MySql.Data.MySqlClient;

namespace DatabaseWrapper
{
    public class DatabaseClient
    {
        #region Constructor

        public DatabaseClient(
            DbTypes dbType,
            string serverIp,
            int serverPort,
            string username,
            string password,
            string instance,
            string database)
        {
            //
            // MsSql, MySql, and PostgreSql will use server IP, port, username, password, database
            // Sqlite will use just database and it should refer to the database file
            //
            if (String.IsNullOrEmpty(serverIp)) throw new ArgumentNullException(nameof(serverIp));
            if (serverPort < 0) throw new ArgumentOutOfRangeException(nameof(serverPort));
            if (String.IsNullOrEmpty(database)) throw new ArgumentNullException(nameof(database));
            
            DbType = dbType;
            ServerIp = serverIp;
            ServerPort = serverPort;
            Username = username;
            Password = password;
            Instance = instance;
            Database = database;

            if (!PopulateConnectionString())
            {
                throw new Exception("Unable to build connection string");
            }

            if (!LoadTableNames())
            {
                throw new Exception("Unable to load table names");
            }

            if (!LoadTableDetails())
            {
                throw new Exception("Unable to load table details from " + ServerIp + ":" + ServerPort + " " + Instance + " " + Database + " using username " + Username);
            }
        }

        #endregion
        
        #region Public-Members

        public string ConnectionString;
        public bool DebugRawQuery = false;
        public bool DebugResultRowCount = false;

        #endregion

        #region Private-Members

        private DbTypes DbType;
        private string ServerIp;
        private int ServerPort;
        private string Username;
        private string Password;
        private string Instance;
        private string Database;

        private readonly object LoadingTablesLock = new object();
        private ConcurrentList<string> TableNames = new ConcurrentList<string>();
        private ConcurrentDictionary<string, List<Column>> TableDetails = new ConcurrentDictionary<string, List<Column>>();

        Random random = new Random();

        #endregion

        #region Public-Methods

        public List<string> ListTables()
        {
            List<string> ret = new List<string>();
            if (TableNames != null && TableNames.Count > 0)
            {
                foreach (string curr in TableNames)
                {
                    ret.Add(curr);
                }
            }
            return ret;
        }

        public List<Column> DescribeTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details;
            if (TableDetails.TryGetValue(tableName, out details))
            {
                return details;
            }
            else
            {
                throw new Exception("Table " + tableName + " is not in the tables list");
            }
        }

        public string GetPrimaryKeyColumn(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details;
            if (TableDetails.TryGetValue(tableName, out details))
            {
                if (details != null && details.Count > 0)
                {
                    foreach (Column c in details)
                    {
                        if (c.IsPrimaryKey) return c.Name;
                    }
                }

                throw new Exception("Unable to find primary key for table " + tableName);
            }
            else
            {
                throw new Exception("Table " + tableName + " is not in the tables list");
            }
        }

        public List<string> GetColumnNames(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details;
            List<string> columnNames = new List<string>();

            if (TableDetails.TryGetValue(tableName, out details))
            {
                if (details != null && details.Count > 0)
                {
                    foreach (Column c in details)
                    {
                        columnNames.Add(c.Name);
                    }

                    return columnNames;
                }

                throw new Exception("Unable to find primary key for table " + tableName);
            }
            else
            {
                throw new Exception("Table " + tableName + " is not in the tables list");
            }
        }

        public DataTable Select(string tableName, int maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (maxResults < 0) throw new ArgumentOutOfRangeException(nameof(maxResults));

            string query = "";
            string whereClause = "";
            DataTable result;
            List<Column> tableDetails = DescribeTable(tableName);

            switch (DbType)
            {
                case DbTypes.MsSql:
                    #region MsSql

                    //
                    // SELECT and TOP
                    //
                    if (maxResults > 0) query += "SELECT TOP " + maxResults + " ";
                    else query += "SELECT ";

                    //
                    // fields
                    //
                    if (returnFields == null || returnFields.Count < 1) query += "* ";
                    else
                    {
                        int fieldsAdded = 0;
                        foreach (string curr in returnFields)
                        {
                            if (fieldsAdded == 0)
                            {
                                query += SanitizeString(curr);
                                fieldsAdded++;
                            }
                            else
                            {
                                query += "," + SanitizeString(curr);
                                fieldsAdded++;
                            }
                        }
                    }
                    query += " ";

                    //
                    // table
                    //
                    query += "FROM " + tableName + " ";

                    //
                    // expressions
                    //
                    if (filter != null)
                    {
                        whereClause = filter.ToWhereClause();
                    }
                    if (!String.IsNullOrEmpty(whereClause))
                    {
                        query += "WHERE " + whereClause + " ";
                    }

                    // 
                    // order clause
                    //
                    if (!String.IsNullOrEmpty(orderByClause)) query += orderByClause;
                    break;

                    #endregion

                case DbTypes.MySql:
                    #region MySql

                    //
                    // SELECT
                    //
                    query += "SELECT ";

                    //
                    // fields
                    //
                    if (returnFields == null || returnFields.Count < 1) query += "* ";
                    else
                    {
                        int fieldsAdded = 0;
                        foreach (string curr in returnFields)
                        {
                            if (fieldsAdded == 0)
                            {
                                query += SanitizeString(curr);
                                fieldsAdded++;
                            }
                            else
                            {
                                query += "," + SanitizeString(curr);
                                fieldsAdded++;
                            }
                        }
                    }
                    query += " ";

                    //
                    // table
                    //
                    query += "FROM " + tableName + " ";

                    //
                    // expressions
                    //
                    if (filter != null)
                    {
                        whereClause = filter.ToWhereClause();
                    }
                    if (!String.IsNullOrEmpty(whereClause))
                    {
                        query += "WHERE " + whereClause + " ";
                    }

                    // 
                    // order clause
                    //
                    if (!String.IsNullOrEmpty(orderByClause)) query += orderByClause;

                    //
                    // limit
                    //
                    if (maxResults > 0)
                    {
                        query += "LIMIT " + maxResults;
                    }

                    break;

                    #endregion
            }

            result = RawQuery(query);
            return result;
        }

        public DataTable Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            string keys = "";
            string values = "";
            string query = "";
            int insertedId = 0;
            string retrievalQuery = "";
            DataTable result;
            List<Column> tableDetails = DescribeTable(tableName);
            List<string> columnNames = GetColumnNames(tableName);
            string primaryKeyColumn = GetPrimaryKeyColumn(tableName);

            #region Build-Key-Value-Pairs

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue;
                if (!columnNames.Contains(curr.Key))
                {
                    throw new ArgumentException("Column " + curr.Key + " does not exist in table " + tableName);
                }

                if (added == 0)
                {
                    keys += curr.Key;
                    if (curr.Value != null) values += "'" + SanitizeString(curr.Value.ToString()) + "'";
                    else values += "null";
                }
                else
                {
                    keys += "," + curr.Key;
                    if (curr.Value != null) values += ",'" + SanitizeString(curr.Value.ToString()) + "'";
                    else values += ",null";
                }
                added++;
            }

            #endregion

            #region Build-INSERT-Query-and-Submit

            switch (DbType)
            {
                case DbTypes.MsSql:
                    #region MsSql

                    query += "INSERT INTO " + tableName + " WITH (ROWLOCK) ";
                    query += "(" + keys + ") ";
                    query += "OUTPUT INSERTED.* ";
                    query += "VALUES ";
                    query += "(" + values + ") ";
                    break;

                    #endregion

                case DbTypes.MySql:
                    #region MySql

                    //
                    // insert into
                    //
                    query += "START TRANSACTION; ";
                    query += "INSERT INTO " + tableName + " ";
                    query += "(" + keys + ") ";
                    query += "VALUES ";
                    query += "(" + values + "); ";
                    query += "SELECT LAST_INSERT_ID() AS id; ";
                    query += "COMMIT; ";
                    break;

                    #endregion
            }

            result = RawQuery(query);

            #endregion

            #region Post-Retrieval

            switch (DbType)
            {
                case DbTypes.MsSql:
                    #region MsSql

                    //
                    // built into the query
                    //
                    break;

                    #endregion

                case DbTypes.MySql:
                    #region MySql

                    if (!Helper.DataTableIsNullOrEmpty(result))
                    {
                        bool idFound = false;

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
                            retrievalQuery = "SELECT * FROM " + tableName + " WHERE " + primaryKeyColumn + "=" + insertedId;
                            result = RawQuery(retrievalQuery);
                        }
                    }
                    break;

                    #endregion
            }

            #endregion

            return result;
        }
        
        public DataTable Update(string tableName, Dictionary<string, object> keyValuePairs, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            string query = "";
            string keyValueClause = "";
            DataTable result;
            List<Column> tableDetails = DescribeTable(tableName);
            List<string> columnNames = GetColumnNames(tableName);
            string primaryKeyColumn = GetPrimaryKeyColumn(tableName);

            #region Build-Key-Value-Clause

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue;
                if (!columnNames.Contains(curr.Key))
                {
                    throw new ArgumentException("Column " + curr.Key + " does not exist in table " + tableName);
                }

                if (added == 0)
                {
                    if (curr.Value != null) keyValueClause += curr.Key + "='" + SanitizeString(curr.Value.ToString()) + "'";
                    else keyValueClause += curr.Key + "= null";
                }
                else
                {
                    if (curr.Value != null) keyValueClause += "," + curr.Key + "='" + SanitizeString(curr.Value.ToString()) + "'";
                    else keyValueClause += "," + curr.Key + "= null";
                }
                added++;
            }

            #endregion

            #region Build-UPDATE-Query-and-Submit

            switch (DbType)
            {
                case DbTypes.MsSql:
                    #region MsSql

                    query += "UPDATE " + tableName + " WITH (ROWLOCK) SET ";
                    query += keyValueClause + " ";
                    query += "OUTPUT INSERTED.* ";
                    if (filter != null) query += "WHERE " + filter.ToWhereClause() + " ";
                    
                    break;

                    #endregion

                case DbTypes.MySql:
                    #region MySql

                    query += "UPDATE " + tableName + " SET ";
                    query += keyValueClause + " ";
                    if (filter != null) query += "WHERE " + filter.ToWhereClause() + " ";
                    break;

                    #endregion
            }

            result = RawQuery(query);

            #endregion

            return result;
        }

        public DataTable Delete(string tableName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (filter == null) throw new ArgumentNullException(nameof(filter));

            string query = "";
            DataTable result;
            List<Column> tableDetails = DescribeTable(tableName);
            List<string> columnNames = GetColumnNames(tableName);
            string primaryKeyColumn = GetPrimaryKeyColumn(tableName);
            
            #region Build-DELETE-Query-and-Submit

            switch (DbType)
            {
                case DbTypes.MsSql:
                    #region MsSql

                    query += "DELETE FROM " + tableName + " WITH (ROWLOCK) ";
                    if (filter != null) query += "WHERE " + filter.ToWhereClause() + " ";
                    break;

                #endregion

                case DbTypes.MySql:
                    #region MySql

                    query += "DELETE FROM " + tableName + " ";
                    if (filter != null) query += "WHERE " + filter.ToWhereClause() + " ";
                    break;

                    #endregion
            }

            result = RawQuery(query);

            #endregion

            return result;
        }

        #endregion

        #region Private-Methods
        
        private bool LoadTableNames()
        {
            lock (LoadingTablesLock)
            {
                string query = "";
                DataTable result = new DataTable();

                #region Build-Query

                switch (DbType)
                {
                    case DbTypes.MsSql:
                        query = "SELECT TABLE_NAME FROM " + Database + ".INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
                        break;

                    case DbTypes.MySql:
                        query = "SHOW TABLES";
                        break;
                }

                #endregion

                #region Process-Results

                result = RawQuery(query);
                List<string> tableNames = new List<string>();

                if (result != null && result.Rows.Count > 0)
                {
                    switch (DbType)
                    {
                        case DbTypes.MsSql:
                            foreach (DataRow curr in result.Rows)
                            {
                                tableNames.Add(curr["TABLE_NAME"].ToString());
                            }
                            break;

                        case DbTypes.MySql:
                            foreach (DataRow curr in result.Rows)
                            {
                                tableNames.Add(curr["Tables_in_" + Database].ToString());
                            }
                            break;
                    }
                }

                if (tableNames != null && tableNames.Count > 0)
                {
                    TableNames = new ConcurrentList<string>();
                    foreach (string curr in tableNames)
                    {
                        TableNames.Add(curr);
                    }
                }

                #endregion

                return true;
            }
        }

        private bool LoadTableDetails()
        {
            lock (LoadingTablesLock)
            {
                string query = "";
                DataTable result = new DataTable();
                Dictionary<string, List<Column>> tableDetails = new Dictionary<string, List<Column>>();

                foreach (string currTable in TableNames)
                {
                    #region Gather-Schema

                    List<Column> columns = new List<Column>();

                    switch (DbType)
                    {
                        case DbTypes.MsSql:
                            query =
                                "SELECT " +
                                "  col.TABLE_NAME, col.COLUMN_NAME, col.IS_NULLABLE, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, con.CONSTRAINT_NAME " +
                                "FROM INFORMATION_SCHEMA.COLUMNS col " +
                                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME " +
                                "WHERE col.TABLE_NAME='" + currTable + "'";
                            break;

                        case DbTypes.MySql:
                            query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + currTable + "'";
                            break;
                    }

                    #endregion

                    #region Process-Schema

                    result = RawQuery(query);
                    if (result != null && result.Rows.Count > 0)
                    {
                        foreach (DataRow currColumn in result.Rows)
                        {
                            #region Process-Each-Column

                            /*
                            public bool IsPrimaryKey;
                            public string Name;
                            public string DataType;
                            public int? MaxLength;
                            public bool Nullable;
                            */
                            Column tempColumn = new Column();
                            int maxLength = 0;

                            switch (DbType)
                            {
                                case DbTypes.MsSql:
                                    tempColumn.Name = currColumn["COLUMN_NAME"].ToString();
                                    if (currColumn["CONSTRAINT_NAME"].ToString().StartsWith("PK_")) tempColumn.IsPrimaryKey = true;
                                    else tempColumn.IsPrimaryKey = false;
                                    tempColumn.DataType = currColumn["DATA_TYPE"].ToString();
                                    if (!Int32.TryParse(currColumn["CHARACTER_MAXIMUM_LENGTH"].ToString(), out maxLength)) { tempColumn.MaxLength = null; }
                                    else tempColumn.MaxLength = maxLength;
                                    if (String.Compare(currColumn["IS_NULLABLE"].ToString(), "YES") == 0) tempColumn.Nullable = true;
                                    else tempColumn.Nullable = false;
                                    break;

                                case DbTypes.MySql:
                                    tempColumn.Name = currColumn["COLUMN_NAME"].ToString();
                                    if (String.Compare(currColumn["COLUMN_KEY"].ToString(), "PRI") == 0) tempColumn.IsPrimaryKey = true;
                                    else tempColumn.IsPrimaryKey = false;
                                    tempColumn.DataType = currColumn["DATA_TYPE"].ToString();
                                    if (!Int32.TryParse(currColumn["CHARACTER_MAXIMUM_LENGTH"].ToString(), out maxLength)) { tempColumn.MaxLength = null; }
                                    else tempColumn.MaxLength = maxLength;
                                    if (String.Compare(currColumn["IS_NULLABLE"].ToString(), "YES") == 0) tempColumn.Nullable = true;
                                    else tempColumn.Nullable = false;
                                    break;
                            }

                            columns.Add(tempColumn);

                            #endregion
                        }

                        tableDetails.Add(currTable, columns);
                    }

                    #endregion
                }

                #region Replace-Table-Details

                TableDetails = new ConcurrentDictionary<string, List<Column>>();
                foreach (KeyValuePair<string, List<Column>> curr in tableDetails)
                {
                    TableDetails.TryAdd(curr.Key, curr.Value);
                }

                #endregion

                return true;
            }
        }

        private bool PopulateConnectionString()
        {
            ConnectionString = "";

            switch (DbType)
            {
                case DbTypes.MsSql:
                    //
                    // http://www.connectionstrings.com/sql-server/
                    //
                    if (String.IsNullOrEmpty(Username) && String.IsNullOrEmpty(Password))
                    {
                        ConnectionString += "Data Source=" + ServerIp;
                        if (!String.IsNullOrEmpty(Instance)) ConnectionString += "\\" + Instance + "; ";
                        else ConnectionString += "; ";
                        ConnectionString += "Integrated Security=SSPI; ";
                        ConnectionString += "Initial Catalog=" + Database + "; ";                            
                    }
                    else
                    {
                        if (ServerPort > 0)
                        {
                            if (String.IsNullOrEmpty(Instance)) ConnectionString += "Server=" + ServerIp + "," + ServerPort + "; ";
                            else ConnectionString += "Server=" + ServerIp + "\\" + Instance + "," + ServerPort + "; ";
                        }
                        else
                        {
                            if (String.IsNullOrEmpty(Instance)) ConnectionString += "Server=" + ServerIp + "; ";
                            else ConnectionString += "Server=" + ServerIp + "\\" + Instance + "; ";
                        }

                        ConnectionString += "Database=" + Database + "; ";
                        if (!String.IsNullOrEmpty(Username)) ConnectionString += "User ID=" + Username + "; ";
                        if (!String.IsNullOrEmpty(Password)) ConnectionString += "Password=" + Password + "; ";
                    }
                    break;

                case DbTypes.MySql:
                    //
                    // http://www.connectionstrings.com/mysql/
                    //
                    // MySQL does not use 'Instance'
                    ConnectionString += "Server=" + ServerIp + "; ";
                    if (ServerPort > 0) ConnectionString += "Port=" + ServerPort + "; ";
                    ConnectionString += "Database=" + Database + "; ";
                    if (!String.IsNullOrEmpty(Username)) ConnectionString += "Uid=" + Username + "; ";
                    if (!String.IsNullOrEmpty(Password)) ConnectionString += "Pwd=" + Password + "; ";
                    break;
            }

            return true;
        }

        private DataTable RawQuery(string query)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(query);
            DataTable result = new DataTable();

            if (DebugRawQuery) Console.WriteLine("RawQuery: " + query);

            switch (DbType)
            {
                case DbTypes.MsSql:
                    using (SqlConnection conn = new SqlConnection(ConnectionString))
                    {
                        conn.Open();
                        SqlDataAdapter sda = new SqlDataAdapter(query, conn);
                        sda.Fill(result);
                        conn.Dispose();
                        conn.Close();
                    }
                    break;

                case DbTypes.MySql:
                    using (MySqlConnection conn = new MySqlConnection(ConnectionString))
                    {
                        conn.Open();
                        MySqlCommand cmd = new MySqlCommand();
                        cmd.Connection = conn;
                        cmd.CommandText = query;
                        MySqlDataAdapter sda = new MySqlDataAdapter(cmd);
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
                        conn.Close();
                    }
                    break;
            }

            if (DebugResultRowCount)
            {
                if (result != null) Console.WriteLine("RawQuery: returning " + result.Rows.Count + " row(s)");
                else Console.WriteLine("RawQery: returning null");
            }
            return result;
        }

        private string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return String.Empty;
            string ret = "";
            int doubleDash = 0;
            int openComment = 0;
            int closeComment = 0;

            switch (DbType)
            {
                case DbTypes.MsSql:
                    #region MsSql

                    //
                    // null, below ASCII range, above ASCII range
                    //
                    for (int i = 0; i < s.Length; i++)
                    {
                        if (((int)(s[i]) == 10) ||      // Preserve carriage return
                            ((int)(s[i]) == 13))        // and line feed
                        {
                            ret += s[i];
                        }
                        else if ((int)(s[i]) < 32)
                        {
                            continue;
                        }
                        else
                        {
                            ret += s[i];
                        }
                    }

                    //
                    // double dash
                    //
                    doubleDash = 0;
                    while (true)
                    {
                        doubleDash = ret.IndexOf("--");
                        if (doubleDash < 0)
                        {
                            break;
                        }
                        else
                        {
                            ret = ret.Remove(doubleDash, 2);
                        }
                    }

                    //
                    // open comment
                    // 
                    openComment = 0;
                    while (true)
                    {
                        openComment = ret.IndexOf("/*");
                        if (openComment < 0) break;
                        else 
                        {
                            ret = ret.Remove(openComment, 2);
                        }
                    }

                    //
                    // close comment
                    //
                    closeComment = 0;
                    while (true)
                    {
                        closeComment = ret.IndexOf("*/");
                        if (closeComment < 0) break;
                        else
                        {
                            ret = ret.Remove(closeComment, 2);
                        }
                    }

                    //
                    // in-string replacement
                    //
                    ret = ret.Replace("'", "''");
                    break;

                    #endregion

                case DbTypes.MySql:
                    #region MySql

                    ret = MySqlHelper.EscapeString(s);
                    break;

                    #endregion
            }

            return ret;
        }

        #endregion
    }
}
