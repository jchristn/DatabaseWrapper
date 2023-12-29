using System;
using System.Collections.Generic;
using static System.FormattableString;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data;
using MySql.Data.MySqlClient;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.Mysql
{
    using QueryAndParameters = System.ValueTuple<string, IEnumerable<KeyValuePair<string,object>>>;

    /// <summary>
    /// MySQL implementation of helper properties and methods.
    /// </summary>
    public class MysqlHelper : DatabaseHelperBase
    {
        #region Public-Members

        /// <summary>
        /// Timestamp format for use in DateTime.ToString([format]).
        /// </summary>
        public new string TimestampFormat { get; set; } = "yyyy-MM-dd HH:mm:ss.ffffff";

        /// <summary>
        /// Timestamp offset format for use in DateTimeOffset.ToString([format]).
        /// </summary>
        public new string TimestampOffsetFormat { get; set; } = "yyyy-MM-dd HH:mm:ss.ffffffzzz";

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Build a connection string from DatabaseSettings.
        /// </summary>
        /// <param name="settings">Settings.</param>
        /// <returns>String.</returns>
        public override string GenerateConnectionString(DatabaseSettings settings)
        {
            string ret = "";

            //
            // http://www.connectionstrings.com/mysql/
            //
            // MySQL does not use 'Instance'
            ret += "Server=" + settings.Hostname + "; ";
            if (settings.Port > 0) ret += "Port=" + settings.Port + "; ";
            ret += "Database=" + settings.DatabaseName + "; ";
            if (!String.IsNullOrEmpty(settings.Username)) ret += "Uid=" + settings.Username + "; ";
            if (!String.IsNullOrEmpty(settings.Password)) ret += "Pwd=" + settings.Password + "; ";

            return ret;
        }

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public override string RetrieveTableNamesQuery(string database)
        {
            return "SHOW TABLES";
        }

        /// <summary>
        /// Query to retrieve the list of columns for a table.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <param name="table">Table name.</param>
        /// <returns></returns>
        public override string RetrieveTableColumnsQuery(string database, string table)
        {
            return
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
                "TABLE_NAME='" + table + "' " +
                "AND TABLE_SCHEMA='" + database + "'";
        }

        /// <summary>
        /// Method to sanitize a string.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>String.</returns>
        public override string SanitizeString(string val)
        {
            string ret = "";
            ret = MySqlHelper.EscapeString(val);
            return ret;
        }

        /// <summary>
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public override string ColumnToCreateQuery(Column col)
        { 
            string ret =
                "`" + SanitizeString(col.Name) + "` ";

            switch (col.Type)
            {
                case DataTypeEnum.Varchar:
                case DataTypeEnum.Nvarchar:
                    ret += "varchar(" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.Guid:
                    ret += "varchar(36) ";
                    break;
                case DataTypeEnum.Int:
                case DataTypeEnum.Long:
                    if (col.MaxLength != null) ret += "int(" + col.MaxLength + ") ";
                    else ret += "int ";
                    break;
                case DataTypeEnum.Decimal:
                    ret += "decimal(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.Double:
                    ret += "float(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.DateTime:
                case DataTypeEnum.DateTimeOffset:
                    if (col.Precision != null)
                    {
                        ret += "datetime(" + col.Precision + ") ";
                    }
                    else
                    {
                        ret += "datetime ";
                    }
                    break;
                case DataTypeEnum.Blob:
                    ret += "longblob ";
                    break;
                case DataTypeEnum.Boolean:
                case DataTypeEnum.TinyInt:
                    ret += "tinyint ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.Nullable) ret += "NULL ";
            else ret += "NOT NULL ";

            if (col.PrimaryKey) ret += "AUTO_INCREMENT ";

            return ret;
        }

        /// <summary>
        /// Retrieve the primary key column from a list of columns.
        /// </summary>
        /// <param name="columns">List of Column.</param>
        /// <returns>Column.</returns>
        public override Column GetPrimaryKeyColumn(List<Column> columns)
        {
            Column c = columns.FirstOrDefault(d => d.PrimaryKey);
            if (c == null || c == default(Column)) return null;
            return c;
        }

        /// <summary>
        /// Retrieve a query used for table creation.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="columns">List of columns.</param>
        /// <returns>String.</returns>
        public override string CreateTableQuery(string tableName, List<Column> columns)
        {
            string query =
                "CREATE TABLE `" + SanitizeString(tableName) + "` " +
                "(";

            int added = 0;
            foreach (Column curr in columns)
            {
                if (added > 0) query += ", ";
                query += ColumnToCreateQuery(curr);
                added++;
            }

            Column primaryKey = GetPrimaryKeyColumn(columns);
            if (primaryKey != null)
            {
                query +=
                    "," +
                    "PRIMARY KEY (`" + SanitizeString(primaryKey.Name) + "`)";
            }

            query +=
                ") " +
                "ENGINE=InnoDB " +
                "AUTO_INCREMENT=1 " +
                "DEFAULT CHARSET=utf8mb4 ";

            return query;
        }

        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string DropTableQuery(string tableName)
        {
            string query = "DROP TABLE IF EXISTS `" + SanitizeString(tableName) + "`";
            return query;
        }

        /// <summary>
        /// Retrieve a query used for selecting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="indexStart">Index start.</param>
        /// <param name="maxResults">Maximum number of results to retrieve.</param>
        /// <param name="returnFields">List of field names to return.</param>
        /// <param name="filter">Expression filter.</param>
        /// <param name="resultOrder">Result order.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        { 
            string query = "";
            string whereClause = "";

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
            query += "FROM `" + SanitizeString(tableName) + "` ";

            //
            // expressions
            //
            var parameters_list = new List<KeyValuePair<string,object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters_list);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            // 
            // order clause
            // 
            query += BuildOrderByClause(resultOrder);

            //
            // limit
            //
            if (maxResults > 0)
            {
                if (indexStart != null && indexStart >= 0)
                {
                    query += "LIMIT " + indexStart + "," + maxResults;
                }
                else
                {
                    query += "LIMIT " + maxResults;
                }
            }

            return (query, parameters_list);
        }

        /// <summary>
        /// Retrieve a query used for inserting data into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters InsertQuery(string tableName, Dictionary<string, object> keyValuePairs)
        {
            var o_ret = keyValuePairs.Select(kv => new KeyValuePair<string,object>("@F_" + kv.Key, kv.Value));
            string ret =
                "START TRANSACTION; " +
                "INSERT INTO `" + SanitizeString(tableName) + "` " +
                "(";

            ret += string.Join(", ", keyValuePairs.Keys.Select(k => PreparedFieldName(k))) + ") " +
                "VALUES " +
                "(" + string.Join(", ", o_ret.Select(k => k.Key)) + "); " + 
                "SELECT LAST_INSERT_ID() AS id; " + 
                "COMMIT; ";

            return (ret, o_ret);
        }

        /// <summary>
        /// Retrieve a query for inserting multiple rows into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters InsertMultipleQuery(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            ValidateInputDictionaries(keyValuePairList);
            string keys = BuildKeysFromDictionary(keyValuePairList[0]);
            var ret_values = new List<KeyValuePair<string,object>>();

            string ret =
                "START TRANSACTION;" +
                "  INSERT INTO `" + SanitizeString(tableName) + "` " +
                "  (" + keys + ") " +
                "  VALUES ";

            for (int i_dict=0; i_dict<keyValuePairList.Count; ++i_dict)
            {
                var dict = keyValuePairList[i_dict];
                var prefix = Invariant($"@F{i_dict}_");
                var this_round = dict.Select(kv => new KeyValuePair<string, object>(prefix + kv.Key, kv.Value));
                if (i_dict>0) {
                    ret += ", ";
                }
                ret += "(" + string.Join(", ", this_round.Select(kv => kv.Key)) + ")";
                ret_values.AddRange(this_round);
            }

            ret +=
                ";  COMMIT; ";

            return (ret, ret_values);
        }

        /// <summary>
        /// Retrieve a query for updating data in a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        /// <returns>String.</returns>
        public override QueryAndParameters UpdateQuery(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            const string FIELD_PREFIX = "@F";
            var parameters = keyValuePairs.Select(kv => new KeyValuePair<string, object>(FIELD_PREFIX + kv.Key, kv.Value)).ToList();

            string ret =
                "UPDATE `" + SanitizeString(tableName) + "` SET " +
                string.Join(", ", parameters.Select(kv => kv.Key.Substring(FIELD_PREFIX.Length) + "=" + kv.Key)) + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter, parameters) + " ";
            
            return (ret, parameters);
        }

        /// <summary>
        /// Retrieve a query for deleting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters DeleteQuery(string tableName, Expr filter)
        {
            string ret =
                "DELETE FROM `" + SanitizeString(tableName) + "` ";

            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter, parameters) + " ";

            return (ret, parameters);
        }

        /// <summary>
        /// Retrieve a query for truncating a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE `" + SanitizeString(tableName) + "`";
        }

        /// <summary>
        /// Retrieve a query for determing whether data matching specified conditions exists.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters ExistsQuery(string tableName, Expr filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT * " +
                "FROM `" + SanitizeString(tableName) + "` ";

            //
            // expressions
            //
            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            query += "LIMIT 1";
            return (query, parameters);
        }

        /// <summary>
        /// Retrieve a query that returns a count of the number of rows matching the supplied conditions.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="countColumnName">Column name to use to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters CountQuery(string tableName, string countColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT COUNT(*) AS " + countColumnName + " " +
                "FROM `" + SanitizeString(tableName) + "` ";

            //
            // expressions
            //
            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            return (query, parameters);
        }

        /// <summary>
        /// Retrieve a query that sums the values found in the specified field.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="fieldName">Column containing values to sum.</param>
        /// <param name="sumColumnName">Column name to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters SumQuery(string tableName, string fieldName, string sumColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS " + sumColumnName + " " +
                "FROM `" + SanitizeString(tableName) + "` ";

            //
            // expressions
            //
            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            return (query, parameters);
        }

        /// <summary>
        /// Retrieve a timestamp in the database format.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>String.</returns>
        public override string GenerateTimestamp(DateTime ts)
        {
            return ts.ToString(TimestampFormat);
        }

        /// <summary>
        /// Retrieve a timestamp offset in the database format.
        /// </summary>
        /// <param name="ts">DateTimeOffset.</param>
        /// <returns>String.</returns>
        public override string GenerateTimestampOffset(DateTimeOffset ts)
        {
            return ts.DateTime.ToString(TimestampFormat);
        }

        /// <summary>
        /// Prepare a field name for use in a SQL query.
        /// </summary>
        /// <param name="fieldName">Name of the field to be prepared.</param>
        /// <returns>Field name for use in a SQL query.</returns>
        public override string PreparedFieldName(string fieldName)
        {
            return "`" + fieldName + "`";
        }

        #endregion

        #region Private-Methods

        private string PreparedStringValue(string str)
        {
            return "'" + SanitizeString(str) + "'";
        }

        private string PreparedUnicodeValue(string s)
        {
            return "N" + PreparedStringValue(s);
        }

        private string BuildOrderByClause(ResultOrder[] resultOrder)
        {
            if (resultOrder == null || resultOrder.Length < 0) return null;

            string ret = "ORDER BY ";

            for (int i = 0; i < resultOrder.Length; i++)
            {
                if (i > 0) ret += ", ";
                ret += SanitizeString(resultOrder[i].ColumnName) + " ";
                if (resultOrder[i].Direction == OrderDirectionEnum.Ascending) ret += "ASC";
                else if (resultOrder[i].Direction == OrderDirectionEnum.Descending) ret += "DESC";
            }

            ret += " ";
            return ret;
        }

        private void ValidateInputDictionaries(List<Dictionary<string, object>> keyValuePairList)
        {
            Dictionary<string, object> reference = keyValuePairList[0];

            if (keyValuePairList.Count > 1)
            {
                foreach (Dictionary<string, object> dict in keyValuePairList)
                {
                    if (!(reference.Count == dict.Count) || !(reference.Keys.SequenceEqual(dict.Keys)))
                    {
                        throw new ArgumentException("All supplied dictionaries must contain exactly the same keys.");
                    }
                }
            }
        }

        private string BuildKeysFromDictionary(Dictionary<string, object> reference)
        {
            string keys = "";
            int keysAdded = 0;
            foreach (KeyValuePair<string, object> curr in reference)
            {
                if (keysAdded > 0) keys += ",";
                keys += PreparedFieldName(curr.Key);
                keysAdded++;
            }

            return keys;
        }

        #endregion
    }
}