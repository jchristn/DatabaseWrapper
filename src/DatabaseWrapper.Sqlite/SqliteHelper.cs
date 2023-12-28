using System;
using System.Collections.Generic;
using static System.FormattableString;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.Sqlite
{
    using QueryAndParameters = System.ValueTuple<string, IEnumerable<KeyValuePair<string,object>>>;

    /// <summary>
    /// Sqlite implementation of helper properties and methods.
    /// </summary>
    public class SqliteHelper : DatabaseHelperBase
    {
        #region Public-Members

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
            return "Data Source=" + settings.Filename;
        }

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public override string RetrieveTableNamesQuery(string database)
        {
            return
                "DROP TABLE IF EXISTS sqlitemetadata; " +
                "CREATE TEMPORARY TABLE sqlitemetadata AS " +
                "  SELECT " +
                "    name AS TABLE_NAME " +
                "  FROM " +
                "    sqlite_master " +
                "  WHERE " +
                "    type ='table' " +
                "    AND name NOT LIKE 'sqlite_%'; " +
                "SELECT * FROM sqlitemetadata;";
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
                "DROP TABLE IF EXISTS sqlitemetadata; " +
                "CREATE TEMPORARY TABLE sqlitemetadata AS " +
                "  SELECT " +
                "    m.name AS TABLE_NAME,  " +
                "    p.name AS COLUMN_NAME, " +
                "    p.type AS DATA_TYPE, " +
                "    p.pk AS IS_PRIMARY_KEY, " +
                "    p.[notnull] AS IS_NOT_NULLABLE " +
                "  FROM " +
                "    sqlite_master m " +
                "  LEFT OUTER JOIN pragma_table_info((m.name)) p " +
                "    ON m.name <> p.name " +
                "  WHERE " +
                "    m.type = 'table' " +
                "    AND m.name = '" + table + "' " +
                "  ORDER BY TABLE_NAME; " +
                "SELECT * FROM sqlitemetadata; ";
        }

        /// <summary>
        /// Method to sanitize a string.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>String.</returns>
        public override string SanitizeString(string val)
        {
            string ret = "";

            //
            // null, below ASCII range, above ASCII range
            //
            for (int i = 0; i < val.Length; i++)
            {
                if (((int)(val[i]) == 10) ||      // Preserve carriage return
                    ((int)(val[i]) == 13))        // and line feed
                {
                    ret += val[i];
                }
                else if ((int)(val[i]) < 32)
                {
                    continue;
                }
                else
                {
                    ret += val[i];
                }
            }

            //
            // double dash
            //
            int doubleDash = 0;
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
            int openComment = 0;
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
            int closeComment = 0;
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
            return ret;
        }

        /// <summary>
        /// Prepare a field name for use in a SQL query by surrounding it with backticks.
        /// </summary>
        /// <param name="fieldName">Name of the field to be prepared.</param>
        /// <returns>Field name for use in a SQL query.</returns>
        public override string PreparedFieldName(string fieldName)
        {
            return "`" + fieldName + "`";
        }

        /// <summary>
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public override string ColumnToCreateQuery(Column col)
        {
            string ret =
                PreparedFieldName(SanitizeString(col.Name)) + " ";

            switch (col.Type)
            {
                case DataTypeEnum.Varchar: 
                case DataTypeEnum.Nvarchar:
                    ret += "VARCHAR(" + col.MaxLength + ") COLLATE NOCASE ";
                    break;
                case DataTypeEnum.Guid:
                    ret += "VARCHAR(36) ";
                    break;
                case DataTypeEnum.Int:
                    ret += "INTEGER ";
                    break;
                case DataTypeEnum.Long:
                    ret += "BIGINT ";
                    break;
                case DataTypeEnum.Decimal:
                    ret += "DECIMAL(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.Double:
                    ret += "REAL ";
                    break;
                case DataTypeEnum.DateTime:
                case DataTypeEnum.DateTimeOffset:
                    ret += "TEXT ";
                    break;
                case DataTypeEnum.Blob:
                    ret += "BLOB ";
                    break;
                case DataTypeEnum.Boolean:
                case DataTypeEnum.TinyInt:
                    ret += "TINYINT ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.PrimaryKey)
            {
                if (col.Type == DataTypeEnum.Varchar || col.Type == DataTypeEnum.Nvarchar)
                {
                    ret += "UNIQUE ";
                }
                else if (col.Type == DataTypeEnum.Int || col.Type == DataTypeEnum.Long)
                {
                    ret += "PRIMARY KEY AUTOINCREMENT ";
                }
                else
                {
                    throw new ArgumentException("Primary key column '" + col.Name + "' is of an unsupported type: " + col.Type.ToString());
                }
            }

            if (!col.Nullable) ret += "NOT NULL ";

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
            string ret =
                "CREATE TABLE IF NOT EXISTS " + PreparedFieldName(SanitizeString(tableName)) + " " +
                "(";

            int added = 0;
            foreach (Column curr in columns)
            {
                if (added > 0) ret += ", ";
                ret += ColumnToCreateQuery(curr); 
                added++;
            }

            ret += ")";

            return ret;
        }

        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string DropTableQuery(string tableName)
        {
            return "DROP TABLE IF EXISTS " + PreparedFieldName(SanitizeString(tableName));
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
            // select
            //
            query = "SELECT ";

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
                        query += PreparedFieldName(SanitizeString(curr));
                        fieldsAdded++;
                    }
                    else
                    {
                        query += "," + PreparedFieldName(SanitizeString(curr));
                        fieldsAdded++;
                    }
                }
            }
            query += " ";

            //
            // table
            //
            query += "FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

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
            // pagination
            //
            if (indexStart != null && maxResults != null)
            {
                query += "LIMIT " + maxResults + " ";
                query += "OFFSET " + indexStart + " ";
            }
            else if (maxResults != null)
            {
                query += "LIMIT " + maxResults + " ";
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
                "BEGIN TRANSACTION; " +
                "INSERT INTO " + PreparedFieldName(SanitizeString(tableName)) + " " +
                "(";

            ret += string.Join(", ", keyValuePairs.Keys.Select(k => PreparedFieldName(k))) + ") " +
                "VALUES " +
                "(" + string.Join(", ", o_ret.Select(k => k.Key)) + "); " +
                "SELECT last_insert_rowid() AS id; " +
                ";COMMIT;";

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
                "BEGIN TRANSACTION; " +
                "  INSERT INTO " + PreparedFieldName(SanitizeString(tableName)) + " " +
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
                ";COMMIT;";

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
                "BEGIN TRANSACTION; " +
                "UPDATE " + PreparedFieldName(SanitizeString(tableName)) + " SET " +
                string.Join(", ", parameters.Select(kv => kv.Key.Substring(FIELD_PREFIX.Length) + "=" + kv.Key)) + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter, parameters) + " ";

            ret +=
                ";COMMIT;";

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
                "DELETE FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

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
            return "DELETE FROM " + PreparedFieldName(SanitizeString(tableName));
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
                "FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

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
                "FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

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
                "FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

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

        #endregion

        #region Private-Methods
        private string PreparedStringValue(string str)
        {
            return "'" + SanitizeString(str) + "'";
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
