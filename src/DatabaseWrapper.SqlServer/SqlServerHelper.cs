using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.SqlServer
{
    /// <summary>
    /// SQL Server implementation of helper properties and methods.
    /// </summary>
    public class SqlServerHelper : DatabaseHelperBase
    {
        #region Public-Members

        /// <summary>
        /// Timestamp format for use in DateTime.ToString([format]).
        /// </summary>
        public new string TimestampFormat { get; set; } = "MM/dd/yyyy hh:mm:ss.fffffff tt";

        /// <summary>
        /// Timestamp offset format for use in DateTimeOffset.ToString([format]).
        /// </summary>
        public new string TimestampOffsetFormat { get; set; } = "MM/dd/yyyy hh:mm:ss.fffffff zzz";

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

            if (String.IsNullOrEmpty(settings.Username) && String.IsNullOrEmpty(settings.Password))
            {
                ret += "Data Source=" + settings.Hostname;
                if (!String.IsNullOrEmpty(settings.Instance)) ret += "\\" + settings.Instance + "; ";
                else ret += "; ";
                ret += "Integrated Security=SSPI; ";
                ret += "Initial Catalog=" + settings.DatabaseName + "; ";
            }
            else
            {
                if (settings.Port > 0)
                {
                    if (String.IsNullOrEmpty(settings.Instance)) ret += "Server=" + settings.Hostname + "," + settings.Port + "; ";
                    else ret += "Server=" + settings.Hostname + "\\" + settings.Instance + "," + settings.Port + "; ";
                }
                else
                {
                    if (String.IsNullOrEmpty(settings.Instance)) ret += "Server=" + settings.Hostname + "; ";
                    else ret += "Server=" + settings.Hostname + "\\" + settings.Instance + "; ";
                }

                ret += "Database=" + settings.DatabaseName + "; ";
                if (!String.IsNullOrEmpty(settings.Username)) ret += "User ID=" + settings.Username + "; ";
                if (!String.IsNullOrEmpty(settings.Password)) ret += "Password=" + settings.Password + "; ";
            }

            return ret;
        }

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public override string RetrieveTableNamesQuery(string database)
        {
            return "SELECT TABLE_NAME FROM " + SanitizeString(database) + ".INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
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
                "SELECT " +
                "  col.TABLE_NAME, col.COLUMN_NAME, col.IS_NULLABLE, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, con.CONSTRAINT_NAME " +
                "FROM INFORMATION_SCHEMA.COLUMNS col " +
                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME AND con.TABLE_NAME = col.TABLE_NAME " +
                "WHERE col.TABLE_NAME='" + ExtractTableName(table) + "' " +
                "AND col.TABLE_CATALOG='" + SanitizeString(database) + "'";
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
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public override string ColumnToCreateQuery(Column col)
        {
            string ret =
                "[" + SanitizeString(col.Name) + "] ";

            switch (col.Type)
            {
                case DataTypeEnum.Varchar:
                    ret += "[varchar](" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.Nvarchar:
                    ret += "[nvarchar](" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.Guid:
                    ret += "[varchar](36) ";
                    break;
                case DataTypeEnum.Int:
                    ret += "[int] ";
                    break;
                case DataTypeEnum.Long:
                    ret += "[bigint] ";
                    break;
                case DataTypeEnum.Decimal:
                    ret += "[decimal](" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.Double:
                    ret += "[float](" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.DateTime:
                    ret += "[datetime2] ";
                    break;
                case DataTypeEnum.DateTimeOffset:
                    ret += "[datetimeoffset] ";
                    break;
                case DataTypeEnum.Blob:
                    ret += "[varbinary](max) ";
                    break;
                case DataTypeEnum.Boolean:
                case DataTypeEnum.TinyInt:
                    ret += "[tinyint] ";
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
                    ret += "IDENTITY(1,1) ";
                }
                else
                {
                    throw new ArgumentException("Primary key column '" + col.Name + "' is of an unsupported type: " + col.Type.ToString());
                }
            }

            if (col.Nullable) ret += "NULL ";
            else ret += "NOT NULL ";

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
                "CREATE TABLE " + PreparedTableName(tableName) + " " +
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
                    ", " +
                    "CONSTRAINT [PK_" + ExtractTableName(tableName) + "] PRIMARY KEY CLUSTERED " +
                    "(" +
                    "  [" + SanitizeString(primaryKey.Name) + "] ASC " +
                    ") " +
                    "WITH " +
                    "(" +
                    "  PAD_INDEX = OFF, " +
                    "  STATISTICS_NORECOMPUTE = OFF, " +
                    "  IGNORE_DUP_KEY = OFF, " +
                    "  ALLOW_ROW_LOCKS = ON, " +
                    "  ALLOW_PAGE_LOCKS = ON " +
                    ") " +
                    "ON [PRIMARY] ";
            } 

            query +=
                ") " +
                "ON [PRIMARY] ";

            return query;
        }

        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string DropTableQuery(string tableName)
        {
            string query = "IF OBJECT_ID('" + PreparedTableNameUnenclosed(tableName) + "', 'U') IS NOT NULL DROP TABLE " + PreparedTableName(tableName);
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
        public override string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        {
            string query = "";
            string whereClause = "";
              
            // select 
            query = "SELECT ";
             
            // top 
            if (maxResults != null && indexStart == null)
            {
                query += "TOP " + maxResults + " ";
            }
             
            // fields 
            if (returnFields == null || returnFields.Count < 1) query += "* ";
            else
            {
                int fieldsAdded = 0;
                foreach (string curr in returnFields)
                {
                    if (fieldsAdded == 0)
                    {
                        query += "[" + SanitizeString(curr) + "]";
                        fieldsAdded++;
                    }
                    else
                    {
                        query += ",[" + SanitizeString(curr) + "]";
                        fieldsAdded++;
                    }
                }
            }
            query += " ";

            // table
            query += "FROM " + PreparedTableName(tableName) + " ";

            // expressions
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            // order clause
            query += BuildOrderByClause(resultOrder);

            // pagination
            if (indexStart != null && maxResults != null)
            {
                query += "OFFSET " + indexStart + " ROWS ";
                query += "FETCH NEXT " + maxResults + " ROWS ONLY ";
            }
            else if (indexStart != null)
            {
                query += "OFFSET " + indexStart + " ROWS ";
            }

            return query;
        }

        /// <summary>
        /// Retrieve a query used for inserting data into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override string InsertQuery(string tableName, Dictionary<string, object> keyValuePairs)
        {
            string ret =
                "INSERT INTO " + PreparedTableName(tableName) + " WITH (ROWLOCK) " +
                "(";

            string keys = "";
            string vals = "";
            BuildKeysValuesFromDictionary(keyValuePairs, out keys, out vals);

            ret += keys + ") " +
                "OUTPUT INSERTED.* " +
                "VALUES " +
                "(" + vals + ") ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for inserting multiple rows into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override string InsertMultipleQuery(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            ValidateInputDictionaries(keyValuePairList);
            string keys = BuildKeysFromDictionary(keyValuePairList[0]);
            List<string> values = BuildValuesFromDictionaries(keyValuePairList);

            string txn = "txn_" + RandomCharacters(12);
            string ret =
                "BEGIN TRANSACTION [" + txn + "] " +
                " BEGIN TRY " +
                "  INSERT INTO " + PreparedTableName(tableName) + " WITH (ROWLOCK) " +
                "  (" + keys + ") " +
                "  VALUES ";

            int added = 0;
            foreach (string value in values)
            {
                if (added > 0) ret += ",";
                ret += "  (" + value + ")";
                added++;
            }

            ret +=
                "  COMMIT TRANSACTION [" + txn + "] " +
                " END TRY " +
                " BEGIN CATCH " +
                "  ROLLBACK TRANSACTION [" + txn + "] " +
                " END CATCH ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for updating data in a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        /// <returns>String.</returns>
        public override string UpdateQuery(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            string keyValueClause = BuildKeyValueClauseFromDictionary(keyValuePairs);

            string ret =
                "UPDATE " + PreparedTableName(tableName) + " WITH (ROWLOCK) SET " +
                keyValueClause + " " +
                "OUTPUT INSERTED.* ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for deleting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string DeleteQuery(string tableName, Expr filter)
        {
            string ret =
                "DELETE FROM " + PreparedTableName(tableName) + " WITH (ROWLOCK) ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for truncating a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE " + PreparedTableName(tableName);
        }

        /// <summary>
        /// Retrieve a query for determing whether data matching specified conditions exists.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string ExistsQuery(string tableName, Expr filter)
        {
            string query = "";
            string whereClause = "";
             
            // select 
            query =
                "SELECT TOP 1 * " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }
             
            return query;
        }

        /// <summary>
        /// Retrieve a query that returns a count of the number of rows matching the supplied conditions.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="countColumnName">Column name to use to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string CountQuery(string tableName, string countColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";
             
            // select 
            query =
                "SELECT COUNT(*) AS " + countColumnName + " " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            return query;
        }

        /// <summary>
        /// Retrieve a query that sums the values found in the specified field.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="fieldName">Column containing values to sum.</param>
        /// <param name="sumColumnName">Column name to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string SumQuery(string tableName, string fieldName, string sumColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";
             
            // select 
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS " + sumColumnName + " " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            return query;
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
            return ts.ToString(TimestampOffsetFormat);
        }

        /// <summary>
        /// Extract the table name from an encapsulated name.
        /// </summary>
        /// <param name="s">String.</param>
        /// <returns>String.</returns>
        public string ExtractTableName(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length != 2) throw new ArgumentException("Table name must have either zero or one period '.' character");
                return SanitizeString(parts[1]);
            }
            else
            {
                return SanitizeString(s);
            }
        }

        #endregion

        #region Private-Members

        private string PreparedUnicodeValue(string s)
        {
            return "N" + PreparedStringValue(s);
        }

        private string PreparedFieldName(string fieldName)
        {
            return "[" + fieldName + "]";
        }

        private string PreparedStringValue(string str)
        {
            return "'" + SanitizeString(str) + "'";
        }

        private string ExpressionToWhereClause(Expr expr)
        {
            if (expr == null) return null;

            string clause = "";

            if (expr.Left == null) return null;

            clause += "(";

            if (expr.Left is Expr)
            {
                clause += ExpressionToWhereClause((Expr)expr.Left) + " ";
            }
            else
            {
                if (!(expr.Left is string))
                {
                    throw new ArgumentException("Left term must be of type Expression or String");
                }

                if (expr.Operator != OperatorEnum.Contains
                    && expr.Operator != OperatorEnum.ContainsNot
                    && expr.Operator != OperatorEnum.StartsWith
                    && expr.Operator != OperatorEnum.StartsWithNot
                    && expr.Operator != OperatorEnum.EndsWith
                    && expr.Operator != OperatorEnum.EndsWithNot)
                {
                    //
                    // These operators will add the left term
                    //
                    clause += PreparedFieldName(expr.Left.ToString()) + " ";
                }
            }

            switch (expr.Operator)
            {
                #region Process-By-Operators

                case OperatorEnum.And:
                    #region And

                    if (expr.Right == null) return null;
                    clause += "AND ";

                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.Or:
                    #region Or

                    if (expr.Right == null) return null;
                    clause += "OR ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.Equals:
                    #region Equals

                    if (expr.Right == null) return null;
                    clause += "= ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.NotEquals:
                    #region NotEquals

                    if (expr.Right == null) return null;
                    clause += "<> ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.In:
                    #region In

                    if (expr.Right == null) return null;
                    int inAdded = 0;
                    if (!Helper.IsList(expr.Right)) return null;
                    List<object> inTempList = Helper.ObjectToList(expr.Right);
                    clause += " IN (";
                    foreach (object currObj in inTempList)
                    {
                        if (currObj == null) continue;
                        if (inAdded > 0) clause += ",";
                        if (currObj is DateTime || currObj is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(currObj)) + "'";
                        }
                        else if (currObj is int || currObj is long || currObj is decimal)
                        {
                            clause += currObj.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(currObj.ToString());
                        }
                        inAdded++;
                    }
                    clause += ")";
                    break;

                #endregion

                case OperatorEnum.NotIn:
                    #region NotIn

                    if (expr.Right == null) return null;
                    int notInAdded = 0;
                    if (!Helper.IsList(expr.Right)) return null;
                    List<object> notInTempList = Helper.ObjectToList(expr.Right);
                    clause += " NOT IN (";
                    foreach (object currObj in notInTempList)
                    {
                        if (currObj == null) continue;
                        if (notInAdded > 0) clause += ",";
                        if (currObj is DateTime || currObj is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(currObj)) + "'";
                        }
                        else if (currObj is int || currObj is long || currObj is decimal)
                        {
                            clause += currObj.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(currObj.ToString());
                        }
                        notInAdded++;
                    }
                    clause += ")";
                    break;

                #endregion

                case OperatorEnum.Contains:
                    #region Contains

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue("%" + expr.Right.ToString() + "%") +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.ContainsNot:
                    #region ContainsNot

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.Right.ToString() + "%") +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.StartsWith:
                    #region StartsWith

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + (PreparedStringValue(expr.Right.ToString() + "%")) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.StartsWithNot:
                    #region StartsWithNot

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + (PreparedStringValue(expr.Right.ToString() + "%")) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.EndsWith:
                    #region EndsWith

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.EndsWithNot:
                    #region EndsWithNot

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.GreaterThan:
                    #region GreaterThan

                    if (expr.Right == null) return null;
                    clause += "> ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.GreaterThanOrEqualTo:
                    #region GreaterThanOrEqualTo

                    if (expr.Right == null) return null;
                    clause += ">= ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.LessThan:
                    #region LessThan

                    if (expr.Right == null) return null;
                    clause += "< ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.LessThanOrEqualTo:
                    #region LessThanOrEqualTo

                    if (expr.Right == null) return null;
                    clause += "<= ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.IsNull:
                    #region IsNull

                    clause += " IS NULL";
                    break;

                #endregion

                case OperatorEnum.IsNotNull:
                    #region IsNotNull

                    clause += " IS NOT NULL";
                    break;

                    #endregion

                    #endregion
            }

            clause += ")";

            return clause;
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

        private string RandomCharacters(int len)
        {
            char[] letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".ToCharArray();
            Random rand = new Random();
            string word = "";
            for (int i = 0; i < len; i++)
            {
                int num = rand.Next(0, letters.Length - 1);
                word += letters[num];
            }
            return word;
        }

        private string PreparedTableName(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length > 0)
                {
                    string ret = "";
                    for (int i = 0; i < parts.Length; i++)
                    {
                        if (i > 0) ret += ".";
                        ret += "[" + SanitizeString(parts[i]) + "]";
                    }
                    return ret;
                }
            }

            return
                "[" +
                SanitizeString(s) +
                "]";
        }

        private string PreparedTableNameUnenclosed(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length != 2) throw new ArgumentException("Table name must have either zero or one period '.' character");
                return
                    SanitizeString(parts[0]) +
                    "." +
                    SanitizeString(parts[1]);
            }
            else
            {
                return
                    SanitizeString(s);
            }
        }

        private void BuildKeysValuesFromDictionary(Dictionary<string, object> keyValuePairs, out string keys, out string vals)
        {
            keys = "";
            vals = "";
            int added = 0;

            foreach (KeyValuePair<string, object> currKvp in keyValuePairs)
            {
                if (String.IsNullOrEmpty(currKvp.Key)) continue;

                if (added > 0)
                {
                    keys += ",";
                    vals += ",";
                }

                keys += PreparedFieldName(currKvp.Key);

                if (currKvp.Value != null)
                {
                    if (currKvp.Value is DateTime
                        || currKvp.Value is DateTime?)
                    {
                        vals += "'" + ((DateTime)currKvp.Value).ToString(TimestampFormat) + "'";
                    }
                    else if (currKvp.Value is DateTimeOffset
                        || currKvp.Value is DateTimeOffset?)
                    {
                        vals += "'" + ((DateTimeOffset)currKvp.Value).ToString(TimestampOffsetFormat) + "'";
                    }
                    else if (currKvp.Value is int
                        || currKvp.Value is long
                        || currKvp.Value is decimal)
                    {
                        vals += currKvp.Value.ToString();
                    }
                    else if (currKvp.Value is bool)
                    {
                        vals += ((bool)currKvp.Value ? "1" : "0");
                    }
                    else if (currKvp.Value is byte[])
                    {
                        vals += "0x" + BitConverter.ToString((byte[])currKvp.Value).Replace("-", "");
                    }
                    else
                    {
                        if (Helper.IsExtendedCharacters(currKvp.Value.ToString()))
                        {
                            vals += PreparedUnicodeValue(currKvp.Value.ToString());
                        }
                        else
                        {
                            vals += PreparedStringValue(currKvp.Value.ToString());
                        }
                    }
                }
                else
                {
                    vals += "null";
                }

                added++;
            }
        }

        private void ValidateInputDictionaries(List<Dictionary<string, object>> dicts)
        {
            Dictionary<string, object> reference = dicts[0];

            if (dicts.Count > 1)
            {
                foreach (Dictionary<string, object> dict in dicts)
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

        private List<string> BuildValuesFromDictionaries(List<Dictionary<string, object>> dicts)
        {
            List<string> values = new List<string>();

            foreach (Dictionary<string, object> currDict in dicts)
            {
                string vals = "";
                int valsAdded = 0;

                foreach (KeyValuePair<string, object> currKvp in currDict)
                {
                    if (valsAdded > 0) vals += ",";

                    if (currKvp.Value != null)
                    {
                        if (currKvp.Value is DateTime
                            || currKvp.Value is DateTime?)
                        {
                            vals += "'" + ((DateTime)currKvp.Value).ToString(TimestampFormat) + "'";
                        }
                        else if (currKvp.Value is DateTimeOffset
                            || currKvp.Value is DateTimeOffset?)
                        {
                            vals += "'" + ((DateTimeOffset)currKvp.Value).ToString(TimestampOffsetFormat) + "'";
                        }
                        else if (currKvp.Value is int
                            || currKvp.Value is long
                            || currKvp.Value is decimal)
                        {
                            vals += currKvp.Value.ToString();
                        }
                        else if (currKvp.Value is bool)
                        {
                            vals += ((bool)currKvp.Value ? "1" : "0");
                        }
                        else if (currKvp.Value is byte[])
                        {
                            vals += "0x" + BitConverter.ToString((byte[])currKvp.Value).Replace("-", "");
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(currKvp.Value.ToString()))
                            {
                                vals += PreparedUnicodeValue(currKvp.Value.ToString());
                            }
                            else
                            {
                                vals += PreparedStringValue(currKvp.Value.ToString());
                            }
                        }
                    }
                    else
                    {
                        vals += "null";
                    }

                    valsAdded++;
                }

                values.Add(vals);
            }

            return values;
        }

        private string BuildKeyValueClauseFromDictionary(Dictionary<string, object> keyValuePairs)
        {
            string keyValueClause = "";
            int added = 0;

            foreach (KeyValuePair<string, object> currKvp in keyValuePairs)
            {
                if (String.IsNullOrEmpty(currKvp.Key)) continue;

                if (added > 0) keyValueClause += ",";

                if (currKvp.Value != null)
                {
                    if (currKvp.Value is DateTime
                        || currKvp.Value is DateTime?)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "='" + ((DateTime)currKvp.Value).ToString(TimestampFormat) + "'";
                    }
                    else if (currKvp.Value is DateTimeOffset
                        || currKvp.Value is DateTimeOffset?)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "='" + ((DateTimeOffset)currKvp.Value).ToString(TimestampOffsetFormat) + "'";
                    }
                    else if (currKvp.Value is int
                        || currKvp.Value is long
                        || currKvp.Value is decimal)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + currKvp.Value.ToString();
                    }
                    else if (currKvp.Value is bool)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + ((bool)currKvp.Value ? "1" : "0");
                    }
                    else if (currKvp.Value is byte[])
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + "0x" + BitConverter.ToString((byte[])currKvp.Value).Replace("-", "");
                    }
                    else
                    {
                        if (Helper.IsExtendedCharacters(currKvp.Value.ToString()))
                        {
                            keyValueClause += PreparedFieldName(currKvp.Key) + "=" + PreparedUnicodeValue(currKvp.Value.ToString());
                        }
                        else
                        {
                            keyValueClause += PreparedFieldName(currKvp.Key) + "=" + PreparedStringValue(currKvp.Value.ToString());
                        }
                    }
                }
                else
                {
                    keyValueClause += PreparedFieldName(currKvp.Key) + "= null";
                }

                added++;
            }

            return keyValueClause;
        }

        #endregion
    }
}
