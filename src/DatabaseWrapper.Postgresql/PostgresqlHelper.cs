using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.Postgresql
{
    /// <summary>
    /// PostgreSQL implementation of helper properties and methods.
    /// </summary>
    public class PostgresqlHelper : DatabaseHelperBase
    {
        #region Public-Members

        /// <summary>
        /// Timestamp format for use in DateTime.ToString([format]).
        /// </summary>
        public new string TimestampFormat = "MM/dd/yyyy hh:mm:ss.fffffff tt";

        /// <summary>
        /// Timestamp offset format for use in DateTimeOffset.ToString([format]).
        /// </summary>
        public new string TimestampOffsetFormat = "MM/dd/yyyy hh:mm:ss.fffffff zzz";

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
        public override string ConnectionString(DatabaseSettings settings)
        {
            string ret = "";

            //
            // http://www.connectionstrings.com/postgresql/
            //
            // PgSQL does not use 'Instance'
            ret += "Server=" + settings.Hostname + "; ";
            if (settings.Port > 0) ret += "Port=" + settings.Port + "; ";
            ret += "Database=" + settings.DatabaseName + "; ";
            if (!String.IsNullOrEmpty(settings.Username)) ret += "User ID=" + settings.Username + "; ";
            if (!String.IsNullOrEmpty(settings.Password)) ret += "Password=" + settings.Password + "; ";

            return ret;
        }

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public override string LoadTableNamesQuery(string database)
        {
            return "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
        }

        /// <summary>
        /// Query to retrieve the list of columns for a table.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <param name="table">Table name.</param>
        /// <returns></returns>
        public override string LoadTableColumnsQuery(string database, string table)
        {
            return
                "SELECT " +
                "  cols.COLUMN_NAME AS COLUMN_NAME, " +
                "  cols.IS_NULLABLE AS IS_NULLABLE, " +
                "  cols.DATA_TYPE AS DATA_TYPE, " +
                "  cols.CHARACTER_MAXIMUM_LENGTH AS CHARACTER_MAXIMUM_LENGTH, " +
                "  CASE " +
                "    WHEN cons.COLUMN_NAME IS NULL THEN 'NO' ELSE 'YES' " +
                "  END AS IS_PRIMARY_KEY " +
                "FROM test.INFORMATION_SCHEMA.COLUMNS cols " +
                "LEFT JOIN " + database + ".INFORMATION_SCHEMA.KEY_COLUMN_USAGE cons ON cols.COLUMN_NAME = cons.COLUMN_NAME " +
                "WHERE cols.TABLE_NAME = '" + ExtractTableName(table) + "';";
        }

        /// <summary>
        /// Method to sanitize a string.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>String.</returns>
        public override string SanitizeString(string val)
        {
            string tag = "$" + EscapeString(val, 2) + "$";
            return tag + val + tag;
        }

        /// <summary>
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public override string ColumnToCreateString(Column col)
        {
            string ret =
                "\"" + SanitizeFieldname(col.Name) + "\" ";

            if (col.PrimaryKey)
            {
                ret += "SERIAL PRIMARY KEY ";
                return ret;
            }

            switch (col.Type)
            {
                case DataTypeEnum.Varchar:
                case DataTypeEnum.Nvarchar:
                    ret += "character varying(" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.Guid:
                    ret += "character varying(36) ";
                    break;
                case DataTypeEnum.Int:
                    ret += "integer ";
                    break;
                case DataTypeEnum.Long:
                    ret += "bigint ";
                    break;
                case DataTypeEnum.Decimal:
                    ret += "numeric(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.Double:
                    ret += "float(" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.DateTime:
                    ret += "timestamp without time zone ";
                    break;
                case DataTypeEnum.DateTimeOffset:
                    ret += "timestamp with time zone ";
                    break;
                case DataTypeEnum.Blob:
                    ret += "bytea ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
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
                query += ColumnToCreateString(curr);
                added++;
            }
             
            query +=
                ") " +
                "WITH " +
                "(" +
                "  OIDS = FALSE" +
                ")";

            return query; 
        }

        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string DropTableQuery(string tableName)
        {
            string query = "DROP TABLE IF EXISTS " + PreparedTableName(tableName) + " ";
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
             
            // SELECT 
            query += "SELECT ";
             
            // fields 
            if (returnFields == null || returnFields.Count < 1) query += "* ";
            else
            {
                int fieldsAdded = 0;
                foreach (string curr in returnFields)
                {
                    if (fieldsAdded == 0)
                    {
                        query += "\"" + SanitizeFieldname(curr) + "\"";
                        fieldsAdded++;
                    }
                    else
                    {
                        query += ",\"" + SanitizeFieldname(curr) + "\"";
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
             
            // limit 
            if (maxResults > 0)
            {
                if (indexStart != null && indexStart >= 0)
                {
                    query += "OFFSET " + indexStart + " LIMIT " + maxResults;
                }
                else
                {
                    query += "LIMIT " + maxResults;
                }
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
                "INSERT INTO " + PreparedTableName(tableName) + " " +
                "(";

            string keys = "";
            string vals = "";
            BuildKeysValuesFromDictionary(keyValuePairs, out keys, out vals);

            ret += keys + ") " +
                "VALUES " +
                "(" + vals + ") " +
                "RETURNING *;"; 

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

            string ret =
                "BEGIN TRANSACTION;" +
                "  INSERT INTO " + PreparedTableName(tableName) + " " +
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
                ";  COMMIT; ";

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
                "UPDATE " + PreparedTableName(tableName) + " SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";
            ret += "RETURNING *";

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
                "DELETE FROM " + PreparedTableName(tableName) + " ";

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
            return "TRUNCATE TABLE " + PreparedTableName(tableName) + " ";
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
                "SELECT * " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            query += "LIMIT 1";
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
            string whereClause = "";
             
            // select 
            string query =
                "SELECT SUM(" + SanitizeFieldname(fieldName) + ") AS " + sumColumnName + " " +
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
        public override string DbTimestamp(DateTime ts)
        {
            return ts.ToString(TimestampFormat);
        }

        /// <summary>
        /// Retrieve a timestamp offset in the database format.
        /// </summary>
        /// <param name="ts">DateTimeOffset.</param>
        /// <returns>String.</returns>
        public override string DbTimestampOffset(DateTimeOffset ts)
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
                return SanitizeStringInternal(parts[1]);
            }
            else
            {
                return SanitizeStringInternal(s);
            }
        }

        #endregion

        #region Private-Methods

        private string PreparedFieldName(string fieldName)
        {
            return "\"" + fieldName + "\"";
        }

        private string PreparedStringValue(string str)
        {
            // uses $xx$ escaping
            return SanitizeString(str);
        }

        private string PreparedUnicodeValue(string s)
        {
            return "U&" + PreparedStringValue(s);
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(currObj)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(currObj)) + "'";
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
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
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
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.Right)) + "'";
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
                ret += SanitizeFieldname(resultOrder[i].ColumnName) + " ";
                if (resultOrder[i].Direction == OrderDirectionEnum.Ascending) ret += "ASC";
                else if (resultOrder[i].Direction == OrderDirectionEnum.Descending) ret += "DESC";
            }

            ret += " ";
            return ret;
        }
         
        private string SanitizeStringInternal(string val)
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

        private string EscapeString(string val, int numChar)
        {
            string ret = "";
            Random random = new Random();
            if (numChar < 1) return ret;

            while (true)
            {
                ret = "";
                random = new Random();

                int valid = 0;
                int num = 0;

                for (int i = 0; i < numChar; i++)
                {
                    num = 0;
                    valid = 0;
                    while (valid == 0)
                    {
                        num = random.Next(126);
                        if (((num > 64) && (num < 91)) ||
                            ((num > 96) && (num < 123)))
                        {
                            valid = 1;
                        }
                    }
                    ret += (char)num;
                }

                if (!val.Contains("$" + ret + "$")) break;
            }

            return ret;
        }

        private string SanitizeFieldname(string val)
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

        private string PreparedTableName(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length != 2) throw new ArgumentException("Table name must have either zero or one period '.' character");
                return
                    SanitizeStringInternal(parts[0]) +
                    "." +
                    SanitizeStringInternal(parts[1]);
            }
            else
            {
                return
                    SanitizeStringInternal(s);
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
                    else if (currKvp.Value is byte[])
                    {
                        vals += "decode('" + Helper.ByteArrayToHexString((byte[])currKvp.Value) + "', 'hex')";
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
                        else if (currKvp.Value is byte[])
                        {
                            vals += "decode('" + Helper.ByteArrayToHexString((byte[])currKvp.Value) + "', 'hex')";
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
                    else if (currKvp.Value is byte[])
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + "decode('" + Helper.ByteArrayToHexString((byte[])currKvp.Value) + "', 'hex')";
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
