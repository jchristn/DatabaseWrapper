using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.SqlServer
{
    internal static class SqlServerHelper
    {
        internal static string TimestampFormat = "MM/dd/yyyy hh:mm:ss.fffffff tt";

        internal static string TimestampOffsetFormat = "MM/dd/yyyy hh:mm:ss.fffffff zzz";

        internal static string ConnectionString(DatabaseSettings settings)
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

        internal static string LoadTableNamesQuery(string database)
        {
            return "SELECT TABLE_NAME FROM " + SanitizeString(database) + ".INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
        }

        internal static string LoadTableColumnsQuery(string database, string table)
        {
            return 
                "SELECT " +
                "  col.TABLE_NAME, col.COLUMN_NAME, col.IS_NULLABLE, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, con.CONSTRAINT_NAME " +
                "FROM INFORMATION_SCHEMA.COLUMNS col " +
                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME AND con.TABLE_NAME = col.TABLE_NAME " +
                "WHERE col.TABLE_NAME='" + ExtractTableName(table) + "' " +
                "AND col.TABLE_CATALOG='" + SanitizeString(database) + "'";
        }

        internal static string SanitizeString(string val)
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

        internal static string ColumnToCreateString(Column col)
        {
            string ret =
                "[" + SanitizeString(col.Name) + "] ";

            switch (col.Type)
            {
                case DataType.Varchar:
                    ret += "[varchar](" + col.MaxLength + ") ";
                    break;
                case DataType.Nvarchar:
                    ret += "[nvarchar](" + col.MaxLength + ") ";
                    break;
                case DataType.Int:
                    ret += "[int] ";
                    break;
                case DataType.Long:
                    ret += "[bigint] ";
                    break;
                case DataType.Decimal:
                    ret += "[decimal](" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataType.Double:
                    ret += "[float](" + col.MaxLength + ") ";
                    break;
                case DataType.DateTime:
                    ret += "[datetime2] ";
                    break;
                case DataType.DateTimeOffset:
                    ret += "[datetimeoffset] ";
                    break;
                case DataType.Blob:
                    ret += "[varbinary](max) ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.PrimaryKey)
            {
                if (col.Type == DataType.Varchar || col.Type == DataType.Nvarchar)
                {
                    ret += "UNIQUE ";
                }
                else if (col.Type == DataType.Int || col.Type == DataType.Long)
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

        internal static Column GetPrimaryKeyColumn(List<Column> columns)
        {
            Column c = columns.FirstOrDefault(d => d.PrimaryKey);
            if (c == null || c == default(Column)) return null;
            return c;
        }

        internal static string CreateTableQuery(string tableName, List<Column> columns)
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

        internal static string DropTableQuery(string tableName)
        {
            string query = "IF OBJECT_ID('" + PreparedTableNameUnenclosed(tableName) + "', 'U') IS NOT NULL DROP TABLE " + PreparedTableName(tableName);
            return query;
        }

        internal static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
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

        internal static string InsertQuery(string tableName, string keys, string values)
        {
            string ret = 
                "INSERT INTO " + PreparedTableName(tableName) + " WITH (ROWLOCK) " + 
                "(" + keys + ") " + 
                "OUTPUT INSERTED.* " + 
                "VALUES " + 
                "(" + values + ") ";

            return ret;
        }

        internal static string InsertMultipleQuery(string tableName, string keys, List<string> values)
        {
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

        internal static string UpdateQuery(string tableName, string keyValueClause, Expr filter)
        {
            string ret =
                "UPDATE " + PreparedTableName(tableName) + " WITH (ROWLOCK) SET " +
                keyValueClause + " " +
                "OUTPUT INSERTED.* ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string DeleteQuery(string tableName, Expr filter)
        {
            string ret =
                "DELETE FROM " + PreparedTableName(tableName) + " WITH (ROWLOCK) ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE " + PreparedTableName(tableName);
        }

        internal static string ExistsQuery(string tableName, Expr filter)
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

        internal static string CountQuery(string tableName, string countColumnName, Expr filter)
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

        internal static string SumQuery(string tableName, string fieldName, string sumColumnName, Expr filter)
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

        internal static string DbTimestamp(DateTime ts)
        {
            return ts.ToString(TimestampFormat);
        }

        internal static string DbTimestampOffset(DateTimeOffset ts)
        {
            return ts.ToString(TimestampOffsetFormat);
        }

        internal static string PreparedFieldName(string s)
        {
            return "[" + s + "]";
        }

        internal static string PreparedStringValue(string s)
        {
            return "'" + SqlServerHelper.SanitizeString(s) + "'";
        }

        internal static string PreparedTableName(string s)
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

        internal static string PreparedTableNameUnenclosed(string s)
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

        internal static string ExtractTableName(string s)
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

        internal static string PreparedUnicodeValue(string s)
        {
            return "N" + PreparedStringValue(s);
        }

        internal static string ExpressionToWhereClause(Expr expr)
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

        private static string BuildOrderByClause(ResultOrder[] resultOrder)
        {
            if (resultOrder == null || resultOrder.Length < 0) return null;

            string ret = "ORDER BY ";

            for (int i = 0; i < resultOrder.Length; i++)
            {
                if (i > 0) ret += ", ";
                ret += SanitizeString(resultOrder[i].ColumnName) + " ";
                if (resultOrder[i].Direction == OrderDirection.Ascending) ret += "ASC";
                else if (resultOrder[i].Direction == OrderDirection.Descending) ret += "DESC";
            }

            ret += " ";
            return ret;
        }

        private static string RandomCharacters(int len)
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
    }
}
