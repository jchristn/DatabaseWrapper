using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.Sqlite
{
    internal static class SqliteHelper
    {
        internal static string TimestampFormat = "yyyy-MM-dd HH:mm:ss.ffffff";

        internal static string TimestampOffsetFormat = "yyyy-MM-dd HH:mm:ss.fffffff zzz";

        internal static string ConnectionString(DatabaseSettings settings)
        {
            return "Data Source=" + settings.Filename;
        }

        internal static string LoadTableNamesQuery()
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

        internal static string LoadTableColumnsQuery(string table)
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
                PreparedFieldName(SanitizeString(col.Name)) + " ";

            switch (col.Type)
            {
                case DataType.Varchar: 
                case DataType.Nvarchar:
                    ret += "VARCHAR(" + col.MaxLength + ") COLLATE NOCASE ";
                    break;
                case DataType.Int:
                    ret += "INTEGER ";
                    break;
                case DataType.Long:
                    ret += "BIGINT ";
                    break;
                case DataType.Decimal:
                    ret += "DECIMAL(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataType.Double:
                    ret += "REAL ";
                    break;
                case DataType.DateTime:
                case DataType.DateTimeOffset:
                    ret += "TEXT ";
                    break;
                case DataType.Blob:
                    ret += "BLOB ";
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

        internal static Column GetPrimaryKeyColumn(List<Column> columns)
        {
            Column c = columns.FirstOrDefault(d => d.PrimaryKey);
            if (c == null || c == default(Column)) return null;
            return c;
        }

        internal static string CreateTableQuery(string tableName, List<Column> columns)
        {
            string ret =
                "CREATE TABLE IF NOT EXISTS " + PreparedFieldName(SanitizeString(tableName)) + " " +
                "(";

            int added = 0;
            foreach (Column curr in columns)
            {
                if (added > 0) ret += ", ";
                ret += ColumnToCreateString(curr); 
                added++;
            }

            ret += ")";

            return ret;
        }

        internal static string DropTableQuery(string tableName)
        {
            return "DROP TABLE IF EXISTS " + PreparedFieldName(SanitizeString(tableName));
        }

        internal static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
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
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
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

            return query;
        }

        internal static string InsertQuery(string tableName, string keys, string values)
        {
            return
                "BEGIN TRANSACTION; " +
                "INSERT INTO " + PreparedFieldName(SanitizeString(tableName)) + " " +
                "(" + keys + ") " +
                "VALUES " +
                "(" + values + "); " +
                "SELECT last_insert_rowid() AS id; " +
                ";COMMIT;";
        }

        internal static string InsertMultipleQuery(string tableName, string keys, List<string> values)
        {
            string ret =
                "BEGIN TRANSACTION; " +
                "  INSERT INTO " + PreparedFieldName(SanitizeString(tableName)) + " " +
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
                ";COMMIT;";

            return ret;
        }

        internal static string UpdateQuery(string tableName, string keyValueClause, Expr filter)
        {
            string ret = 
                "BEGIN TRANSACTION; " +
                "UPDATE " + PreparedFieldName(SanitizeString(tableName)) + " SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            ret +=
                ";COMMIT;";

            return ret;
        }

        internal static string DeleteQuery(string tableName, Expr filter)
        {
            string ret =
                "DELETE FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string TruncateQuery(string tableName)
        {
            return "DELETE FROM " + PreparedFieldName(SanitizeString(tableName));
        }

        internal static string ExistsQuery(string tableName, Expr filter)
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
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            query += "LIMIT 1";
            return query;
        }

        internal static string CountQuery(string tableName, string countColumnName, Expr filter)
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

            //
            // select
            //
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS " + sumColumnName + " " +
                "FROM " + PreparedFieldName(SanitizeString(tableName)) + " ";

            //
            // expressions
            //
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
            return "`" + s + "`";
        }

        internal static string PreparedStringValue(string s)
        {
            return "'" + SqliteHelper.SanitizeString(s) + "'";
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
                    #region EndsWith

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
    }
}
