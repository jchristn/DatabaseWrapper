using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;

namespace DatabaseWrapper.Sqlite
{
    internal static class SqliteHelper
    {
        internal static string TimestampFormat = "yyyy-MM-dd HH:mm:ss.ffffff";

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
                SanitizeString(col.Name) + " ";

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
                    ret += "TEXT ";
                    break;
                case DataType.Blob:
                    ret += "BLOB ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.PrimaryKey) ret += "PRIMARY KEY AUTOINCREMENT "; 
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
                "CREATE TABLE IF NOT EXISTS " + SanitizeString(tableName) + " " +
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
            return "DROP TABLE IF EXISTS '" + SanitizeString(tableName) + "'";
        }

        internal static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, ResultOrder[] resultOrder)
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
            query += "FROM " + SanitizeString(tableName) + " ";

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
                "INSERT INTO " + SanitizeString(tableName) + " " +
                "(" + keys + ") " +
                "VALUES " +
                "(" + values + "); " +
                "SELECT last_insert_rowid() AS id; ";
        }

        internal static string InsertMultipleQuery(string tableName, string keys, List<string> values)
        {
            string ret =
                "BEGIN TRANSACTION; " +
                "  INSERT INTO " + SanitizeString(tableName) + " " +
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

        internal static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret = 
                "UPDATE " + SanitizeString(tableName) + " SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " "; 
            return ret;
        }

        internal static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM " + SanitizeString(tableName) + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string TruncateQuery(string tableName)
        {
            return "DELETE FROM " + SanitizeString(tableName);
        }

        internal static string ExistsQuery(string tableName, Expression filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT * " +
                "FROM " + SanitizeString(tableName) + " ";

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

        internal static string CountQuery(string tableName, string countColumnName, Expression filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT COUNT(*) AS " + countColumnName + " " +
                "FROM " + SanitizeString(tableName) + " ";

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

        internal static string SumQuery(string tableName, string fieldName, string sumColumnName, Expression filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS " + sumColumnName + " " +
                "FROM " + SanitizeString(tableName) + " ";

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

        internal static string PreparedFieldName(string s)
        {
            return "`" + s + "`";
        }

        internal static string PreparedStringValue(string s)
        {
            return "'" + SqliteHelper.SanitizeString(s) + "'";
        }

        internal static string PreparedUnicodeValue(string s)
        {
            return "N" + PreparedStringValue(s);
        }

        internal static string ExpressionToWhereClause(Expression expr)
        {
            if (expr == null) return null;

            string clause = "";

            if (expr.LeftTerm == null) return null;

            clause += "(";

            if (expr.LeftTerm is Expression)
            {
                clause += ExpressionToWhereClause((Expression)expr.LeftTerm) + " ";
            }
            else
            {
                if (!(expr.LeftTerm is string))
                {
                    throw new ArgumentException("Left term must be of type Expression or String");
                }

                if (expr.Operator != Operators.Contains
                    && expr.Operator != Operators.ContainsNot
                    && expr.Operator != Operators.StartsWith
                    && expr.Operator != Operators.StartsWithNot
                    && expr.Operator != Operators.EndsWith
                    && expr.Operator != Operators.EndsWithNot)
                {
                    //
                    // These operators will add the left term
                    //
                    clause += PreparedFieldName(expr.LeftTerm.ToString()) + " ";
                }
            }

            switch (expr.Operator)
            {
                #region Process-By-Operators

                case Operators.And:
                    #region And

                    if (expr.RightTerm == null) return null;
                    clause += "AND ";

                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.Or:
                    #region Or

                    if (expr.RightTerm == null) return null;
                    clause += "OR ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.Equals:
                    #region Equals

                    if (expr.RightTerm == null) return null;
                    clause += "= ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.NotEquals:
                    #region NotEquals

                    if (expr.RightTerm == null) return null;
                    clause += "<> ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.In:
                    #region In

                    if (expr.RightTerm == null) return null;
                    int inAdded = 0;
                    if (!Helper.IsList(expr.RightTerm)) return null;
                    List<object> inTempList = Helper.ObjectToList(expr.RightTerm);
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

                case Operators.NotIn:
                    #region NotIn

                    if (expr.RightTerm == null) return null;
                    int notInAdded = 0;
                    if (!Helper.IsList(expr.RightTerm)) return null;
                    List<object> notInTempList = Helper.ObjectToList(expr.RightTerm);
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

                case Operators.Contains:
                    #region Contains

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            "OR " + PreparedFieldName(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString() + "%") +
                            "OR " + PreparedFieldName(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.ContainsNot:
                    #region ContainsNot

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            "OR " + PreparedFieldName(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString() + "%") +
                            "OR " + PreparedFieldName(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.StartsWith:
                    #region StartsWith

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.StartsWithNot:
                    #region StartsWithNot

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.EndsWith:
                    #region EndsWith

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.EndsWithNot:
                    #region EndsWith

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.GreaterThan:
                    #region GreaterThan

                    if (expr.RightTerm == null) return null;
                    clause += "> ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.GreaterThanOrEqualTo:
                    #region GreaterThanOrEqualTo

                    if (expr.RightTerm == null) return null;
                    clause += ">= ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.LessThan:
                    #region LessThan

                    if (expr.RightTerm == null) return null;
                    clause += "< ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.LessThanOrEqualTo:
                    #region LessThanOrEqualTo

                    if (expr.RightTerm == null) return null;
                    clause += "<= ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        if (expr.RightTerm is DateTime || expr.RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(Convert.ToDateTime(expr.RightTerm)) + "'";
                        }
                        else if (expr.RightTerm is int || expr.RightTerm is long || expr.RightTerm is decimal)
                        {
                            clause += expr.RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.IsNull:
                    #region IsNull

                    clause += " IS NULL";
                    break;

                #endregion

                case Operators.IsNotNull:
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
