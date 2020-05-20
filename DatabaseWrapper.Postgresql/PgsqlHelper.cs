using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using DatabaseWrapper.Core;

namespace DatabaseWrapper.Postgresql
{
    internal static class PgsqlHelper
    {
        internal static string ConnectionString(string serverIp, int serverPort, string username, string password, string database)
        {
            string ret = "";

            //
            // http://www.connectionstrings.com/postgresql/
            //
            // PgSQL does not use 'Instance'
            ret += "Server=" + serverIp + "; ";
            if (serverPort > 0) ret += "Port=" + serverPort + "; ";
            ret += "Database=" + database + "; ";
            if (!String.IsNullOrEmpty(username)) ret += "User ID=" + username + "; ";
            if (!String.IsNullOrEmpty(password)) ret += "Password=" + password + "; ";

            return ret;
        }

        internal static string LoadTableNamesQuery()
        {
            return "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
        }

        internal static string LoadTableColumnsQuery(string database, string table)
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
                "WHERE cols.TABLE_NAME = '" + table + "';";
        }

        internal static string SanitizeString(string val)
        {
            string tag = "$" + EscapeString(val, 2) + "$";
            return tag + val + tag;
        }

        internal static string EscapeString(string val, int numChar)
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

        internal static string SanitizeFieldname(string val)
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

        internal static string ColumnToCreateString(string tableName, Column col)
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
                case DataType.Varchar:
                case DataType.Nvarchar:
                    ret += "character varying(" + col.MaxLength + ") ";
                    break;
                case DataType.Int:
                    ret += "integer ";
                    break;
                case DataType.Long:
                    ret += "bigint ";
                    break;
                case DataType.Decimal:
                    ret += "numeric(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataType.Double:
                    ret += "float(" + col.MaxLength + ") ";
                    break;
                case DataType.DateTime:
                    ret += "timestamp with time zone ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
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
                "CREATE TABLE " + SanitizeFieldname(tableName) + " " +
                "(";

            int added = 0;
            foreach (Column curr in columns)
            {
                if (added > 0) query += ", ";
                query += ColumnToCreateString(tableName, curr);
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

        internal static string DropTableQuery(string tableName)
        {
            string query = "DROP TABLE IF EXISTS " + SanitizeFieldname(tableName) + " ";
            return query;
        }

        internal static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            string outerQuery = "";
            string whereClause = "";

            //
            // SELECT
            //
            outerQuery += "SELECT ";

            //
            // fields
            //
            if (returnFields == null || returnFields.Count < 1) outerQuery += "* ";
            else
            {
                int fieldsAdded = 0;
                foreach (string curr in returnFields)
                {
                    if (fieldsAdded == 0)
                    {
                        outerQuery += "\"" + SanitizeFieldname(curr) + "\"";
                        fieldsAdded++;
                    }
                    else
                    {
                        outerQuery += ",\"" + SanitizeFieldname(curr) + "\"";
                        fieldsAdded++;
                    }
                }
            }
            outerQuery += " ";

            //
            // table
            //
            outerQuery += "FROM " + SanitizeFieldname(tableName) + " ";

            //
            // expressions
            //
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                outerQuery += "WHERE " + whereClause + " ";
            }

            // 
            // order clause
            //
            if (!String.IsNullOrEmpty(orderByClause)) outerQuery += PreparedOrderByClause(orderByClause) + " ";

            //
            // limit
            //
            if (maxResults > 0)
            {
                if (indexStart != null && indexStart >= 0)
                {
                    outerQuery += "OFFSET " + indexStart + " LIMIT " + maxResults;
                }
                else
                {
                    outerQuery += "LIMIT " + maxResults;
                }
            }

            return outerQuery;
        }

        internal static string PreparedOrderByClause(string val)
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

        internal static string InsertQuery(string tableName, string keys, string values)
        {
            string ret =
                "INSERT INTO " + SanitizeFieldname(tableName) + " " +
                "(" + keys + ") " +
                "VALUES " +
                "(" + values + ") " +
                "RETURNING *;"; 
            return ret;
        }

        internal static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret =
                "UPDATE " + SanitizeFieldname(tableName) + " SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";
            ret += "RETURNING *";

            return ret;
        }

        internal static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM " + SanitizeFieldname(tableName) + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE " + SanitizeFieldname(tableName) + " ";
        }

        internal static string PreparedFieldname(string s)
        {
            return "\"" + s + "\"";
        }

        internal static string PreparedStringValue(string s)
        {
            // uses $xx$ escaping
            return PgsqlHelper.SanitizeString(s);
        }

        internal static string PreparedUnicodeValue(string s)
        {
            return "U&" + PreparedStringValue(s);
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
                    && expr.Operator != Operators.EndsWith)
                {
                    //
                    // These operators will add the left term
                    //
                    clause += PreparedFieldname(expr.LeftTerm.ToString()) + " ";
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
                    clause += " IN ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        clause += "(";
                        foreach (object currObj in inTempList)
                        {
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
                    }
                    break;

                #endregion

                case Operators.NotIn:
                    #region NotIn

                    if (expr.RightTerm == null) return null;
                    int notInAdded = 0;
                    if (!Helper.IsList(expr.RightTerm)) return null;
                    List<object> notInTempList = Helper.ObjectToList(expr.RightTerm);
                    clause += " NOT IN ";
                    if (expr.RightTerm is Expression)
                    {
                        clause += ExpressionToWhereClause((Expression)expr.RightTerm);
                    }
                    else
                    {
                        clause += "(";
                        foreach (object currObj in notInTempList)
                        {
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
                    }
                    break;

                #endregion

                case Operators.Contains:
                    #region Contains

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            "OR " + PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString() + "%") +
                            "OR " + PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
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
                            PreparedFieldname(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            "OR " + PreparedFieldname(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString() + "%") +
                            "OR " + PreparedFieldname(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
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
                            PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue(expr.RightTerm.ToString() + "%") +
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
                            PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + "%" + PreparedStringValue(expr.RightTerm.ToString()) +
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

        internal static string DbTimestamp(DateTime ts)
        {
            return ts.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");
        } 
    }
}
