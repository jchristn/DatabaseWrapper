using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;

namespace DatabaseWrapper.SqlServer
{
    internal static class SqlServerHelper
    {
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
            return "SELECT TABLE_NAME FROM " + database + ".INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
        }

        internal static string LoadTableColumnsQuery(string database, string table)
        {
            return 
                "SELECT " +
                "  col.TABLE_NAME, col.COLUMN_NAME, col.IS_NULLABLE, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, con.CONSTRAINT_NAME " +
                "FROM INFORMATION_SCHEMA.COLUMNS col " +
                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME AND con.TABLE_NAME = col.TABLE_NAME " +
                "WHERE col.TABLE_NAME='" + table + "' " +
                "AND col.TABLE_CATALOG='" + database + "'";
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
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.PrimaryKey) ret += "IDENTITY(1,1) ";

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
                "CREATE TABLE [" + SanitizeString(tableName) + "] " +
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
                    "CONSTRAINT [PK_" + SanitizeString(tableName) + "] PRIMARY KEY CLUSTERED " +
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
            string query = "IF OBJECT_ID('dbo." + SanitizeString(tableName) + "', 'U') IS NOT NULL DROP TABLE [" + SanitizeString(tableName) + "]";
            return query;
        }

        internal static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            string query = "";
            string whereClause = "";

            if (indexStart != null || maxResults != null)
            {
                if (String.IsNullOrEmpty(orderByClause)) throw new ArgumentNullException(nameof(orderByClause));
            }

            //
            // select
            //
            query = "SELECT ";

            //
            // top
            //
            if (maxResults != null && indexStart == null)
            {
                query += "TOP " + maxResults + " ";
            }

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

            //
            // table
            //
            query += "FROM [" + SanitizeString(tableName) + "] ";

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
            if (!String.IsNullOrEmpty(orderByClause))
            {
                query += SanitizeString(orderByClause) + " ";
            }
            
            //
            // pagination
            //
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
                "INSERT INTO [" + SanitizeString(tableName) + "] WITH (ROWLOCK) " + 
                "(" + keys + ") " + 
                "OUTPUT INSERTED.* " + 
                "VALUES " + 
                "(" + values + ") ";

            return ret;
        }

        internal static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret =
                "UPDATE [" + SanitizeString(tableName) + "] WITH (ROWLOCK) SET " +
                keyValueClause + " " +
                "OUTPUT INSERTED.* ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM [" + SanitizeString(tableName) + "] WITH (ROWLOCK) ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        internal static string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE [" + SanitizeString(tableName) + "]";
        }

        internal static string ExistsQuery(string tableName, Expression filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT TOP 1 * " +
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

        internal static string CountQuery(string tableName, Expression filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT COUNT(*) AS __count__ " +
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

        internal static string SumQuery(string tableName, string fieldName, Expression filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS __sum__ " +
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
            return ts.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");
        }

        internal static string PreparedFieldname(string s)
        {
            return "[" + s + "]";
        }

        internal static string PreparedStringValue(string s)
        {
            return "'" + SqlServerHelper.SanitizeString(s) + "'";
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
                            PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + (PreparedStringValue(expr.RightTerm.ToString() + "%")) +
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
                            PreparedFieldname(expr.LeftTerm.ToString()) + " NOT LIKE " + (PreparedStringValue(expr.RightTerm.ToString() + "%")) +
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
                            PreparedFieldname(expr.LeftTerm.ToString()) + " LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.EndsWithNot:
                    #region EndsWithNot

                    if (expr.RightTerm == null) return null;
                    if (expr.RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldname(expr.LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.RightTerm.ToString()) +
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
    }
}
