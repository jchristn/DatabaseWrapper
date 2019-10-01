using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    internal static class MssqlHelper
    {
        public static string ConnectionString(string serverIp, int serverPort, string username, string password, string instance, string database)
        {
            string ret = "";

            if (String.IsNullOrEmpty(username) && String.IsNullOrEmpty(password))
            {
                ret += "Data Source=" + serverIp;
                if (!String.IsNullOrEmpty(instance)) ret += "\\" + instance + "; ";
                else ret += "; ";
                ret += "Integrated Security=SSPI; ";
                ret += "Initial Catalog=" + database + "; ";
            }
            else
            {
                if (serverPort > 0)
                {
                    if (String.IsNullOrEmpty(instance)) ret += "Server=" + serverIp + "," + serverPort + "; ";
                    else ret += "Server=" + serverIp + "\\" + instance + "," + serverPort + "; ";
                }
                else
                {
                    if (String.IsNullOrEmpty(instance)) ret += "Server=" + serverIp + "; ";
                    else ret += "Server=" + serverIp + "\\" + instance + "; ";
                }

                ret += "Database=" + database + "; ";
                if (!String.IsNullOrEmpty(username)) ret += "User ID=" + username + "; ";
                if (!String.IsNullOrEmpty(password)) ret += "Password=" + password + "; ";
            }

            return ret;
        }

        public static string LoadTableNamesQuery(string database)
        {
            return "SELECT TABLE_NAME FROM " + database + ".INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
        }

        public static string LoadTableColumnsQuery(string database, string table)
        {
            return 
                "SELECT " +
                "  col.TABLE_NAME, col.COLUMN_NAME, col.IS_NULLABLE, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, con.CONSTRAINT_NAME " +
                "FROM INFORMATION_SCHEMA.COLUMNS col " +
                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME AND con.TABLE_NAME = col.TABLE_NAME " +
                "WHERE col.TABLE_NAME='" + table + "' " +
                "AND col.TABLE_CATALOG='" + database + "'";
        }

        public static string SanitizeString(string val)
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

        private static string ColumnToCreateString(Column col)
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

        private static Column GetPrimaryKeyColumn(List<Column> columns)
        {
            Column c = columns.FirstOrDefault(d => d.PrimaryKey);
            if (c == null || c == default(Column)) return null;
            return c;
        }

        public static string CreateTableQuery(string tableName, List<Column> columns)
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

        public static string DropTableQuery(string tableName)
        {
            string query = "IF OBJECT_ID('dbo." + SanitizeString(tableName) + "', 'U') IS NOT NULL DROP TABLE [" + SanitizeString(tableName) + "]";
            return query;
        }

        public static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
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
            if (filter != null) whereClause = filter.ToWhereClause(DbTypes.MsSql);
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

        public static string InsertQuery(string tableName, string keys, string values)
        {
            string ret = 
                "INSERT INTO [" + tableName + "] WITH (ROWLOCK) " + 
                "(" + keys + ") " + 
                "OUTPUT INSERTED.* " + 
                "VALUES " + 
                "(" + values + ") ";

            return ret;
        }

        public static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret =
                "UPDATE [" + tableName + "] WITH (ROWLOCK) SET " +
                keyValueClause + " " +
                "OUTPUT INSERTED.* ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.MsSql) + " ";

            return ret;
        }

        public static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM [" + tableName + "] WITH (ROWLOCK) ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.MsSql) + " ";

            return ret;
        }
    }
}
