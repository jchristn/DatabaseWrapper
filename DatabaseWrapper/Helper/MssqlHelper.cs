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
                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME " +
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

        public static string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            string innerQuery = "";
            string outerQuery = "";
            string whereClause = "";
             
            //
            // select
            //
            if (indexStart != null)
            {
                if (String.IsNullOrEmpty(orderByClause)) throw new ArgumentNullException(nameof(orderByClause));
                innerQuery = "SELECT ROW_NUMBER() OVER ( " + orderByClause + " ) AS __row_num__, ";
            }
            else
            {
                if (maxResults > 0) innerQuery += "SELECT TOP " + maxResults + " ";
                else innerQuery += "SELECT ";
            }

            //
            // fields
            //
            if (returnFields == null || returnFields.Count < 1) innerQuery += "* ";
            else
            {
                int fieldsAdded = 0;
                foreach (string curr in returnFields)
                {
                    if (fieldsAdded == 0)
                    {
                        innerQuery += SanitizeString(curr);
                        fieldsAdded++;
                    }
                    else
                    {
                        innerQuery += "," + SanitizeString(curr);
                        fieldsAdded++;
                    }
                }
            }
            innerQuery += " ";

            //
            // table
            //
            innerQuery += "FROM " + tableName + " ";

            //
            // expressions
            //
            if (filter != null) whereClause = filter.ToWhereClause(DbTypes.MsSql);
            if (!String.IsNullOrEmpty(whereClause))
            {
                innerQuery += "WHERE " + whereClause + " ";
            }

            // 
            // order clause
            //
            if (indexStart == null)
            {
                if (!String.IsNullOrEmpty(orderByClause)) innerQuery += SanitizeString(orderByClause) + " ";
            }

            // 
            // wrap in outer query
            //
            if (indexStart != null)
            {
                if (indexStart == 0)
                {
                    outerQuery = "SELECT * FROM (" + innerQuery + ") AS row_constrained_result WHERE __row_num__ > " + indexStart + " ";
                    if (maxResults > 0) outerQuery += "AND __row_num__ <= " + (indexStart + maxResults) + " ";
                    outerQuery += "ORDER BY __row_num__ ";
                }
                else
                {
                    outerQuery = "SELECT * FROM (" + innerQuery + ") AS row_constrained_result WHERE __row_num__ >= " + indexStart + " ";
                    if (maxResults > 0) outerQuery += "AND __row_num__ < " + (indexStart + maxResults) + " ";
                    outerQuery += "ORDER BY __row_num__ ";
                }
            }
            else
            {
                outerQuery = innerQuery;
            }

            return outerQuery;
        }

        public static string InsertQuery(string tableName, string keys, string values)
        {
            string ret = 
                "INSERT INTO " + tableName + " WITH (ROWLOCK) " + 
                "(" + keys + ") " + 
                "OUTPUT INSERTED.* " + 
                "VALUES " + 
                "(" + values + ") ";

            return ret;
        }

        public static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret =
                "UPDATE " + tableName + " WITH (ROWLOCK) SET " +
                keyValueClause + " " +
                "OUTPUT INSERTED.* ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.MsSql) + " ";

            return ret;
        }

        public static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM " + tableName + " WITH (ROWLOCK) ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.MsSql) + " ";

            return ret;
        }
    }
}
