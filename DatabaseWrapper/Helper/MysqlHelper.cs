using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace DatabaseWrapper
{
    internal static class MysqlHelper
    {
        internal static string ConnectionString(string serverIp, int serverPort, string username, string password, string database)
        {
            string ret = "";

            //
            // http://www.connectionstrings.com/mysql/
            //
            // MySQL does not use 'Instance'
            ret += "Server=" + serverIp + "; ";
            if (serverPort > 0) ret += "Port=" + serverPort + "; ";
            ret += "Database=" + database + "; ";
            if (!String.IsNullOrEmpty(username)) ret += "Uid=" + username + "; ";
            if (!String.IsNullOrEmpty(password)) ret += "Pwd=" + password + "; ";

            return ret;
        }

        internal static string LoadTableNamesQuery()
        {
            return "SHOW TABLES";
        }

        internal static string LoadTableColumnsQuery(string database, string table)
        {
            return
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
                "TABLE_NAME='" + table + "' " +
                "AND TABLE_SCHEMA='" + database + "'";
        }

        internal static string SanitizeString(string val)
        {
            string ret = "";
            ret = MySqlHelper.EscapeString(val);
            return ret;
        }

        internal static string ColumnToCreateString(Column col)
        { 
            string ret =
                "`" + SanitizeString(col.Name) + "` ";

            switch (col.Type)
            {
                case DataType.Varchar:
                case DataType.Nvarchar:
                    ret += "varchar(" + col.MaxLength + ") ";
                    break;
                case DataType.Int:
                case DataType.Long:
                    ret += "int(" + col.MaxLength + ") ";
                    break;
                case DataType.Decimal:
                    ret += "decimal(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataType.Double:
                    ret += "float(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataType.DateTime:
                    ret += "datetime ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.Nullable) ret += "NULL ";
            else ret += "NOT NULL ";

            if (col.PrimaryKey) ret += "AUTO_INCREMENT ";

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
                "CREATE TABLE `" + SanitizeString(tableName) + "` " +
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
                    "," +
                    "PRIMARY KEY (`" + SanitizeString(primaryKey.Name) + "`)";
            }

            query +=
                ") " +
                "ENGINE=InnoDB " +
                "AUTO_INCREMENT=1 " +
                "DEFAULT CHARSET=utf8mb4 " +
                "COLLATE=utf8mb4_0900_ai_ci";

            return query;
        }

        internal static string DropTableQuery(string tableName)
        {
            string query = "DROP TABLE IF EXISTS `" + SanitizeString(tableName) + "`";
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
                        outerQuery += SanitizeString(curr);
                        fieldsAdded++;
                    }
                    else
                    {
                        outerQuery += "," + SanitizeString(curr);
                        fieldsAdded++;
                    }
                }
            }
            outerQuery += " ";

            //
            // table
            //
            outerQuery += "FROM `" + tableName + "` ";

            //
            // expressions
            //
            if (filter != null) whereClause = filter.ToWhereClause(DbTypes.MySql);
            if (!String.IsNullOrEmpty(whereClause))
            {
                outerQuery += "WHERE " + whereClause + " ";
            }

            // 
            // order clause
            //
            if (!String.IsNullOrEmpty(orderByClause)) outerQuery += SanitizeString(orderByClause) + " ";

            //
            // limit
            //
            if (maxResults > 0)
            {
                if (indexStart != null && indexStart >= 0)
                {
                    outerQuery += "LIMIT " + indexStart + "," + maxResults;
                }
                else
                {
                    outerQuery += "LIMIT " + maxResults;
                }
            }

            return outerQuery;
        }

        internal static string InsertQuery(string tableName, string keys, string values)
        {
            string ret =
                "START TRANSACTION; " +
                "INSERT INTO `" + tableName + "` " +
                "(" + keys + ") " + 
                "VALUES " + 
                "(" + values + "); " + 
                "SELECT LAST_INSERT_ID() AS id; " + 
                "COMMIT; ";

            return ret;
        }

        internal static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret =
                "UPDATE `" + tableName + "` SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.MySql) + " ";
            
            return ret;
        }

        internal static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM `" + tableName + "` ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.MySql) + " ";

            return ret;
        }
    }
}