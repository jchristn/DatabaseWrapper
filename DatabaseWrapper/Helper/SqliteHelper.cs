using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    internal static class SqliteHelper
    { 
        internal static string ConnectionString(string filename)
        {
            return "Data Source=" + filename + ";Version=3;Pooling=False";
        }

        internal static string LoadTableNamesQuery()
        {
            return "SELECT name AS TABLE_NAME FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%'; ";
        }

        internal static string LoadTableColumnsQuery(string table)
        {
            return
               "SELECT " +
               "    m.name AS TABLE_NAME,  " +
               "    p.cid AS COLUMN_ID, " +
               "    p.name AS COLUMN_NAME, " +
               "    p.type AS DATA_TYPE, " +
               "    p.pk AS IS_PRIMARY_KEY, " +
               "    p.[notnull] AS IS_NOT_NULLABLE " +
               "FROM sqlite_master m " +
               "LEFT OUTER JOIN pragma_table_info((m.name)) p " +
               "    ON m.name <> p.name " +
               "WHERE m.type = 'table' " +
               "    AND m.name = '" + table + "' " +
               "ORDER BY TABLE_NAME, COLUMN_ID "; 
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
                    ret += "VARCHAR(" + col.MaxLength + ") ";
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
            if (filter != null) whereClause = filter.ToWhereClause(DbTypes.Sqlite);
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
                "INSERT INTO " + tableName + " " +
                "(" + keys + ") " +
                "VALUES " +
                "(" + values + "); " +
                "SELECT last_insert_rowid() AS id; ";
        }

        internal static string UpdateQuery(string tableName, string keyValueClause, Expression filter)
        {
            string ret = 
                "UPDATE " + tableName + " SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.Sqlite) + " "; 
            return ret;
        }

        internal static string DeleteQuery(string tableName, Expression filter)
        {
            string ret =
                "DELETE FROM " + tableName + " ";

            if (filter != null) ret += "WHERE " + filter.ToWhereClause(DbTypes.Sqlite) + " ";

            return ret;
        }
    }
}
