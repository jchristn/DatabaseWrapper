using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Static helper methods for DatabaseWrapper.
    /// </summary>
    public class Helper
    {
        /// <summary>
        /// Determines if an object is of a List type.
        /// </summary>
        /// <param name="o">Object.</param>
        /// <returns>True if the object is of a List type.</returns>
        public static bool IsList(object o)
        {
            if (o == null) return false;
            return o is IList &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }

        /// <summary>
        /// Convert an object to a List object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>List object.</returns>
        public static List<object> ObjectToList(object obj)
        {
            if (obj == null) return null;
            List<object> ret = new List<object>();
            var enumerator = ((IEnumerable)obj).GetEnumerator();
            while (enumerator.MoveNext())
            {
                ret.Add(enumerator.Current);
            }
            return ret;
        }

        /// <summary>
        /// Determine if a DataTable is null or has no rows.
        /// </summary>
        /// <param name="table">DataTable.</param>
        /// <returns>True if DataTable is null or has no rows.</returns>
        public static bool DataTableIsNullOrEmpty(DataTable table)
        {
            if (table == null) return true;
            if (table.Rows.Count < 1) return true;
            return false;
        }

        /// <summary>
        /// Convert a DataTable to an object.
        /// </summary>
        /// <typeparam name="T">Type of object.</typeparam>
        /// <param name="table">DataTable.</param>
        /// <returns>Object of specified type.</returns>
        public static T DataTableToObject<T>(DataTable table) where T : new()
        {
            if (table == null) throw new ArgumentNullException(nameof(table));
            if (table.Rows.Count < 1) throw new ArgumentException("No rows in DataTable");
            foreach (DataRow r in table.Rows)
            {
                return DataRowToObject<T>(r);
            }
            return default(T);
        }

        /// <summary>
        /// Convert a DataRow to an object.
        /// </summary>
        /// <typeparam name="T">Type of object.</typeparam>
        /// <param name="row">DataRow.</param>
        /// <returns>Object of specified type.</returns>
        public static T DataRowToObject<T>(DataRow row) where T : new()
        {
            if (row == null) throw new ArgumentNullException(nameof(row));
            T item = new T();
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            foreach (var property in properties)
            {
                property.SetValue(item, row[property.Name], null);
            }
            return item;
        }

        /// <summary>
        /// Convert a DataTable to a List of dynamic objects.
        /// </summary>
        /// <param name="table">DataTable.</param>
        /// <returns>List of dynamic objects.</returns>
        public static List<dynamic> DataTableToListDynamic(DataTable table)
        {
            List<dynamic> ret = new List<dynamic>();
            if (table == null || table.Rows.Count < 1) return ret;

            foreach (DataRow curr in table.Rows)
            {
                dynamic dyn = new ExpandoObject();
                foreach (DataColumn col in table.Columns)
                {
                    var dic = (IDictionary<string, object>)dyn;
                    dic[col.ColumnName] = curr[col];
                }
                ret.Add(dyn);
            }

            return ret;
        }

        /// <summary>
        /// Convert a DataTable to a dynamic object.
        /// </summary>
        /// <param name="table">DataTable.</param>
        /// <returns>Dynamic object.</returns>
        public static dynamic DataTableToDynamic(DataTable table)
        {
            dynamic ret = new ExpandoObject();
            if (table == null || table.Rows.Count < 1) return ret;
            if (table.Rows.Count != 1) throw new ArgumentException("DataTable must contain only one row.");

            foreach (DataRow curr in table.Rows)
            {
                foreach (DataColumn col in table.Columns)
                {
                    var dic = (IDictionary<string, object>)ret;
                    dic[col.ColumnName] = curr[col];
                }

                return ret;
            }

            return ret;
        }

        /// <summary>
        /// Convert a DataTable to a List Dictionary.
        /// </summary>
        /// <param name="table">DataTable.</param>
        /// <returns>List Dictionary.</returns>
        public static List<Dictionary<string, object>> DataTableToListDictionary(DataTable table)
        {
            List<Dictionary<string, object>> ret = new List<Dictionary<string, object>>();
            if (table == null || table.Rows.Count < 1) return ret;

            foreach (DataRow curr in table.Rows)
            {
                Dictionary<string, object> currDict = new Dictionary<string, object>();

                foreach (DataColumn col in table.Columns)
                {
                    currDict.Add(col.ColumnName, curr[col]);
                }

                ret.Add(currDict);
            }

            return ret;
        }

        /// <summary>
        /// Convert a DataTable to a Dictionary.
        /// </summary>
        /// <param name="table">DataTable.</param>
        /// <returns>Dictionary.</returns>
        public static Dictionary<string, object> DataTableToDictionary(DataTable table)
        {
            Dictionary<string, object> ret = new Dictionary<string, object>();
            if (table == null || table.Rows.Count < 1) return ret;
            if (table.Rows.Count != 1) throw new ArgumentException("DataTable must contain only one row.");

            foreach (DataRow curr in table.Rows)
            {
                foreach (DataColumn col in table.Columns)
                {
                    ret.Add(col.ColumnName, curr[col]);
                }

                return ret;
            }

            return ret;
        }

        /// <summary>
        /// Determine if string contains extended characters.
        /// </summary>
        /// <param name="data">String.</param>
        /// <returns>True if string contains extended characters.</returns>
        public static bool IsExtendedCharacters(string data)
        {
            if (String.IsNullOrEmpty(data)) return false;
            foreach (char c in data)
            {
                if ((int)c > 255) return true;
            }
            return false;
        }
         
        /// <summary>
        /// Retrieve the DataType from the column type.
        /// </summary>
        /// <param name="s">String containing column type.</param>
        /// <returns>DataType.</returns>
        public static DataType DataTypeFromString(string s)
        {
            if (String.IsNullOrEmpty(s)) throw new ArgumentNullException(nameof(s));

            s = s.ToLower();
            if (s.Contains("(")) s = s.Substring(0, s.IndexOf("("));

            switch (s)
            {
                case "bigserial":               // pgsql
                case "bigint":                  // mssql
                    return DataType.Long;

                case "boolean":                 // pgsql
                case "bit":                     // mssql
                case "smallserial":             // pgsql
                case "smallest":                // pgsql
                case "tinyint":                 // mssql, mysql
                case "integer":                 // pgsql, sqlite
                case "int":                     // mssql, mysql
                case "smallint":                // mssql, mysql
                case "mediumint":               // mysql
                case "serial":                  // pgsql
                    return DataType.Int;

                case "real":                    // pgsql, sqlite
                case "double":                  // mysql
                case "double precision":        // pgsql
                case "float":                   // mysql
                    return DataType.Double;

                case "decimal":                 // mssql
                case "numeric":                 // mssql
                    return DataType.Decimal;

                case "timestamp without timezone":      // pgsql
                case "timestamp without time zone":     // pgsql
                case "time without timezone":           // pgsql
                case "time without time zone":          // pgsql
                case "time":                    // mssql, mysql
                case "date":                    // mssql, mysql
                case "datetime":                // mssql, mysql
                case "datetime2":               // mssql
                case "timestamp":               // mysql
                    return DataType.DateTime;

                case "time with timezone":              // pgsql
                case "time with time zone":             // pgsql
                case "timestamp with timezone":         // pgsql
                case "timestamp with time zone":        // pgsql
                case "datetimeoffset":          // mssql
                    return DataType.DateTimeOffset;

                case "enum":                    // mysql
                case "character":               // pgsql
                case "char":                    // mssql, mysql, pgsql
                case "text":                    // mssql, mysql, pgsql, sqlite
                case "varchar":                 // mssql, mysql, pgsql
                    return DataType.Varchar;

                case "character varying":       // pgsql
                case "nchar":
                case "ntext":
                case "nvarchar":
                    return DataType.Nvarchar;   // mssql

                case "blob":                    // sqlite, mysql
                case "tinyblob":                // mysql
                case "mediumblob":              // mysql
                case "longblob":                // mysql
                case "bytea":                   // pgsql
                case "varbinary":               // mssql
                    return DataType.Blob;

                default:
                    throw new ArgumentException("Unknown DataType: " + s);
            }
        }

        /// <summary>
        /// Convert byte array to hex string.
        /// </summary>
        /// <param name="bytes">Byte array.</param>
        /// <returns>Hex string.</returns>
        public static string ByteArrayToHexString(byte[] bytes)
        {
            if (bytes == null) return null;
            if (bytes.Length < 1) return "";
            StringBuilder hex = new StringBuilder(bytes.Length * 2);
            foreach (byte b in bytes) hex.AppendFormat("{0:x2}", b);
            return hex.ToString();
        }

        /// <summary>
        /// Convert hex string to byte array.
        /// </summary>
        /// <param name="hex">Hex string.</param>
        /// <returns>Byte array.</returns>
        public static byte[] HexStringToBytes(string hex)
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
        {
            if (String.IsNullOrEmpty(hex)) throw new ArgumentNullException(nameof(hex));
            if (hex.Length % 2 == 1) throw new ArgumentException("The supplied hex cannot have an odd number of digits.");

            byte[] arr = new byte[hex.Length >> 1];

            for (int i = 0; i < hex.Length >> 1; ++i)
            {
                arr[i] = (byte)((GetHexValue(hex[i << 1]) << 4) + (GetHexValue(hex[(i << 1) + 1])));
            }

            return arr;
        }

        private static int GetHexValue(char hex)
        {
            int val = (int)hex;
            // For uppercase A-F letters:
            // return val - (val < 58 ? 48 : 55);
            // For lowercase a-f letters:
            // return val - (val < 58 ? 48 : 87);
            // Or the two combined, but a bit slower:
            return val - (val < 58 ? 48 : (val < 97 ? 55 : 87));
        }
    }
}
