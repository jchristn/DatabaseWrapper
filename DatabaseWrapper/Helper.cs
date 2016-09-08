using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace DatabaseWrapper
{
    public class Helper
    {
        /// <summary>
        /// Determines if an object is a list.
        /// </summary>
        /// <param name="o">An object.</param>
        /// <returns>Boolean indicating if the object is a list.</returns>
        public static bool IsList(object o)
        {
            if (o == null) return false;
            return o is IList &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }

        /// <summary>
        /// Converts an object to a List object.
        /// </summary>
        /// <param name="o">An object.</param>
        /// <returns>A List object.</returns>
        public static List<object> ObjectToList(object o)
        {
            if (o == null) return null;
            List<object> ret = new List<object>();
            var enumerator = ((IEnumerable)o).GetEnumerator();
            while (enumerator.MoveNext())
            {
                ret.Add(enumerator.Current);
            }
            return ret;
        }

        /// <summary>
        /// Determines if a DataTable is null or empty.
        /// </summary>
        /// <param name="t">A DataTable.</param>
        /// <returns>Boolean indicating if the DataTable is null or empty.</returns>
        public static bool DataTableIsNullOrEmpty(DataTable t)
        {
            if (t == null) return true;
            if (t.Rows.Count < 1) return true;
            return false;
        }

        /// <summary>
        /// Converts a DataTable to an object of a given type.
        /// </summary>
        /// <typeparam name="T">The type of object to which the DataTable should be converted.</typeparam>
        /// <param name="t">A DataTable.</param>
        /// <returns>An object of type T containing values from the DataTable.</returns>
        public static T DataTableToObject<T>(DataTable t) where T : new()
        {
            if (t == null) throw new ArgumentNullException(nameof(t));
            if (t.Rows.Count < 1) throw new ArgumentException("No rows in DataTable");
            foreach (DataRow r in t.Rows)
            {
                return DataRowToObject<T>(r);
            }
            return default(T);
        }

        /// <summary>
        /// Converts a DataTable to a List of objects of a given type.
        /// </summary>
        /// <typeparam name="T">The type of object to which each DataRow within the DataTable should be converted.</typeparam>
        /// <param name="t">A DataTable.</param>
        /// <returns>A list of objects of type T containing values from each DataRow within the DataTable.</returns>
        public static List<T> DataTableToListObject<T>(DataTable t)
        {
            if (t == null) throw new ArgumentNullException(nameof(t));
            if (t.Rows.Count < 1) throw new ArgumentException("No rows in DataTable");

            var columnNames = t.Columns.Cast<DataColumn>()
                    .Select(c => c.ColumnName)
                    .ToList();
            var properties = typeof(T).GetProperties();
            return t.AsEnumerable().Select(row =>
            {
                var objT = Activator.CreateInstance<T>();
                foreach (var pro in properties)
                {
                    if (columnNames.Contains(pro.Name))
                    {
                        PropertyInfo pI = objT.GetType().GetProperty(pro.Name);
                        pro.SetValue(objT, row[pro.Name] == DBNull.Value ? null : Convert.ChangeType(row[pro.Name], pI.PropertyType));
                    }
                }
                return objT;
            }).ToList();
        }

        /// <summary>
        /// Convert a DataRow to an object of type T.
        /// </summary>
        /// <typeparam name="T">The type of object to which the DataRow should be converted.</typeparam>
        /// <param name="r">A DataRow.</param>
        /// <returns>An object of type T containing values from the DataRow.</returns>
        public static T DataRowToObject<T>(DataRow r) where T : new()
        {
            if (r == null) throw new ArgumentNullException(nameof(r));
            T item = new T();
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            foreach (var property in properties)
            {
                property.SetValue(item, r[property.Name], null);
            }
            return item;
        }

        /// <summary>
        /// Deserialize a JSON string to an object of type T.
        /// </summary>
        /// <typeparam name="T">The type of object to which the JSON should be deserialized.</typeparam>
        /// <param name="json">The JSON string.</param>
        /// <returns>An object of type T built using data from the JSON string.</returns>
        public static T DeserializeJson<T>(string json)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            return (T)ser.Deserialize<T>(json);
        }

        /// <summary>
        /// Deserialize a byte array containing JSON to an object of type T.
        /// </summary>
        /// <typeparam name="T">The type of object to which the JSON should be deserialized.</typeparam>
        /// <param name="bytes">The byte array containing JSON data.</param>
        /// <returns>An object of type T built using data from the JSON byte data.</returns>
        public static T DeserializeJson<T>(byte[] bytes)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            return (T)ser.Deserialize<T>(Encoding.UTF8.GetString(bytes));
        }

        /// <summary>
        /// Serialize an object to a JSON string.
        /// </summary>
        /// <param name="obj">An object.</param>
        /// <returns>A string containing JSON built from the supplied object.</returns>
        public static string SerializeJson(object obj)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            string json = ser.Serialize(obj);
            return json;
        }

        /// <summary>
        /// Check to see if extended characters are in use in a string.
        /// </summary>
        /// <param name="data">The string to evaluate.</param>
        /// <returns>A Boolean indicating whether or not extended characters were detected.</returns>
        public static bool IsExtendedCharacters(string data)
        {
            if (String.IsNullOrEmpty(data)) return false;
            foreach (char c in data)
            {
                if ((int)c > 128) return true;
            }
            return false;
        }
    }
}
