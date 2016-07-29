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
        public static bool IsList(object o)
        {
            if (o == null) return false;
            return o is IList &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }

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

        public static bool DataTableIsNullOrEmpty(DataTable t)
        {
            if (t == null) return true;
            if (t.Rows.Count < 1) return true;
            return false;
        }

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

        public static T DeserializeJson<T>(string json)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            return (T)ser.Deserialize<T>(json);
        }

        public static T DeserializeJson<T>(byte[] bytes)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            return (T)ser.Deserialize<T>(Encoding.UTF8.GetString(bytes));
        }

        public static string SerializeJson(object obj)
        {
            JavaScriptSerializer ser = new JavaScriptSerializer();
            ser.MaxJsonLength = Int32.MaxValue;
            string json = ser.Serialize(obj);
            return json;
        }

    }
}
