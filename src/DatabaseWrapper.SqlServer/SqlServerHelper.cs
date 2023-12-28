using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using static System.FormattableString;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.SqlServer
{
    using QueryAndParameters = System.ValueTuple<string, IEnumerable<KeyValuePair<string,object>>>;

    /// <summary>
    /// SQL Server implementation of helper properties and methods.
    /// </summary>
    public class SqlServerHelper : DatabaseHelperBase
    {
        #region Public-Members

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Build a connection string from DatabaseSettings.
        /// </summary>
        /// <param name="settings">Settings.</param>
        /// <returns>String.</returns>
        public override string GenerateConnectionString(DatabaseSettings settings)
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

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public override string RetrieveTableNamesQuery(string database)
        {
            return "SELECT TABLE_NAME FROM " + SanitizeString(database) + ".INFORMATION_SCHEMA.Tables WHERE TABLE_TYPE = 'BASE TABLE'";
        }

        /// <summary>
        /// Query to retrieve the list of columns for a table.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <param name="table">Table name.</param>
        /// <returns></returns>
        public override string RetrieveTableColumnsQuery(string database, string table)
        {
            return 
                "SELECT " +
                "  col.TABLE_NAME, col.COLUMN_NAME, col.IS_NULLABLE, col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, con.CONSTRAINT_NAME " +
                "FROM INFORMATION_SCHEMA.COLUMNS col " +
                "LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE con ON con.COLUMN_NAME = col.COLUMN_NAME AND con.TABLE_NAME = col.TABLE_NAME " +
                "WHERE col.TABLE_NAME='" + ExtractTableName(table) + "' " +
                "AND col.TABLE_CATALOG='" + SanitizeString(database) + "'";
        }

        /// <summary>
        /// Method to sanitize a string.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>String.</returns>
        public override string SanitizeString(string val)
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

        /// <summary>
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public override string ColumnToCreateQuery(Column col)
        {
            string ret =
                "[" + SanitizeString(col.Name) + "] ";

            switch (col.Type)
            {
                case DataTypeEnum.Varchar:
                    ret += "[varchar](" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.Nvarchar:
                    ret += "[nvarchar](" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.Guid:
                    ret += "[varchar](36) ";
                    break;
                case DataTypeEnum.Int:
                    ret += "[int] ";
                    break;
                case DataTypeEnum.Long:
                    ret += "[bigint] ";
                    break;
                case DataTypeEnum.Decimal:
                    ret += "[decimal](" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.Double:
                    ret += "[float](" + col.MaxLength + ") ";
                    break;
                case DataTypeEnum.DateTime:
                    ret += "[datetime2] ";
                    break;
                case DataTypeEnum.DateTimeOffset:
                    ret += "[datetimeoffset] ";
                    break;
                case DataTypeEnum.Blob:
                    ret += "[varbinary](max) ";
                    break;
                case DataTypeEnum.Boolean:
                case DataTypeEnum.TinyInt:
                    ret += "[tinyint] ";
                    break;
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.PrimaryKey)
            {
                if (col.Type == DataTypeEnum.Varchar || col.Type == DataTypeEnum.Nvarchar)
                {
                    ret += "UNIQUE ";
                }
                else if (col.Type == DataTypeEnum.Int || col.Type == DataTypeEnum.Long)
                {
                    ret += "IDENTITY(1,1) ";
                }
                else
                {
                    throw new ArgumentException("Primary key column '" + col.Name + "' is of an unsupported type: " + col.Type.ToString());
                }
            }

            if (col.Nullable) ret += "NULL ";
            else ret += "NOT NULL ";

            return ret;
        }

        /// <summary>
        /// Retrieve the primary key column from a list of columns.
        /// </summary>
        /// <param name="columns">List of Column.</param>
        /// <returns>Column.</returns>
        public override Column GetPrimaryKeyColumn(List<Column> columns)
        {
            Column c = columns.FirstOrDefault(d => d.PrimaryKey);
            if (c == null || c == default(Column)) return null;
            return c;
        }

        /// <summary>
        /// Retrieve a query used for table creation.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="columns">List of columns.</param>
        /// <returns>String.</returns>
        public override string CreateTableQuery(string tableName, List<Column> columns)
        {
            string query =
                "CREATE TABLE " + PreparedTableName(tableName) + " " +
                "(";

            int added = 0;
            foreach (Column curr in columns)
            {
                if (added > 0) query += ", ";
                query += ColumnToCreateQuery(curr); 
                added++;
            }

            Column primaryKey = GetPrimaryKeyColumn(columns);
            if (primaryKey != null)
            {
                query +=
                    ", " +
                    "CONSTRAINT [PK_" + ExtractTableName(tableName) + "] PRIMARY KEY CLUSTERED " +
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

        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string DropTableQuery(string tableName)
        {
            string query = "IF OBJECT_ID('" + PreparedTableNameUnenclosed(tableName) + "', 'U') IS NOT NULL DROP TABLE " + PreparedTableName(tableName);
            return query;
        }

        /// <summary>
        /// Retrieve a query used for selecting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="indexStart">Index start.</param>
        /// <param name="maxResults">Maximum number of results to retrieve.</param>
        /// <param name="returnFields">List of field names to return.</param>
        /// <param name="filter">Expression filter.</param>
        /// <param name="resultOrder">Result order.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        {
            string query = "";
            string whereClause = "";
              
            // select 
            query = "SELECT ";
             
            // top 
            if (maxResults != null && indexStart == null)
            {
                query += "TOP " + maxResults + " ";
            }
             
            // fields 
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

            // table
            query += "FROM " + PreparedTableName(tableName) + " ";

            // expressions
            var parameters_list = new List<KeyValuePair<string,object>>();
            if (filter != null) {
                whereClause = ExpressionToWhereClause(filter, parameters_list);
            }
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            // order clause
            query += BuildOrderByClause(resultOrder);

            // pagination
            if (indexStart != null && maxResults != null)
            {
                query += "OFFSET " + indexStart + " ROWS ";
                query += "FETCH NEXT " + maxResults + " ROWS ONLY ";
            }
            else if (indexStart != null)
            {
                query += "OFFSET " + indexStart + " ROWS ";
            }

            return (query, parameters_list);
        }

        /// <summary>
        /// Retrieve a query used for inserting data into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters InsertQuery(string tableName, Dictionary<string, object> keyValuePairs)
        {
            var s_ret = new StringBuilder();
            var o_ret = keyValuePairs.Select(kv => new KeyValuePair<string,object>("@F_" + kv.Key, kv.Value));
            s_ret.Append("INSERT INTO " + PreparedTableName(tableName) + " WITH (ROWLOCK) " + "(");
            s_ret.Append(string.Join(", ", keyValuePairs.Keys.Select(k => PreparedFieldName(k))));
            s_ret.Append(") " +
                "OUTPUT INSERTED.* " +
                "VALUES " +
                "(");
            s_ret.Append(string.Join(", ", o_ret.Select(k => k.Key)));
            s_ret.Append(") ");
            return (s_ret.ToString(), o_ret);
        }

        /// <summary>
        /// Retrieve a query for inserting multiple rows into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters InsertMultipleQuery(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            ValidateInputDictionaries(keyValuePairList);

            string txn = "txn_" + RandomCharacters(12);
            var ret = new StringBuilder();
            var ret_values = new List<KeyValuePair<string,object>>();
            ret.Append(
                "BEGIN TRANSACTION [" + txn + "] " +
                " BEGIN TRY " +
                "  INSERT INTO " + PreparedTableName(tableName) + " WITH (ROWLOCK) " +
                "  (" + string.Join(", ", keyValuePairList[0].Keys.Select(k => PreparedFieldName(k))) + ") " +
                "  VALUES ");

            for (int i_dict=0; i_dict<keyValuePairList.Count; ++i_dict)
            {
                var dict = keyValuePairList[i_dict];
                var prefix = Invariant($"@F{i_dict}_");
                var this_round = dict.Select(kv => new KeyValuePair<string, object>(prefix + kv.Key, kv.Value));
                if (i_dict>0) {
                    ret.Append(", ");
                }
                ret.Append("(");
                ret.Append(string.Join(", ", this_round.Select(kv => kv.Key)));
                ret.Append(")");
                ret_values.AddRange(this_round);
            }

            ret.Append(
                "  COMMIT TRANSACTION [" + txn + "] " +
                " END TRY " +
                " BEGIN CATCH " +
                "  ROLLBACK TRANSACTION [" + txn + "] " +
                " END CATCH ");
            return (ret.ToString(), ret_values);
        }

        /// <summary>
        /// Retrieve a query for updating data in a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        /// <returns>String.</returns>
        public override QueryAndParameters UpdateQuery(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            const string FIELD_PREFIX = "@F";
            var parameters = keyValuePairs.Select(kv => new KeyValuePair<string, object>(FIELD_PREFIX + kv.Key, kv.Value)).ToList();
            string ret =
                "UPDATE " + PreparedTableName(tableName) + " WITH (ROWLOCK) SET " +
                string.Join(", ", parameters.Select(kv => kv.Key.Substring(FIELD_PREFIX.Length) + "=" + kv.Key)) + " " +
                "OUTPUT INSERTED.* ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter, parameters) + " ";
            return (ret, parameters);
        }

        /// <summary>
        /// Retrieve a query for deleting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters DeleteQuery(string tableName, Expr filter)
        {
            string ret =
                "DELETE FROM " + PreparedTableName(tableName) + " WITH (ROWLOCK) ";

            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter, parameters) + " ";

            return (ret, parameters);
        }

        /// <summary>
        /// Retrieve a query for truncating a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE " + PreparedTableName(tableName);
        }

        /// <summary>
        /// Retrieve a query for determing whether data matching specified conditions exists.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters ExistsQuery(string tableName, Expr filter)
        {
            string query = "";
            string whereClause = "";
             
            // select 
            query =
                "SELECT TOP 1 * " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }
             
            return (query, parameters);
        }

        /// <summary>
        /// Retrieve a query that returns a count of the number of rows matching the supplied conditions.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="countColumnName">Column name to use to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters CountQuery(string tableName, string countColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";
             
            // select 
            query =
                "SELECT COUNT(*) AS " + countColumnName + " " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            return (query, parameters);
        }

        /// <summary>
        /// Retrieve a query that sums the values found in the specified field.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="fieldName">Column containing values to sum.</param>
        /// <param name="sumColumnName">Column name to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override QueryAndParameters SumQuery(string tableName, string fieldName, string sumColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";
             
            // select 
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS " + sumColumnName + " " +
                "FROM " + PreparedTableName(tableName) + " ";
             
            // expressions 
            var parameters = new List<KeyValuePair<string, object>>();
            if (filter != null) whereClause = ExpressionToWhereClause(filter, parameters);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }

            return (query, parameters);
        }

        /// <summary>
        /// Extract the table name from an encapsulated name.
        /// </summary>
        /// <param name="s">String.</param>
        /// <returns>String.</returns>
        public string ExtractTableName(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length != 2) throw new ArgumentException("Table name must have either zero or one period '.' character");
                return SanitizeString(parts[1]);
            }
            else
            {
                return SanitizeString(s);
            }
        }

        #endregion

        #region Private-Members

        private string PreparedFieldName(string fieldName)
        {
            return "[" + fieldName + "]";
        }

        /// <summary>
        /// Append object as the parameter to the parameters list.
        /// 
        /// Returns the parameter name, that is to be used in the query currently under construction.
        /// </summary>
        /// <param name="parameters">List of the parameters.</param>
        /// <param name="o">Object to be added.</param>
        /// <returns>Parameter name.</returns>
        static string AppendParameter(List<KeyValuePair<string, object>> parameters, object o)
        {
            var r = Invariant($"@E{parameters.Count}");
            parameters.Add(new KeyValuePair<string, object>(r, o));
            return r;
        }

        /// <summary>
        /// Append object as the parameter to the parameters list.
        /// Use the object type to apply conversions, if any needed.
        /// 
        /// Returns the parameter name, that is to be used in the query currently under construction.
        /// </summary>
        /// <param name="parameters">List of the parameters.</param>
        /// <param name="untypedObject">Object to be added.</param>
        /// <returns>Parameter name.</returns>
        static string AppendParameterByType(List<KeyValuePair<string, object>> parameters, object untypedObject)
        {
            if (untypedObject is DateTime || untypedObject is DateTime?)
            {
                return AppendParameter(parameters, Convert.ToDateTime(untypedObject));
            }
            if (untypedObject is int || untypedObject is long || untypedObject is decimal)
            {
                return AppendParameter(parameters, untypedObject);
            }
            if (untypedObject is bool)
            {
                return AppendParameter(parameters, (bool)untypedObject ? 1 : 0);
            }
            if (untypedObject is byte[])
            {
                return AppendParameter(parameters, untypedObject);
            }
            return AppendParameter(parameters, untypedObject);
        }

        static readonly Dictionary<OperatorEnum, string> BinaryOperators = new Dictionary<OperatorEnum, string>() {
            { OperatorEnum.And, "AND" },
            { OperatorEnum.Or, "OR" },
            { OperatorEnum.Equals, "=" },
            { OperatorEnum.NotEquals, "<>" },
            { OperatorEnum.GreaterThan, ">" },
            { OperatorEnum.GreaterThanOrEqualTo, ">=" },
            { OperatorEnum.LessThan, "<" },
            { OperatorEnum.LessThanOrEqualTo, "<=" },
        };

        static readonly Dictionary<OperatorEnum, string> InListOperators = new Dictionary<OperatorEnum, string>() {
            { OperatorEnum.In, "IN" },
            { OperatorEnum.NotIn, "NOT IN" },
        };

        static readonly Dictionary<OperatorEnum, (string,string)> ContainsOperators = new Dictionary<OperatorEnum, (string,string)>() {
            { OperatorEnum.Contains, ("LIKE", "OR") },
            { OperatorEnum.ContainsNot, ("NOT LIKE", "AND") },
        };
        static readonly Dictionary<OperatorEnum, (string,string,string)> LikeOperators = new Dictionary<OperatorEnum, (string,string,string)>() {
            { OperatorEnum.StartsWith, ("LIKE", "", "%") },
            { OperatorEnum.StartsWithNot, ("NOT LIKE", "", "%") },
            { OperatorEnum.EndsWith, ("LIKE", "%", "") },
            { OperatorEnum.EndsWithNot, ("NOT LIKE", "%", "") },
        };
        static readonly Dictionary<OperatorEnum, string> IsNullOperators = new Dictionary<OperatorEnum, string>() {
            { OperatorEnum.IsNull, "IS NULL" },
            { OperatorEnum.IsNotNull, "IS NOT NULL" },
        };


        private string ExpressionToWhereClause(Expr expr, List<KeyValuePair<string, object>> parameters)
        {
            if (expr == null) return null;

            string clause = "";

            if (expr.Left == null) return null;

            clause += "(";

            if (expr.Left is Expr)
            {
                clause += ExpressionToWhereClause((Expr)expr.Left, parameters) + " ";
            }
            else
            {
                if (!(expr.Left is string))
                {
                    throw new ArgumentException("Left term must be of type Expression or String");
                }

                if (expr.Operator != OperatorEnum.Contains
                    && expr.Operator != OperatorEnum.ContainsNot
                    && expr.Operator != OperatorEnum.StartsWith
                    && expr.Operator != OperatorEnum.StartsWithNot
                    && expr.Operator != OperatorEnum.EndsWith
                    && expr.Operator != OperatorEnum.EndsWithNot)
                {
                    //
                    // These operators will add the left term
                    //
                    clause += PreparedFieldName(expr.Left.ToString()) + " ";
                }
            }

            string operator_name;
            string logic_operator_name;
            string prefix;
            string suffix;
            (string, string) operator_pair;
            (string, string, string) operator_triple;
            if (BinaryOperators.TryGetValue(expr.Operator, out operator_name))
            {
                if (expr.Right == null) return null;
                clause +=  operator_name + " ";

                if (expr.Right is Expr)
                {
                    clause += ExpressionToWhereClause((Expr)expr.Right, parameters);
                }
                else
                {
                    clause += AppendParameterByType(parameters, expr.Right);
                }
            }
            else if (InListOperators.TryGetValue(expr.Operator, out operator_name))
            {
                if (expr.Right == null) return null;
                int inAdded = 0;
                if (!Helper.IsList(expr.Right)) return null;
                List<object> inTempList = Helper.ObjectToList(expr.Right);
                clause += Invariant($" {operator_name} (");
                foreach (object currObj in inTempList)
                {
                    if (currObj == null) continue;
                    if (inAdded > 0) clause += ",";
                    clause += AppendParameterByType(parameters, currObj);
                    inAdded++;
                }
                clause += ")";
            }
            else if (ContainsOperators.TryGetValue(expr.Operator, out operator_pair))
            {
                if (expr.Right == null) return null;
                if (!(expr.Right is string)) return null;
                (operator_name, logic_operator_name) = operator_pair;
                var field = PreparedFieldName(expr.Left.ToString());
                var p1_name = AppendParameterByType(parameters, "%" + expr.Right.ToString());
                var p2_name = AppendParameterByType(parameters, "%" + expr.Right.ToString() + "%");
                var p3_name = AppendParameterByType(parameters, expr.Right.ToString() + "%");
                clause += Invariant($"({field} {operator_name} {p1_name} {logic_operator_name} {field} {operator_name} {p2_name} {logic_operator_name} {field} {operator_name} {p3_name})");
            }
            else if (LikeOperators.TryGetValue(expr.Operator, out operator_triple))
            {
                if (expr.Right == null) return null;
                if (!(expr.Right is string)) return null;
                (operator_name, prefix, suffix) = operator_triple;
                var p_name = AppendParameterByType(parameters, prefix + expr.Right.ToString() + suffix);
                clause += Invariant($"({PreparedFieldName(expr.Left.ToString())} {operator_name} {p_name})");
            }
            else if (IsNullOperators.TryGetValue(expr.Operator, out operator_name))
            {
                clause += " " + operator_name;
            }
            else
            {
                throw new ApplicationException(Invariant($"Error in SqlServerHelper.ExpressionToWhereClause: Unknown operator {expr.Operator}"));
            }

            clause += ")";

            return clause;
        }

        private string BuildOrderByClause(ResultOrder[] resultOrder)
        {
            if (resultOrder == null || resultOrder.Length < 0) return null;

            string ret = "ORDER BY ";

            for (int i = 0; i < resultOrder.Length; i++)
            {
                if (i > 0) ret += ", ";
                ret += SanitizeString(resultOrder[i].ColumnName) + " ";
                if (resultOrder[i].Direction == OrderDirectionEnum.Ascending) ret += "ASC";
                else if (resultOrder[i].Direction == OrderDirectionEnum.Descending) ret += "DESC";
            }

            ret += " ";
            return ret;
        }

        private string RandomCharacters(int len)
        {
            char[] letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".ToCharArray();
            Random rand = new Random();
            string word = "";
            for (int i = 0; i < len; i++)
            {
                int num = rand.Next(0, letters.Length - 1);
                word += letters[num];
            }
            return word;
        }

        private string PreparedTableName(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length > 0)
                {
                    string ret = "";
                    for (int i = 0; i < parts.Length; i++)
                    {
                        if (i > 0) ret += ".";
                        ret += "[" + SanitizeString(parts[i]) + "]";
                    }
                    return ret;
                }
            }

            return
                "[" +
                SanitizeString(s) +
                "]";
        }

        private string PreparedTableNameUnenclosed(string s)
        {
            s = s.Replace("[", "");
            s = s.Replace("]", "");
            if (s.Contains("."))
            {
                string[] parts = s.Split('.');
                if (parts.Length != 2) throw new ArgumentException("Table name must have either zero or one period '.' character");
                return
                    SanitizeString(parts[0]) +
                    "." +
                    SanitizeString(parts[1]);
            }
            else
            {
                return
                    SanitizeString(s);
            }
        }

        private void ValidateInputDictionaries(List<Dictionary<string, object>> dicts)
        {
            Dictionary<string, object> reference = dicts[0];

            if (dicts.Count > 1)
            {
                foreach (Dictionary<string, object> dict in dicts)
                {
                    if (!(reference.Count == dict.Count) || !(reference.Keys.SequenceEqual(dict.Keys)))
                    {
                        throw new ArgumentException("All supplied dictionaries must contain exactly the same keys.");
                    }
                }
            }
        }

        private string BuildKeysFromDictionary(Dictionary<string, object> reference)
        {
            string keys = "";
            int keysAdded = 0;
            foreach (KeyValuePair<string, object> curr in reference)
            {
                if (keysAdded > 0) keys += ",";
                keys += PreparedFieldName(curr.Key);
                keysAdded++;
            }

            return keys;
        }
        #endregion
    }
}
