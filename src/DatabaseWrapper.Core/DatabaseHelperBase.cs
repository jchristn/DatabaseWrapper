using System;
using System.Collections.Generic;
using System.Data;
using static System.FormattableString;
using System.Text;
using ExpressionTree;

namespace DatabaseWrapper.Core
{
    using QueryAndParameters = System.ValueTuple<string, IEnumerable<KeyValuePair<string,object>>>;

    /// <summary>
    /// Base implementation of helper properties and methods.
    /// </summary>
    public abstract class DatabaseHelperBase
    {
        #region Public-Members
        #endregion

        #region Private-Members

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

        static readonly Dictionary<Type, SqlDbType> SqlTypeMap = new Dictionary<Type, SqlDbType>() {
            { typeof(Int32), SqlDbType.Int },
            { typeof(Int64), SqlDbType.BigInt },
            { typeof(double), SqlDbType.Float },
            { typeof(Guid), SqlDbType.UniqueIdentifier },
            { typeof(byte[]), SqlDbType.Image },
            { typeof(DateTimeOffset), SqlDbType.DateTimeOffset },
        };

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods
        /// <summary>
        /// Compose a where-cluase corresponding to the tree expression.
        /// </summary>
        /// <param name="expr">Expression to be converted.</param>
        /// <param name="parameters">Parameters to append SQL query parameters to.</param>
        /// <returns>Where-clause.</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ApplicationException"></exception>
        public string ExpressionToWhereClause(Expr expr, List<KeyValuePair<string, object>> parameters)
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

        /// <summary>
        /// Add parameters to the SQL command.
        /// </summary>
        /// <typeparam name="TC">Subtype of DbCommand</typeparam>
        /// <typeparam name="TP">Subtype of DbPaameter</typeparam>
        /// <param name="cmd">Command to add parameters to.</param>
        /// <param name="createParameter">Parameter constructor.</param>
        /// <param name="parameters">Parameters to be added.</param>
        /// <exception cref="ApplicationException"></exception>
        public static void AddParameters<TC, TP>(TC  cmd, Func<string, SqlDbType,TP> createParameter, IEnumerable<KeyValuePair<string,object>> parameters)
            where TC : System.Data.Common.DbCommand
            where TP : System.Data.Common.DbParameter
        {
            if (parameters==null)
            {
                return;
            }
            foreach (var kv in parameters)
            {
                int param_index = cmd.Parameters.Count;
                var param_name = kv.Key;
                var p = kv.Value;
                if (p == null)
                {
                    cmd.Parameters.Add(createParameter(param_name, SqlDbType.NVarChar));
                    cmd.Parameters[param_index].Value = DBNull.Value;
                    continue;
                }
                var t = p.GetType();
                SqlDbType sql_type;
                if (SqlTypeMap.TryGetValue(t, out sql_type))
                {
                    cmd.Parameters.Add(createParameter(param_name, sql_type));
                    cmd.Parameters[param_index].Value = p;
                    continue;
                }
                if (t == typeof(string))
                {
                    var s_string = p as string;
                    cmd.Parameters.Add(createParameter(param_name, s_string.Length > 4000 ? System.Data.SqlDbType.NText : System.Data.SqlDbType.NVarChar));
                    cmd.Parameters[param_index].Value = s_string;
                    continue;
                }
                if (t == typeof(bool)) {
                    cmd.Parameters.Add(createParameter(param_name, System.Data.SqlDbType.Bit));
                    cmd.Parameters[param_index].Value = (bool)p ? 1 : 0;
                    continue;
                }
                if (t == typeof(DateTime))
                {
                    var dt_object = DBNull.Value as object;
                    var dt_datetime = (DateTime)p;
                    if (dt_datetime != DateTime.MinValue)
                    {
                        var dt = dt_datetime.ToLocalTime();
                        if (dt != DateTime.MinValue)
                        {
                            dt_object = dt;
                        }
                    }
                    cmd.Parameters.Add(createParameter(param_name, System.Data.SqlDbType.DateTime));
                    cmd.Parameters[param_index].Value = dt_object;
                    continue;
                }
                throw new ApplicationException(Invariant($"{nameof(AddParameters)}: Unknown type: {t.Name}"));
            }
        }

        /// <summary>
        /// Build a connection string from DatabaseSettings.
        /// </summary>
        /// <param name="settings">Settings.</param>
        /// <returns>String.</returns>
        public abstract string GenerateConnectionString(DatabaseSettings settings);

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public abstract string RetrieveTableNamesQuery(string database);

        /// <summary>
        /// Query to retrieve the list of columns for a table.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <param name="table">Table name.</param>
        /// <returns></returns>
        public abstract string RetrieveTableColumnsQuery(string database, string table);

        /// <summary>
        /// Method to sanitize a string.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>String.</returns>
        public abstract string SanitizeString(string val);

        /// <summary>
        /// Prepare a field name for use in a SQL query.
        /// </summary>
        /// <param name="fieldName">Name of the field to be prepared.</param>
        /// <returns>Field name for use in a SQL query.</returns>
        public abstract string PreparedFieldName(string fieldName);

        /// <summary>
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public abstract string ColumnToCreateQuery(Column col);

        /// <summary>
        /// Retrieve the primary key column from a list of columns.
        /// </summary>
        /// <param name="columns">List of Column.</param>
        /// <returns>Column.</returns>
        public abstract Column GetPrimaryKeyColumn(List<Column> columns);

        /// <summary>
        /// Retrieve a query used for table creation.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="columns">List of columns.</param>
        /// <returns>String.</returns>
        public abstract string CreateTableQuery(string tableName, List<Column> columns);

        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public abstract string DropTableQuery(string tableName);

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
        public abstract QueryAndParameters SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder);

        /// <summary>
        /// Retrieve a query used for inserting data into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters InsertQuery(string tableName, Dictionary<string, object> keyValuePairs);

        /// <summary>
        /// Retrieve a query for inserting multiple rows into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters InsertMultipleQuery(string tableName, List<Dictionary<string, object>> keyValuePairList);

        /// <summary>
        /// Retrieve a query for updating data in a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters UpdateQuery(string tableName, Dictionary<string, object> keyValuePairs, Expr filter);

        /// <summary>
        /// Retrieve a query for deleting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters DeleteQuery(string tableName, Expr filter);

        /// <summary>
        /// Retrieve a query for truncating a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public abstract string TruncateQuery(string tableName);

        /// <summary>
        /// Retrieve a query for determing whether data matching specified conditions exists.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters ExistsQuery(string tableName, Expr filter);

        /// <summary>
        /// Retrieve a query that returns a count of the number of rows matching the supplied conditions.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="countColumnName">Column name to use to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters CountQuery(string tableName, string countColumnName, Expr filter);

        /// <summary>
        /// Retrieve a query that sums the values found in the specified field.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="fieldName">Column containing values to sum.</param>
        /// <param name="sumColumnName">Column name to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public abstract QueryAndParameters SumQuery(string tableName, string fieldName, string sumColumnName, Expr filter);

        #endregion
    }
}
