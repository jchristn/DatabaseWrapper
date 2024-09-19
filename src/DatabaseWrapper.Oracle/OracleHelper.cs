using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper.Core;
using ExpressionTree;

namespace DatabaseWrapper.Oracle
{
    public class OracleHelper : DatabaseHelperBase
    {
        private enum CharClass : byte
        {
            None,
            Quote,
            Backslash
        }

        private static string stringOfBackslashChars = "\\¥Š₩∖﹨＼";

        private static string stringOfQuoteChars = "\"'`\u00b4ʹʺʻʼˈˊˋ\u02d9\u0300\u0301‘’‚′‵❛❜＇";

        private static CharClass[] charClassArray = MakeCharClassArray();

        #region Public-Members

        /// <summary>
        /// Timestamp format for use in DateTime.ToString([format]).
        /// </summary>
        public new string TimestampFormat { get; set; } = "yyyy-MM-dd HH:mm:ss.ffffff";

        /// <summary>
        /// Timestamp offset format for use in DateTimeOffset.ToString([format]).
        /// </summary>
        public new string TimestampOffsetFormat { get; set; } = "yyyy-MM-dd HH:mm:ss.ffffffzzz";

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

            //
            // http://www.connectionstrings.com/mysql/
            //
            // Data Source=TORCL;User Id=urUsername;Password=urPassword;
            ret += "Data Source=" + settings.Hostname + "; ";
            //if (settings.Port > 0) ret += "Port=" + settings.Port + "; ";
            //ret += "Database=" + settings.DatabaseName + "; ";
            if (!String.IsNullOrEmpty(settings.Username)) ret += "User Id=" + settings.Username + "; ";
            if (!String.IsNullOrEmpty(settings.Password)) ret += "Password=" + settings.Password + "; ";

            return ret;
        }

        /// <summary>
        /// Query to retrieve the names of tables from a database.
        /// </summary>
        /// <param name="database">Database name.</param>
        /// <returns>String.</returns>
        public override string RetrieveTableNamesQuery(string database)
        {
            return "SELECT * FROM user_tables";
        }
        public string RetrieveSequencesQuery(string database)
        {
            return "SELECT * FROM user_sequences";
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
            "select * from user_tab_columns where table_name = '" + table.ToUpper() + "' order by column_id";
        }
        internal string IsPrimaryKeyQuery(string database, string table, string column) 
        {
            string query = $"SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner\r\nFROM all_constraints cons, all_cons_columns cols\r\nWHERE cols.table_name = '{table.ToUpper()}'\r\nAND cons.constraint_type = 'P'\r\nAND cons.constraint_name = cols.constraint_name\r\nAND cons.owner = cols.owner\r\n AND cols.column_name='{column.ToUpper()}' ORDER BY cols.table_name, cols.position";
            return query;
        }
        /// <summary>
        /// Method to sanitize a string.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>String.</returns>
        public override string SanitizeString(string val)
        {
            string ret = "";
            ret = OracleHelper.EscapeString(val);
            return ret;
        }
        private static CharClass[] MakeCharClassArray()
        {
            CharClass[] array = new CharClass[65536];
            string text = stringOfBackslashChars;
            foreach (char c in text)
            {
                array[(uint)c] = CharClass.Backslash;
            }

            text = stringOfQuoteChars;
            foreach (char c2 in text)
            {
                array[(uint)c2] = CharClass.Quote;
            }

            return array;
        }
        private static bool NeedsQuoting(string s)
        {
            return s.Any((char c) => charClassArray[(uint)c] != CharClass.None);
        }
        public static string EscapeString(string value)
        {
            if (!NeedsQuoting(value))
            {
                return value;
            }

            StringBuilder stringBuilder = new StringBuilder();
            foreach (char c in value)
            {
                if (charClassArray[(uint)c] != 0)
                {
                    stringBuilder.Append("\\");
                }

                stringBuilder.Append(c);
            }

            return stringBuilder.ToString();
        }
        /// <summary>
        /// Method to convert a Column object to the values used in a table create statement.
        /// </summary>
        /// <param name="col">Column.</param>
        /// <returns>String.</returns>
        public override string ColumnToCreateQuery(Column col)
        {
            string ret =
                "" + SanitizeString(col.Name) + " ";

            switch (col.Type)
            {
                
                case DataTypeEnum.Varchar:
                case DataTypeEnum.Nvarchar:
                    ret += "varchar2(" + col.MaxLength + " char) ";
                    break;
                case DataTypeEnum.Guid:
                    ret += "varchar2(36) ";
                    break;
                case DataTypeEnum.TinyInt:
                case DataTypeEnum.Boolean:
                case DataTypeEnum.Int:
                case DataTypeEnum.Long:
                    if(col.MaxLength < 38)
                        ret += "number(" + col.MaxLength + ") ";
                    else
                        ret += "number(37) ";
                    break;
                case DataTypeEnum.Decimal:
                    ret += "decimal(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.Double:
                    ret += "float(" + col.MaxLength + "," + col.Precision + ") ";
                    break;
                case DataTypeEnum.DateTime:
                case DataTypeEnum.DateTimeOffset:
                    if (col.Precision != null)
                    {
                        ret += "timestamp(" + col.Precision + ") ";
                    }
                    else
                    {
                        ret += "timestamp ";
                    }
                    break;
                case DataTypeEnum.Blob:
                    ret += "blob  ";
                    break;
                
                
                default:
                    throw new ArgumentException("Unknown DataType: " + col.Type.ToString());
            }

            if (col.Nullable) ret += "NULL ";
            else ret += "NOT NULL ";

            //if (col.PrimaryKey) ret += "AUTO_INCREMENT ";

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
                "CREATE TABLE " + SanitizeString(tableName) + " " +
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
                    "," +
                    "PRIMARY KEY (" + SanitizeString(primaryKey.Name) + ")";
            }

            query +=
                ") ";
            if (primaryKey != null) {
                //query += $"CREATE SEQUENCE {tableName}_seq START WITH 1 INCREMENT BY 1;" +
                //           $"CREATE OR REPLACE TRIGGER {tableName}_seq_tr " +
                //           "BEFORE INSERT ON person FOR EACH ROW " +
                //           "WHEN (NEW.id IS NULL) " +
                //           "BEGIN " +
                //           "SELECT person_seq.NEXTVAL INTO :NEW.id FROM DUAL; " +
                //           "END; ";


            }
            return query;
        }
        public string CreateSequnce(string tableName) 
        {
            string query = $"CREATE SEQUENCE {tableName}_seq START WITH 1 INCREMENT BY 1";
            return query;
        }
        public string CreateTrigger(string tableName)
        {
            string query = $"CREATE OR REPLACE TRIGGER {tableName}_seq_tr " +
                           "BEFORE INSERT ON person FOR EACH ROW " +
                           "WHEN (NEW.id IS NULL) " +
                           "BEGIN " +
                           "SELECT person_seq.NEXTVAL INTO :NEW.id FROM DUAL; " +
                           "END; ";
            return query;
        }
        /// <summary>
        /// Retrieve a query used for dropping a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string DropTableQuery(string tableName)
        {
            //string query = "DECLARE cnt NUMBER;\r\n  BEGIN\r\n    SELECT COUNT(*) INTO cnt FROM user_tables WHERE table_name = '" + SanitizeString(tableName) + "';\r\n    IF cnt <> 0 THEN\r\n      EXECUTE IMMEDIATE 'DROP TABLE " + SanitizeString(tableName) + "';\r\n    END IF;\r\n  END;";
            string query = "DROP TABLE " + SanitizeString(tableName.ToUpper()) + "";
            return query;
        }
        internal string DropSequenceQuery(string tableName)
        {
            //string query = "DECLARE cnt NUMBER;\r\n  BEGIN\r\n    SELECT COUNT(*) INTO cnt FROM user_tables WHERE table_name = '" + SanitizeString(tableName) + "';\r\n    IF cnt <> 0 THEN\r\n      EXECUTE IMMEDIATE 'DROP TABLE " + SanitizeString(tableName) + "';\r\n    END IF;\r\n  END;";
            string query = "DROP SEQUENCE " + SanitizeString(tableName.ToUpper()+"_SEQ") + "";
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
        public override string SelectQuery(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expr filter, ResultOrder[] resultOrder)
        {
            string query = "";
            string whereClause = "";

            //
            // SELECT
            //
            query += "SELECT ";

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
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }
            else 
            {
                query += "WHERE 1=1 ";
            }

            //
            // limit
            //
            if (maxResults > 0)
            {
                if (indexStart != null && indexStart >= 0)
                {
                    query += "AND ROWNUM >= " + indexStart + " AND ROWNUM <=" + maxResults + " ";
                }
                else
                {
                    query += "AND ROWNUM <= " + maxResults + " ";
                }
            }

            // 
            // order clause
            // 
            query += BuildOrderByClause(resultOrder);


            return query;
        }

        /// <summary>
        /// Retrieve a query used for inserting data into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override string InsertQuery(string tableName, Dictionary<string, object> keyValuePairs)
        {
            string ret =
                "INSERT INTO " + SanitizeString(tableName) + " " +
                "(";

            string keys = "";
            string vals = "";
            BuildKeysValuesFromDictionary(keyValuePairs, out keys, out vals);

            ret += keys + ") " +
                //"OUTPUT INSERTED.* " +
                "VALUES " +
                "(" + vals + ") ";

            return ret;
        }
        internal string InsertQueryParams(string tableName, Dictionary<string, object> keyValuePairs)
        {
            string ret =
                "INSERT INTO " + SanitizeString(tableName) + " " +
                "(";

            string keys = "";
            string vals = "";
            BuildKeysValuesFromDictionaryParams(keyValuePairs, out keys, out vals);

            ret += keys + ") " +
                //"OUTPUT INSERTED.* " +
                "VALUES " +
                "(" + vals + ") ";

            return ret;
        }
        /// <summary>
        /// Retrieve a query for inserting multiple rows into a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairList">List of dictionaries containing key-value pairs for the rows you wish to INSERT.</param>
        /// <returns>String.</returns>
        public override string InsertMultipleQuery(string tableName, List<Dictionary<string, object>> keyValuePairList)
        {
            ValidateInputDictionaries(keyValuePairList);
            string keys = BuildKeysFromDictionary(keyValuePairList[0]);
            List<string> values = BuildValuesFromDictionaries(keyValuePairList);

            string ret =
                "START TRANSACTION;" +
                "  INSERT INTO " + SanitizeString(tableName) + " " +
                "  (" + keys + ") " +
                "  VALUES ";

            int added = 0;
            foreach (string value in values)
            {
                if (added > 0) ret += ",";
                ret += "  (" + value + ")";
                added++;
            }

            ret +=
                ";  COMMIT; ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for updating data in a table.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        /// <returns>String.</returns>
        public override string UpdateQuery(string tableName, Dictionary<string, object> keyValuePairs, Expr filter)
        {
            string keyValueClause = BuildKeyValueClauseFromDictionary(keyValuePairs);

            string ret =
                "UPDATE " + SanitizeString(tableName) + " SET " +
                keyValueClause + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for deleting data from a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string DeleteQuery(string tableName, Expr filter)
        {
            string ret =
                "DELETE FROM " + SanitizeString(tableName) + " ";

            if (filter != null) ret += "WHERE " + ExpressionToWhereClause(filter) + " ";

            return ret;
        }

        /// <summary>
        /// Retrieve a query for truncating a table.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <returns>String.</returns>
        public override string TruncateQuery(string tableName)
        {
            return "TRUNCATE TABLE `" + SanitizeString(tableName) + "`";
        }

        /// <summary>
        /// Retrieve a query for determing whether data matching specified conditions exists.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string ExistsQuery(string tableName, Expr filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT * " +
                "FROM " + SanitizeString(tableName) + " ";

            //
            // expressions
            //
            if (filter != null) whereClause = ExpressionToWhereClause(filter);
            if (!String.IsNullOrEmpty(whereClause))
            {
                query += "WHERE " + whereClause + " ";
            }
            else 
            {
                query += "WHERE 1=1 ";
            }

            query += "AND ROWNUM <= 1";
            return query;
        }

        /// <summary>
        /// Retrieve a query that returns a count of the number of rows matching the supplied conditions.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="countColumnName">Column name to use to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string CountQuery(string tableName, string countColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT COUNT(*) AS " + countColumnName + " " +
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

        /// <summary>
        /// Retrieve a query that sums the values found in the specified field.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="fieldName">Column containing values to sum.</param>
        /// <param name="sumColumnName">Column name to temporarily store the result.</param>
        /// <param name="filter">Expression filter.</param>
        /// <returns>String.</returns>
        public override string SumQuery(string tableName, string fieldName, string sumColumnName, Expr filter)
        {
            string query = "";
            string whereClause = "";

            //
            // select
            //
            query =
                "SELECT SUM(" + SanitizeString(fieldName) + ") AS " + sumColumnName + " " +
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

        /// <summary>
        /// Retrieve a timestamp in the database format.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>String.</returns>
        public override string GenerateTimestamp(DateTime ts)
        {
            return ts.ToString(TimestampFormat);
        }

        /// <summary>
        /// Retrieve a timestamp offset in the database format.
        /// </summary>
        /// <param name="ts">DateTimeOffset.</param>
        /// <returns>String.</returns>
        public override string GenerateTimestampOffset(DateTimeOffset ts)
        {
            return ts.DateTime.ToString(TimestampFormat);
        }

        #endregion

        #region Private-Methods

        private string PreparedFieldName(string fieldName)
        {
            return "" + fieldName + "";
        }

        private string PreparedStringValue(string str)
        {
            return "'" + SanitizeString(str) + "'";
        }

        private string ExpressionToWhereClause(Expr expr)
        {
            if (expr == null) return null;

            string clause = "";

            if (expr.Left == null) return null;

            clause += "(";

            if (expr.Left is Expr)
            {
                clause += ExpressionToWhereClause((Expr)expr.Left) + " ";
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

            switch (expr.Operator)
            {
                #region Process-By-Operators

                case OperatorEnum.And:
                    #region And

                    if (expr.Right == null) return null;
                    clause += "AND ";

                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.Or:
                    #region Or

                    if (expr.Right == null) return null;
                    clause += "OR ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.Equals:
                    #region Equals

                    if (expr.Right == null) return null;
                    clause += "= ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.NotEquals:
                    #region NotEquals

                    if (expr.Right == null) return null;
                    clause += "<> ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.In:
                    #region In

                    if (expr.Right == null) return null;
                    int inAdded = 0;
                    if (!Helper.IsList(expr.Right)) return null;
                    List<object> inTempList = Helper.ObjectToList(expr.Right);
                    clause += " IN (";
                    foreach (object currObj in inTempList)
                    {
                        if (currObj == null) continue;
                        if (inAdded > 0) clause += ",";
                        if (currObj is DateTime || currObj is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(currObj)) + "'";
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
                    break;

                #endregion

                case OperatorEnum.NotIn:
                    #region NotIn

                    if (expr.Right == null) return null;
                    int notInAdded = 0;
                    if (!Helper.IsList(expr.Right)) return null;
                    List<object> notInTempList = Helper.ObjectToList(expr.Right);
                    clause += " NOT IN (";
                    foreach (object currObj in notInTempList)
                    {
                        if (currObj == null) continue;
                        if (notInAdded > 0) clause += ",";
                        if (currObj is DateTime || currObj is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(currObj)) + "'";
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
                    break;

                #endregion

                case OperatorEnum.Contains:
                    #region Contains

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue("%" + expr.Right.ToString() + "%") +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.ContainsNot:
                    #region ContainsNot

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.Right.ToString() + "%") +
                            "OR " + PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.StartsWith:
                    #region StartsWith

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.StartsWithNot:
                    #region StartsWithNot

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue(expr.Right.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.EndsWith:
                    #region EndsWith

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.EndsWithNot:
                    #region EndsWithNot

                    if (expr.Right == null) return null;
                    if (expr.Right is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldName(expr.Left.ToString()) + " NOT LIKE " + PreparedStringValue("%" + expr.Right.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case OperatorEnum.GreaterThan:
                    #region GreaterThan

                    if (expr.Right == null) return null;
                    clause += "> ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.GreaterThanOrEqualTo:
                    #region GreaterThanOrEqualTo

                    if (expr.Right == null) return null;
                    clause += ">= ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.LessThan:
                    #region LessThan

                    if (expr.Right == null) return null;
                    clause += "< ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.LessThanOrEqualTo:
                    #region LessThanOrEqualTo

                    if (expr.Right == null) return null;
                    clause += "<= ";
                    if (expr.Right is Expr)
                    {
                        clause += ExpressionToWhereClause((Expr)expr.Right);
                    }
                    else
                    {
                        if (expr.Right is DateTime || expr.Right is DateTime?)
                        {
                            clause += "'" + GenerateTimestamp(Convert.ToDateTime(expr.Right)) + "'";
                        }
                        else if (expr.Right is int || expr.Right is long || expr.Right is decimal)
                        {
                            clause += expr.Right.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(expr.Right.ToString());
                        }
                    }
                    break;

                #endregion

                case OperatorEnum.IsNull:
                    #region IsNull

                    clause += " IS NULL";
                    break;

                #endregion

                case OperatorEnum.IsNotNull:
                    #region IsNotNull

                    clause += " IS NOT NULL";
                    break;

                    #endregion

                    #endregion
            }

            clause += ")";

            return clause;
        }

        private string PreparedUnicodeValue(string s)
        {
            return "N" + PreparedStringValue(s);
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

        private void BuildKeysValuesFromDictionary(Dictionary<string, object> keyValuePairs, out string keys, out string vals)
        {
            keys = "";
            vals = "";
            int added = 0;

            foreach (KeyValuePair<string, object> currKvp in keyValuePairs)
            {
                if (String.IsNullOrEmpty(currKvp.Key)) continue;

                if (added > 0)
                {
                    keys += ",";
                    vals += ",";
                }

                keys += PreparedFieldName(currKvp.Key);

                if (currKvp.Value != null)
                {
                    if (currKvp.Value is DateTime
                        || currKvp.Value is DateTime?)
                    {
                        vals += "'" + ((DateTime)currKvp.Value).ToString(TimestampFormat) + "'";
                    }
                    else if (currKvp.Value is DateTimeOffset
                        || currKvp.Value is DateTimeOffset?)
                    {
                        vals += "'" + ((DateTimeOffset)currKvp.Value).ToString(TimestampOffsetFormat) + "'";
                    }
                    else if (currKvp.Value is int
                        || currKvp.Value is long
                        || currKvp.Value is decimal)
                    {
                        vals += currKvp.Value.ToString();
                    }
                    else if (currKvp.Value is bool)
                    {
                        vals += ((bool)currKvp.Value ? "1" : "0");
                    }
                    else if (currKvp.Value is byte[])
                    {
                        vals += "x'" + BitConverter.ToString((byte[])currKvp.Value).Replace("-", "") + "'";
                    }
                    else
                    {
                        if (IsExtendedCharacters(currKvp.Value.ToString()))
                        {
                            vals += PreparedUnicodeValue(currKvp.Value.ToString());
                        }
                        else
                        {
                            vals += PreparedStringValue(currKvp.Value.ToString());
                        }
                    }
                }
                else
                {
                    vals += "null";
                }

                added++;
            }
        }
        private void BuildKeysValuesFromDictionaryParams(Dictionary<string, object> keyValuePairs, out string keys, out string vals)
        {
            keys = "";
            vals = "";
            int added = 0;

            foreach (KeyValuePair<string, object> currKvp in keyValuePairs)
            {
                if (String.IsNullOrEmpty(currKvp.Key)) continue;

                if (added > 0)
                {
                    keys += ",";
                    vals += ",";
                }

                keys += PreparedFieldName(currKvp.Key);

                vals += ":"+PreparedFieldName(currKvp.Key);

                added++;
            }
        }
        private bool IsExtendedCharacters(string data)
        {
            if (String.IsNullOrEmpty(data)) return false;
            foreach (char c in data)
            {
                if ((int)c > 255) return true;
            }
            return false;
        }

        private void ValidateInputDictionaries(List<Dictionary<string, object>> keyValuePairList)
        {
            Dictionary<string, object> reference = keyValuePairList[0];

            if (keyValuePairList.Count > 1)
            {
                foreach (Dictionary<string, object> dict in keyValuePairList)
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

        private List<string> BuildValuesFromDictionaries(List<Dictionary<string, object>> dicts)
        {
            List<string> values = new List<string>();

            foreach (Dictionary<string, object> currDict in dicts)
            {
                string vals = "";
                int valsAdded = 0;

                foreach (KeyValuePair<string, object> currKvp in currDict)
                {
                    if (valsAdded > 0) vals += ",";

                    if (currKvp.Value != null)
                    {
                        if (currKvp.Value is DateTime
                            || currKvp.Value is DateTime?)
                        {
                            vals += "'" + ((DateTime)currKvp.Value).ToString(TimestampFormat) + "'";
                        }
                        else if (currKvp.Value is DateTimeOffset
                            || currKvp.Value is DateTimeOffset?)
                        {
                            vals += "'" + ((DateTimeOffset)currKvp.Value).ToString(TimestampOffsetFormat) + "'";
                        }
                        else if (currKvp.Value is int
                            || currKvp.Value is long
                            || currKvp.Value is decimal)
                        {
                            vals += currKvp.Value.ToString();
                        }
                        else if (currKvp.Value is bool)
                        {
                            vals += ((bool)currKvp.Value ? "1" : "0");
                        }
                        else if (currKvp.Value is byte[])
                        {
                            vals += "x'" + BitConverter.ToString((byte[])currKvp.Value).Replace("-", "") + "'";
                        }
                        else
                        {
                            if (IsExtendedCharacters(currKvp.Value.ToString()))
                            {
                                vals += PreparedUnicodeValue(currKvp.Value.ToString());
                            }
                            else
                            {
                                vals += PreparedStringValue(currKvp.Value.ToString());
                            }
                        }

                    }
                    else
                    {
                        vals += "null";
                    }

                    valsAdded++;
                }

                values.Add(vals);
            }

            return values;
        }

        private string BuildKeyValueClauseFromDictionary(Dictionary<string, object> keyValuePairs)
        {
            string keyValueClause = "";
            int added = 0;

            foreach (KeyValuePair<string, object> currKvp in keyValuePairs)
            {
                if (String.IsNullOrEmpty(currKvp.Key)) continue;

                if (added > 0) keyValueClause += ",";

                if (currKvp.Value != null)
                {
                    if (currKvp.Value is DateTime
                        || currKvp.Value is DateTime?)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "='" + ((DateTime)currKvp.Value).ToString(TimestampFormat) + "'";
                    }
                    else if (currKvp.Value is DateTimeOffset
                        || currKvp.Value is DateTimeOffset?)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "='" + ((DateTimeOffset)currKvp.Value).ToString(TimestampOffsetFormat) + "'";
                    }
                    else if (currKvp.Value is int
                        || currKvp.Value is long
                        || currKvp.Value is decimal)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + currKvp.Value.ToString();
                    }
                    else if (currKvp.Value is bool)
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + ((bool)currKvp.Value ? "1" : "0");
                    }
                    else if (currKvp.Value is byte[])
                    {
                        keyValueClause += PreparedFieldName(currKvp.Key) + "=" + "x'" + BitConverter.ToString((byte[])currKvp.Value).Replace("-", "") + "'";
                    }
                    else
                    {
                        if (IsExtendedCharacters(currKvp.Value.ToString()))
                        {
                            keyValueClause += PreparedFieldName(currKvp.Key) + "=" + PreparedUnicodeValue(currKvp.Value.ToString());
                        }
                        else
                        {
                            keyValueClause += PreparedFieldName(currKvp.Key) + "=" + PreparedStringValue(currKvp.Value.ToString());
                        }
                    }
                }
                else
                {
                    keyValueClause += PreparedFieldName(currKvp.Key) + "= null";
                }

                added++;
            }

            return keyValueClause;
        }

        #endregion
    }
}
