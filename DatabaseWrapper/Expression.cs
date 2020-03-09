using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    /// <summary>
    /// Boolean expression.
    /// </summary>
    public class Expression
    {
        #region Constructor

        /// <summary>
        /// A structure in the form of term-operator-term that defines a boolean operation within a WHERE clause.
        /// </summary>
        public Expression()
        {
        }

        /// <summary>
        /// A structure in the form of term-operator-term that defines a boolean operation within a WHERE clause.
        /// </summary>
        /// <param name="left">The left term of the expression; can either be a string term or a nested Expression.</param>
        /// <param name="oper">The operator.</param>
        /// <param name="right">The right term of the expression; can either be an object for comparison or a nested Expression.</param>
        public Expression(object left, Operators oper, object right)
        {
            LeftTerm = left;
            Operator = oper;
            RightTerm = right;
        }

        #endregion

        #region Public-Members

        /// <summary>
        /// The left term of the expression; can either be a string term or a nested Expression.
        /// </summary>
        public object LeftTerm;

        /// <summary>
        /// The operator.
        /// </summary>
        public Operators Operator;

        /// <summary>
        /// The right term of the expression; can either be an object for comparison or a nested Expression.
        /// </summary>
        public object RightTerm;

        #endregion

        #region Private-Members

        #endregion

        #region Public-Methods

        /// <summary>
        /// Converts an Expression to a string that is compatible for use in a WHERE clause.
        /// </summary>
        /// <param name="dbType">The database type.</param>
        /// <returns>String containing human-readable version of the Expression.</returns>
        public string ToWhereClause(string dbType)
        {
            if (String.IsNullOrEmpty(dbType)) throw new ArgumentNullException(nameof(dbType));
            switch (dbType.ToLower())
            {
                case "mssql":
                    return ToWhereClause(DbTypes.MsSql);

                case "mysql":
                    return ToWhereClause(DbTypes.MySql);

                case "pgsql":
                    return ToWhereClause(DbTypes.PgSql);

                case "sqlite":
                    return ToWhereClause(DbTypes.Sqlite);

                default:
                    throw new ArgumentOutOfRangeException(nameof(dbType));
            }
        }

        /// <summary>
        /// Converts an Expression to a string that is compatible for use in a WHERE clause.
        /// </summary>
        /// <param name="dbType">The database type.</param>
        /// <returns>String containing human-readable version of the Expression.</returns>
        public string ToWhereClause(DbTypes dbType)
        {
            string clause = "";

            if (LeftTerm == null) return null;

            clause += "(";
            
            if (LeftTerm is Expression)
            {
                clause += ((Expression)LeftTerm).ToWhereClause(dbType) + " ";
            }
            else
            {
                if (!(LeftTerm is string))
                {
                    throw new ArgumentException("Left term must be of type Expression or String");
                }

                if (Operator != Operators.Contains
                    && Operator != Operators.ContainsNot
                    && Operator != Operators.StartsWith
                    && Operator != Operators.EndsWith)
                {
                    //
                    // These operators will add the left term
                    //
                    clause += PreparedFieldname(dbType, LeftTerm.ToString()) + " ";
                }
            }

            switch (Operator)
            {
                #region Process-By-Operators

                case Operators.And:
                    #region And

                    if (RightTerm == null) return null;
                    clause += "AND ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.Or:
                    #region Or

                    if (RightTerm == null) return null;
                    clause += "OR ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.Equals:
                    #region Equals

                    if (RightTerm == null) return null;
                    clause += "= ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.NotEquals:
                    #region NotEquals

                    if (RightTerm == null) return null;
                    clause += "<> ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.In:
                    #region In

                    if (RightTerm == null) return null;
                    int inAdded = 0;
                    if (!Helper.IsList(RightTerm)) return null;
                    List<object> inTempList = Helper.ObjectToList(RightTerm);
                    clause += " IN ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        clause += "(";
                        foreach (object currObj in inTempList)
                        {
                            if (inAdded > 0) clause += ",";
                            if (currObj is DateTime || currObj is DateTime?)
                            {
                                clause += "'" + DbTimestamp(dbType, currObj) + "'";
                            }
                            else if (currObj is int || currObj is long || currObj is decimal)
                            {
                                clause += currObj.ToString();
                            }
                            else
                            {
                                clause += PreparedStringValue(dbType, currObj.ToString());
                            }
                            inAdded++;
                        }
                        clause += ")";
                    }
                    break;

                #endregion

                case Operators.NotIn: 
                    #region NotIn

                    if (RightTerm == null) return null;
                    int notInAdded = 0;
                    if (!Helper.IsList(RightTerm)) return null;
                    List<object> notInTempList = Helper.ObjectToList(RightTerm);
                    clause += " NOT IN ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        clause += "(";
                        foreach (object currObj in notInTempList)
                        {
                            if (notInAdded > 0) clause += ",";
                            if (currObj is DateTime || currObj is DateTime?)
                            {
                                clause += "'" + DbTimestamp(dbType, currObj) + "'";
                            }
                            else if (currObj is int || currObj is long || currObj is decimal)
                            {
                                clause += currObj.ToString();
                            }
                            else
                            {
                                clause += PreparedStringValue(dbType, currObj.ToString());
                            }
                            notInAdded++;
                        }
                        clause += ")";
                    }
                    break;

                #endregion

                case Operators.Contains:
                    #region Contains

                    if (RightTerm == null) return null;
                    if (RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldname(dbType, LeftTerm.ToString()) + " LIKE " + PreparedStringValue(dbType, "%" + RightTerm.ToString()) +
                            "OR " + PreparedFieldname(dbType, LeftTerm.ToString()) + " LIKE " + PreparedStringValue(dbType, "%" + RightTerm.ToString() + "%") +
                            "OR " + PreparedFieldname(dbType, LeftTerm.ToString()) + " LIKE " + PreparedStringValue(dbType, RightTerm.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.ContainsNot:
                    #region ContainsNot

                    if (RightTerm == null) return null;
                    if (RightTerm is string)
                    { 
                        clause +=
                            "(" + 
                            PreparedFieldname(dbType, LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue(dbType, "%" + RightTerm.ToString()) +
                            "OR " + PreparedFieldname(dbType, LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue(dbType, "%" + RightTerm.ToString() + "%") +
                            "OR " + PreparedFieldname(dbType, LeftTerm.ToString()) + " NOT LIKE " + PreparedStringValue(dbType, RightTerm.ToString() + "%") + 
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.StartsWith:
                    #region StartsWith

                    if (RightTerm == null) return null;
                    if (RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldname(dbType, LeftTerm.ToString()) + " LIKE " + PreparedStringValue(dbType, RightTerm.ToString() + "%") +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.EndsWith:
                    #region EndsWith

                    if (RightTerm == null) return null;
                    if (RightTerm is string)
                    {
                        clause +=
                            "(" +
                            PreparedFieldname(dbType, LeftTerm.ToString()) + " LIKE " + "%" + PreparedStringValue(dbType, RightTerm.ToString()) +
                            ")";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                #endregion

                case Operators.GreaterThan:
                    #region GreaterThan

                    if (RightTerm == null) return null;
                    clause += "> ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.GreaterThanOrEqualTo:
                    #region GreaterThanOrEqualTo

                    if (RightTerm == null) return null;
                    clause += ">= ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.LessThan:
                    #region LessThan

                    if (RightTerm == null) return null;
                    clause += "< ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.LessThanOrEqualTo:
                    #region LessThanOrEqualTo

                    if (RightTerm == null) return null;
                    clause += "<= ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause(dbType);
                    }
                    else
                    {
                        if (RightTerm is DateTime || RightTerm is DateTime?)
                        {
                            clause += "'" + DbTimestamp(dbType, RightTerm) + "'";
                        }
                        else if (RightTerm is int || RightTerm is long || RightTerm is decimal)
                        {
                            clause += RightTerm.ToString();
                        }
                        else
                        {
                            clause += PreparedStringValue(dbType, RightTerm.ToString());
                        }
                    }
                    break;

                #endregion

                case Operators.IsNull:
                    #region IsNull

                    clause += " IS NULL";
                    break;

                    #endregion

                case Operators.IsNotNull:
                    #region IsNotNull

                    clause += " IS NOT NULL";
                    break;

                    #endregion

                #endregion
            }

            clause += ")";

            return clause;
        }

        /// <summary>
        /// Display Expression in a human-readable string.
        /// </summary>
        /// <returns>String containing human-readable version of the Expression.</returns>
        public override string ToString()
        {
            string ret = "";
            ret += "(";

            if (LeftTerm is Expression) ret += ((Expression)LeftTerm).ToString();
            else ret += LeftTerm.ToString();

            ret += " " + Operator.ToString() + " ";

            if (RightTerm is Expression) ret += ((Expression)RightTerm).ToString();
            else ret += RightTerm.ToString();

            ret += ")";
            return ret;        
        }

        /// <summary>
        /// Prepends a new Expression using the supplied left term, operator, and right term using an AND clause.
        /// </summary>
        /// <param name="left">The left term of the expression; can either be a string term or a nested Expression.</param>
        /// <param name="oper">The operator.</param>
        /// <param name="right">The right term of the expression; can either be an object for comparison or a nested Expression.</param>
        public void PrependAnd(object left, Operators oper, object right)
        {
            Expression e = new Expression(left, oper, right);
            PrependAnd(e);
        }

        /// <summary>
        /// Prepends the Expression with the supplied Expression using an AND clause.
        /// </summary>
        /// <param name="prepend">The Expression to prepend.</param> 
        public void PrependAnd(Expression prepend)
        {
            if (prepend == null) throw new ArgumentNullException(nameof(prepend));

            Expression orig = new Expression(this.LeftTerm, this.Operator, this.RightTerm);
            Expression e = PrependAndClause(prepend, orig);
            LeftTerm = e.LeftTerm;
            Operator = e.Operator;
            RightTerm = e.RightTerm;

            return;
        }

        /// <summary>
        /// Prepends a new Expression using the supplied left term, operator, and right term using an OR clause.
        /// </summary>
        /// <param name="left">The left term of the expression; can either be a string term or a nested Expression.</param>
        /// <param name="oper">The operator.</param>
        /// <param name="right">The right term of the expression; can either be an object for comparison or a nested Expression.</param>
        public void PrependOr(object left, Operators oper, object right)
        {
            Expression e = new Expression(left, oper, right);
            PrependOr(e);
        }

        /// <summary>
        /// Prepends the Expression with the supplied Expression using an OR clause.
        /// </summary>
        /// <param name="prepend">The Expression to prepend.</param> 
        public void PrependOr(Expression prepend)
        {
            if (prepend == null) throw new ArgumentNullException(nameof(prepend));

            Expression orig = new Expression(this.LeftTerm, this.Operator, this.RightTerm);
            Expression e = PrependOrClause(prepend, orig);
            LeftTerm = e.LeftTerm;
            Operator = e.Operator;
            RightTerm = e.RightTerm;

            return;
        }

        /// <summary>
        /// Prepends the Expression in prepend to the Expression original using an AND clause.
        /// </summary>
        /// <param name="prepend">The Expression to prepend.</param>
        /// <param name="original">The original Expression.</param>
        /// <returns>A new Expression.</returns>
        public static Expression PrependAndClause(Expression prepend, Expression original)
        {
            if (prepend == null) throw new ArgumentNullException(nameof(prepend));
            if (original == null) throw new ArgumentNullException(nameof(original));
            Expression ret = new Expression
            {
                LeftTerm = prepend,
                Operator = Operators.And,
                RightTerm = original
            };
            return ret;
        }

        /// <summary>
        /// Prepends the Expression in prepend to the Expression original using an OR clause.
        /// </summary>
        /// <param name="prepend">The Expression to prepend.</param>
        /// <param name="original">The original Expression.</param>
        /// <returns>A new Expression.</returns>
        public static Expression PrependOrClause(Expression prepend, Expression original)
        {
            if (prepend == null) throw new ArgumentNullException(nameof(prepend));
            if (original == null) throw new ArgumentNullException(nameof(original));
            Expression ret = new Expression
            {
                LeftTerm = prepend,
                Operator = Operators.Or,
                RightTerm = original
            };
            return ret;
        }

        /// <summary>
        /// Convert a List of Expression objects to a nested Expression containing AND between each Expression in the list. 
        /// </summary>
        /// <param name="exprList">List of Expression objects.</param>
        /// <returns>A nested Expression.</returns>
        public static Expression ListToNestedAndExpression(List<Expression> exprList)
        {
            if (exprList == null) throw new ArgumentNullException(nameof(exprList));
            if (exprList.Count < 1) return null;
            
            int evaluated = 0;
            Expression ret = null;
            Expression left = null;
            List<Expression> remainder = new List<Expression>();

            if (exprList.Count == 1)
            {
                foreach (Expression curr in exprList)
                {
                    ret = curr;
                    break;
                }

                return ret;
            }
            else
            {
                foreach (Expression curr in exprList)
                {
                    if (evaluated == 0)
                    {
                        left = new Expression();
                        left.LeftTerm = curr.LeftTerm;
                        left.Operator = curr.Operator;
                        left.RightTerm = curr.RightTerm;
                        evaluated++;
                    }
                    else
                    {
                        remainder.Add(curr);
                        evaluated++;
                    }
                }

                ret = new Expression();
                ret.LeftTerm = left;
                ret.Operator = Operators.And;
                Expression right = ListToNestedAndExpression(remainder);
                ret.RightTerm = right;

                return ret;
            }
        }

        /// <summary>
        /// Convert a List of Expression objects to a nested Expression containing OR between each Expression in the list. 
        /// </summary>
        /// <param name="exprList">List of Expression objects.</param>
        /// <returns>A nested Expression.</returns>
        public static Expression ListToNestedOrExpression(List<Expression> exprList)
        {
            if (exprList == null) throw new ArgumentNullException(nameof(exprList));
            if (exprList.Count < 1) return null;

            int evaluated = 0;
            Expression ret = null;
            Expression left = null;
            List<Expression> remainder = new List<Expression>();

            if (exprList.Count == 1)
            {
                foreach (Expression curr in exprList)
                {
                    ret = curr;
                    break;
                }

                return ret;
            }
            else
            {
                foreach (Expression curr in exprList)
                {
                    if (evaluated == 0)
                    {
                        left = new Expression();
                        left.LeftTerm = curr.LeftTerm;
                        left.Operator = curr.Operator;
                        left.RightTerm = curr.RightTerm;
                        evaluated++;
                    }
                    else
                    {
                        remainder.Add(curr);
                        evaluated++;
                    }
                }

                ret = new Expression();
                ret.LeftTerm = left;
                ret.Operator = Operators.Or;
                Expression right = ListToNestedOrExpression(remainder);
                ret.RightTerm = right;

                return ret;
            }
        }

        #endregion

        #region Private-Methods

        private string SanitizeString(DbTypes dbType, string s)
        {
            if (String.IsNullOrEmpty(s)) return String.Empty;
            string ret = "";

            switch (dbType)
            {
                case DbTypes.MsSql:
                    ret = MssqlHelper.SanitizeString(s);
                    break;

                case DbTypes.MySql:
                    ret = MysqlHelper.SanitizeString(s);
                    break;

                case DbTypes.PgSql:
                    ret = PgsqlHelper.SanitizeString(s);
                    break;

                case DbTypes.Sqlite:
                    ret = SqliteHelper.SanitizeString(s);
                    break;
            }

            return ret;
        }

        private string PreparedFieldname(DbTypes dbType, string s)
        {
            switch (dbType)
            {
                case DbTypes.MsSql:
                    return "[" + s + "]";

                case DbTypes.MySql:
                    return "`" + s + "`";

                case DbTypes.PgSql:
                    return "\"" + s + "\"";

                case DbTypes.Sqlite:
                    return "`" + s + "`";
            }

            return null;
        }

        private string PreparedStringValue(DbTypes dbType, string s)
        {
            switch (dbType)
            {
                case DbTypes.MsSql:
                    return "'" + MssqlHelper.SanitizeString(s) + "'";

                case DbTypes.MySql:
                    return "'" + MysqlHelper.SanitizeString(s) + "'";

                case DbTypes.PgSql:
                    // uses $xx$ escaping
                    return PgsqlHelper.SanitizeString(s);

                case DbTypes.Sqlite:
                    return "'" + SqliteHelper.SanitizeString(s) + "'"; 
            }

            return null;
        }

        private string PreparedUnicodeValue(DbTypes dbType, string s)
        {
            switch (dbType)
            {
                case DbTypes.MsSql:
                    return "N" + PreparedStringValue(dbType, s);

                case DbTypes.MySql:
                    return "N" + PreparedStringValue(dbType, s);

                case DbTypes.PgSql:
                    return "U&" + PreparedStringValue(dbType, s);

                case DbTypes.Sqlite:
                    return "N" + PreparedStringValue(dbType, s);
            }

            return null;
        }

        private string DbTimestamp(DbTypes dbType, object ts)
        {
            DateTime dt = DateTime.Now;
            if (ts == null) return null;
            if (ts is DateTime?) dt = Convert.ToDateTime(ts);
            else if (ts is DateTime) dt = (DateTime)ts;

            switch (dbType)
            {
                case DbTypes.MsSql:
                    return dt.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");

                case DbTypes.MySql:
                    return dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                case DbTypes.PgSql:
                    return dt.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");

                case DbTypes.Sqlite:
                    return dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                default:
                    return null;
            }
        }
        
        #endregion
    }
}
