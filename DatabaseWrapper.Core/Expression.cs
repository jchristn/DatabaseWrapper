using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Boolean expression.
    /// </summary>
    public class Expression
    {
        #region Constructor

        /// <summary>
        /// A structure in the form of term-operator-term that defines a Boolean evaluation within a WHERE clause.
        /// </summary>
        public Expression()
        {
        }

        /// <summary>
        /// A structure in the form of term-operator-term that defines a Boolean evaluation within a WHERE clause.
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

        /// <summary>
        /// An Expression that allows you to determine if an object is between two values, i.e. GreaterThanOrEqualTo the first value, and LessThanOrEqualTo the second value.
        /// </summary>
        /// <param name="left">The left term of the expression; can either be a string term or a nested Expression.</param> 
        /// <param name="right">List of two values where the first value is the lower value and the second value is the higher value.</param>
        public static Expression Between(object left, List<object> right)
        {
            if (right == null) throw new ArgumentNullException(nameof(right));
            if (right.Count != 2) throw new ArgumentException("Right term must contain exactly two members."); 
            Expression startOfBetween = new Expression(left, Operators.GreaterThanOrEqualTo, right.First());
            Expression endOfBetween = new Expression(left, Operators.LessThanOrEqualTo, right.Last());
            return PrependAndClause(startOfBetween, endOfBetween); 
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

        private string aSanitizeString(DbTypes dbType, string s)
        {
            if (String.IsNullOrEmpty(s)) return String.Empty;
            string ret = "";

            switch (dbType)
            {
                case DbTypes.SqlServer:
                    ret = Sanitizer.SqlServerSanitizer(s);
                    break;

                case DbTypes.Mysql:
                    ret = Sanitizer.MysqlSanitizer(s);
                    break;

                case DbTypes.Postgresql:
                    ret = Sanitizer.PostgresqlSanitizer(s);
                    break;

                case DbTypes.Sqlite:
                    ret = Sanitizer.SqliteSanitizer(s);
                    break;
            }

            return ret;
        }

        private string PreparedFieldname(DbTypes dbType, string s)
        {
            switch (dbType)
            {
                case DbTypes.SqlServer:
                    return "[" + s + "]";

                case DbTypes.Mysql:
                    return "`" + s + "`";

                case DbTypes.Postgresql:
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
                case DbTypes.SqlServer:
                    return "'" + Sanitizer.SqlServerSanitizer(s) + "'";

                case DbTypes.Mysql:
                    return "'" + Sanitizer.MysqlSanitizer(s) + "'";

                case DbTypes.Postgresql:
                    // uses $xx$ escaping
                    return Sanitizer.PostgresqlSanitizer(s);

                case DbTypes.Sqlite:
                    return "'" + Sanitizer.SqliteSanitizer(s) + "'";
            }

            return null;
        }

        private string PreparedUnicodeValue(DbTypes dbType, string s)
        {
            switch (dbType)
            {
                case DbTypes.SqlServer:
                    return "N" + PreparedStringValue(dbType, s);

                case DbTypes.Mysql:
                    return "N" + PreparedStringValue(dbType, s);

                case DbTypes.Postgresql:
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
                case DbTypes.SqlServer:
                    return dt.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");

                case DbTypes.Mysql:
                    return dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                case DbTypes.Postgresql:
                    return dt.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");

                case DbTypes.Sqlite:
                    return dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                default:
                    return null;
            }
        }

        private bool IsList(object obj)
        {
            return obj is IList &&
                obj.GetType().IsGenericType &&
                obj.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }

        #endregion
    }
}
