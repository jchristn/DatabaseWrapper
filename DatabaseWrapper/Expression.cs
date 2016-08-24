using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    public class Expression
    {
        #region Constructor

        /// <summary>
        /// A structure in the form of term-operator-term that defines a boolean operation within a WHERE clause.
        /// </summary>
        public Expression()
        {
        }

        #endregion

        #region Public-Members

        /// <summary>
        /// The left term of the expression; can either be a string term or a nested Expression.
        /// </summary>
        public object LeftTerm;

        /// <summary>
        /// The boolean operator.
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
        /// <returns></returns>
        public string ToWhereClause()
        {
            string clause = "";
            if (LeftTerm == null) return null;
            clause += "(";

            if (LeftTerm is Expression)
            {
                clause += ((Expression)LeftTerm).ToWhereClause() + " ";
            }
            else
            {
                if (!(LeftTerm is string))
                {
                    return null;
                }

                if (Operator != Operators.Contains
                    && Operator != Operators.ContainsNot)
                {
                    //
                    // These operators will add the left term
                    //
                    clause += SanitizeString(LeftTerm.ToString()) + " ";
                }
            }

            switch (Operator)
            {
                #region Process-By-Operators

                case Operators.And:
                    if (RightTerm == null) return null;
                    clause += "AND ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        return null;
                    }
                    break;

                case Operators.Or:
                    if (RightTerm == null) return null;
                    clause += "OR ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        return null;
                    }
                    break;
                    
                case Operators.Equals:
                    if (RightTerm == null) return null;
                    clause += "= ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "'" + SanitizeString(RightTerm.ToString()) + "'";
                    }
                    break;

                case Operators.NotEquals:
                    if (RightTerm == null) return null;
                    clause += "<> ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "'" + SanitizeString(RightTerm.ToString()) + "'";
                    }
                    break;

                case Operators.In:
                    if (RightTerm == null) return null;
                    int inAdded = 0;
                    if (!Helper.IsList(RightTerm)) return null;
                    List<object> tempList = Helper.ObjectToList(RightTerm);
                    clause += " IN ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "(";
                        foreach (object currObj in tempList)
                        {
                            if (inAdded == 0)
                            {
                                clause += "'" + SanitizeString(currObj.ToString()) + "'";
                                inAdded++;
                            }
                            else
                            {
                                clause += ",'" + SanitizeString(currObj.ToString()) + "'";
                                inAdded++;
                            }
                        }
                        clause += ")";
                    }
                    break;

                case Operators.Contains:
                    if (RightTerm == null) return null;
                    if (RightTerm is string)
                    {
                        clause +=
                            "(" + SanitizeString(LeftTerm.ToString()) + " LIKE '" + SanitizeString(RightTerm.ToString()) + "%'" +
                            "OR " + SanitizeString(LeftTerm.ToString()) + " LIKE '%" + SanitizeString(RightTerm.ToString()) + "%'" +
                            "OR " + SanitizeString(LeftTerm.ToString()) + " LIKE '%" + SanitizeString(RightTerm.ToString()) + "')";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                case Operators.ContainsNot:
                    if (RightTerm == null) return null;
                    if (RightTerm is string)
                    {
                        clause +=
                            "(" + SanitizeString(LeftTerm.ToString()) + " NOT LIKE '" + SanitizeString(RightTerm.ToString()) + "%'" +
                            "OR " + SanitizeString(LeftTerm.ToString()) + " NOT LIKE '%" + SanitizeString(RightTerm.ToString()) + "%'" +
                            "OR " + SanitizeString(LeftTerm.ToString()) + " NOT LIKE '%" + SanitizeString(RightTerm.ToString()) + "')";
                    }
                    else
                    {
                        return null;
                    }
                    break;

                case Operators.GreaterThan:
                    if (RightTerm == null) return null;
                    clause += "> ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "'" + SanitizeString(RightTerm.ToString()) + "'";
                    }
                    break;

                case Operators.GreaterThanOrEqualTo:
                    if (RightTerm == null) return null;
                    clause += ">= ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "'" + SanitizeString(RightTerm.ToString()) + "'";
                    }
                    break;

                case Operators.LessThan:
                    if (RightTerm == null) return null;
                    clause += "< ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "'" + SanitizeString(RightTerm.ToString()) + "'";
                    }
                    break;

                case Operators.LessThanOrEqualTo:
                    if (RightTerm == null) return null;
                    clause += "<= ";
                    if (RightTerm is Expression)
                    {
                        clause += ((Expression)RightTerm).ToWhereClause();
                    }
                    else
                    {
                        clause += "'" + SanitizeString(RightTerm.ToString()) + "'";
                    }
                    break;

                case Operators.IsNull:
                    clause += " IS NULL";
                    break;

                case Operators.IsNotNull:
                    clause += " IS NOT NULL";
                    break;
                    
                #endregion
            }

            clause += ")";

            return clause;
        }
        
        #endregion

        #region Private-Methods

        private string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return String.Empty;
            string ret = "";
            int doubleDash = 0;
            int openComment = 0;
            int closeComment = 0;
            
            //
            // null, below ASCII range, above ASCII range
            //
            for (int i = 0; i < s.Length; i++)
            {
                if (
                    ((int)(s[i]) == 0) || // null
                    ((int)(s[i]) < 32)
                    )
                {
                    continue;
                }
                else
                {
                    ret += s[i];
                }
            }

            //
            // double dash
            //
            doubleDash = 0;
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
            openComment = 0;
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
            closeComment = 0;
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

        #endregion
    }
}
