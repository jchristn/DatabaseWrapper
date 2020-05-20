using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Sanitization methods.
    /// </summary>
    public static class Sanitizer
    {
        /// <summary>
        /// SQL Server sanitizer.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>Sanitized string.</returns>
        public static string SqlServerSanitizer(string val)
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
        /// MySQL sanitizer.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>Sanitized string.</returns>
        public static string MysqlSanitizer(string val)
        {
            return SqlServerSanitizer(val); 
        }

        /// <summary>
        /// PostgreSQL sanitizer.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>Sanitized string.</returns>
        public static string PostgresqlSanitizer(string val)
        {
            string tag = "$" + PostgresqlEscapeString(val, 2) + "$";
            return tag + val + tag;
        }

        private static string PostgresqlEscapeString(string val, int numChar)
        {
            string ret = "";
            Random random = new Random();
            if (numChar < 1) return ret;

            while (true)
            {
                ret = "";
                random = new Random();

                int valid = 0;
                int num = 0;

                for (int i = 0; i < numChar; i++)
                {
                    num = 0;
                    valid = 0;
                    while (valid == 0)
                    {
                        num = random.Next(126);
                        if (((num > 64) && (num < 91)) ||
                            ((num > 96) && (num < 123)))
                        {
                            valid = 1;
                        }
                    }
                    ret += (char)num;
                }

                if (!val.Contains("$" + ret + "$")) break;
            }

            return ret;
        }

        /// <summary>
        /// Sqlite sanitizer.
        /// </summary>
        /// <param name="val">String.</param>
        /// <returns>Sanitized string.</returns>
        public static string SqliteSanitizer(string val)
        {
            return SqlServerSanitizer(val); 
        }
    }
}
