namespace DatabaseWrapper.Core
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;

    /// <summary>
    /// Database query event arguments.
    /// </summary>
    public class DatabaseQueryEvent : EventArgs
    {
        #region Public-Members

        /// <summary>
        /// Query.
        /// </summary>
        public string Query { get; private set; } = null;

        /// <summary>
        /// Total runtime in milliseconds.
        /// </summary>
        public double TotalMilliseconds { get; private set; } = 0;

        /// <summary>
        /// Result.
        /// </summary>
        public DataTable Result { get; private set; } = null;

        /// <summary>
        /// Number of rows returned.
        /// </summary>
        public int RowsReturned
        {
            get
            {
                if (Result != null
                    && Result.Rows != null)
                {
                    return Result.Rows.Count;
                }

                return 0;
            }
        }

        /// <summary>
        /// Exception, if any.
        /// </summary>
        public Exception Exception { get; private set; } = null;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate.
        /// </summary>
        /// <param name="query">Query.</param>
        /// <param name="totalMs">Total runtime in milliseconds.</param>
        /// <param name="result">Result.</param>
        /// <param name="ex">Exception, if any.</param>
        public DatabaseQueryEvent(string query, double totalMs, DataTable result, Exception ex = null)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(nameof(query));
            if (totalMs < 0) throw new ArgumentOutOfRangeException(nameof(totalMs));
            Query = query;
            TotalMilliseconds = totalMs;
            Result = result;
            Exception = ex;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
