using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Describe on which columns and in which direction results should be ordered.
    /// </summary>
    public class ResultOrder
    {
        #region Public-Members

        /// <summary>
        /// Column name on which to order results.
        /// </summary>
        public string ColumnName { get; set; } = null;

        /// <summary>
        /// Direction by which results should be returned.
        /// </summary>
        public OrderDirection Direction { get; set; } = OrderDirection.Ascending;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ResultOrder()
        {
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="columnName">Column name on which to order results.</param>
        /// <param name="direction">Direction by which results should be returned.</param>
        public ResultOrder(string columnName, OrderDirection direction)
        {
            if (String.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));
            ColumnName = columnName;
            Direction = direction;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
