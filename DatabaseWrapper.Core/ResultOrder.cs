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
        /// <summary>
        /// Column name on which to order results.
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Direction by which results should be returned.
        /// </summary>
        public OrderDirection Direction { get; set; }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ResultOrder()
        {
            ColumnName = null;
            Direction = OrderDirection.Ascending;
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
    }
}
