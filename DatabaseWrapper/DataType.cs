using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseWrapper
{
    /// <summary>
    /// Type of data contained in the column.
    /// </summary>
    public enum DataType
    { 
        /// <summary>
        /// Variable-length character.
        /// </summary>
        Varchar,
        /// <summary>
        /// Variable-length unicode character.
        /// </summary>
        Nvarchar,
        /// <summary>
        /// Integer.
        /// </summary>
        Int,
        /// <summary>
        /// Long
        /// </summary>
        Long,
        /// <summary>
        /// Decimal
        /// </summary>
        Decimal,
        /// <summary>
        /// Timestamp
        /// </summary>
        DateTime
    }
}
