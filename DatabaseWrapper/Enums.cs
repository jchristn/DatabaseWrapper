using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    /// <summary>
    /// Enumeration containing the supported database types.
    /// </summary>
    public enum DbTypes
    {
        /// <summary>
        /// Microsoft SQL Server
        /// </summary>
        MsSql,
        /// <summary>
        /// MySQL
        /// </summary>
        MySql,
        /// <summary>
        /// PostgreSQL
        /// </summary>
        PgSql,
        /// <summary>
        /// Sqlite
        /// </summary>
        Sqlite
    }

    /// <summary>
    /// Enumeration containing the supported WHERE clause operators.
    /// </summary>
    public enum Operators
    {
        /// <summary>
        /// Boolean AND
        /// </summary>
        And,
        /// <summary>
        /// Boolean OR
        /// </summary>
        Or,
        /// <summary>
        /// Values are equal to one another
        /// </summary>
        Equals,
        /// <summary>
        /// Values are not equal to one another
        /// </summary>
        NotEquals,
        /// <summary>
        /// Value is contained within a list
        /// </summary>
        In,
        /// <summary>
        /// Value is not contained within a list
        /// </summary>
        NotIn,
        /// <summary>
        /// Value contains the specified value
        /// </summary>
        Contains,
        /// <summary>
        /// Value does not contain the specified value
        /// </summary>
        ContainsNot,
        /// <summary>
        /// Value starts with the specified value
        /// </summary>
        StartsWith,
        /// <summary>
        /// Value ends with the specified value
        /// </summary>
        EndsWith,
        /// <summary>
        /// Value is greater than the specified value
        /// </summary>
        GreaterThan,
        /// <summary>
        /// Value is greater than or equal to the specified value
        /// </summary>
        GreaterThanOrEqualTo,
        /// <summary>
        /// Value is less than the specified value
        /// </summary>
        LessThan,
        /// <summary>
        /// Value is less than or equal to the specified value
        /// </summary>
        LessThanOrEqualTo,
        /// <summary>
        /// Value is null
        /// </summary>
        IsNull,
        /// <summary>
        /// Value is not null
        /// </summary>
        IsNotNull
    }

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
        /// Double
        /// </summary>
        Double,
        /// <summary>
        /// Timestamp
        /// </summary>
        DateTime,
        /// <summary>
        /// Blob
        /// </summary>
        Blob
    }
}