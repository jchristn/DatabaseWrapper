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
        MsSql,
        MySql
    }

    /// <summary>
    /// Enumeration containing the supported WHERE clause operators.
    /// </summary>
    public enum Operators
    {
        And,
        Or,
        Equals,
        NotEquals,
        In,
        NotIn,
        Contains,
        ContainsNot,
        GreaterThan,
        GreaterThanOrEqualTo,
        LessThan,
        LessThanOrEqualTo,
        IsNull,
        IsNotNull
    }
}