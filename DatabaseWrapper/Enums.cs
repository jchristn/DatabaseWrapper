using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    public enum DbTypes
    {
        MsSql,
        MySql
    }

    public enum Operators
    {
        And,
        Or,
        Equals,
        NotEquals,
        In,
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