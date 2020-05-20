using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Enumeration containing the supported WHERE clause operators.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum Operators
    {
        /// <summary>
        /// Boolean AND
        /// </summary>
        [EnumMember(Value = "And")]
        And,
        /// <summary>
        /// Boolean OR
        /// </summary>
        [EnumMember(Value = "Or")]
        Or,
        /// <summary>
        /// Values are equal to one another
        /// </summary>
        [EnumMember(Value = "Equals")]
        Equals,
        /// <summary>
        /// Values are not equal to one another
        /// </summary>
        [EnumMember(Value = "NotEquals")]
        NotEquals,
        /// <summary>
        /// Value is contained within a list
        /// </summary>
        [EnumMember(Value = "In")]
        In,
        /// <summary>
        /// Value is not contained within a list
        /// </summary>
        [EnumMember(Value = "NotIn")]
        NotIn,
        /// <summary>
        /// Value contains the specified value
        /// </summary>
        [EnumMember(Value = "Contains")]
        Contains,
        /// <summary>
        /// Value does not contain the specified value
        /// </summary>
        [EnumMember(Value = "ContainsNot")]
        ContainsNot,
        /// <summary>
        /// Value starts with the specified value
        /// </summary>
        [EnumMember(Value = "StartsWith")]
        StartsWith,
        /// <summary>
        /// Value ends with the specified value
        /// </summary>
        [EnumMember(Value = "EndsWith")]
        EndsWith,
        /// <summary>
        /// Value is greater than the specified value
        /// </summary>
        [EnumMember(Value = "GreaterThan")]
        GreaterThan,
        /// <summary>
        /// Value is greater than or equal to the specified value
        /// </summary>
        [EnumMember(Value = "GreaterThanOrEqualTo")]
        GreaterThanOrEqualTo,
        /// <summary>
        /// Value is less than the specified value
        /// </summary>
        [EnumMember(Value = "LessThan")]
        LessThan,
        /// <summary>
        /// Value is less than or equal to the specified value
        /// </summary>
        [EnumMember(Value = "LessThanOrEqualTo")]
        LessThanOrEqualTo,
        /// <summary>
        /// Value is null
        /// </summary>
        [EnumMember(Value = "IsNull")]
        IsNull,
        /// <summary>
        /// Value is not null
        /// </summary>
        [EnumMember(Value = "IsNotNull")]
        IsNotNull
    }
}