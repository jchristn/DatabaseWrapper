using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Direction by which results should be returned.
    /// </summary>
    public enum OrderDirection
    {
        /// <summary>
        /// Return results in ascending order.
        /// </summary>
        [EnumMember(Value = "Ascending")]
        Ascending,
        /// <summary>
        /// Return results in descending order.
        /// </summary>
        [EnumMember(Value = "Descending")]
        Descending
    }
}