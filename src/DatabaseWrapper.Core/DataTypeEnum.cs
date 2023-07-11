using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Type of data contained in the column.
    /// </summary>
    public enum DataTypeEnum
    {
        /// <summary>
        /// Variable-length character.
        /// </summary>
        [EnumMember(Value = "Varchar")]
        Varchar,
        /// <summary>
        /// Variable-length unicode character.
        /// </summary>
        [EnumMember(Value = "Nvarchar")]
        Nvarchar,
        /// <summary>
        /// Tiny integer.
        /// </summary>
        [EnumMember(Value = "TinyInt")]
        TinyInt,
        /// <summary>
        /// Integer.
        /// </summary>
        [EnumMember(Value = "Int")]
        Int,
        /// <summary>
        /// Long
        /// </summary>
        [EnumMember(Value = "Long")]
        Long,
        /// <summary>
        /// Decimal
        /// </summary>
        [EnumMember(Value = "Decimal")]
        Decimal,
        /// <summary>
        /// Double
        /// </summary>
        [EnumMember(Value = "Double")]
        Double,
        /// <summary>
        /// Timestamp
        /// </summary>
        [EnumMember(Value = "DateTime")]
        DateTime,
        /// <summary>
        /// Timestamp with offset.
        /// </summary>
        [EnumMember(Value = "DateTimeOffset")]
        DateTimeOffset,
        /// <summary>
        /// Blob
        /// </summary>
        [EnumMember(Value = "Blob")]
        Blob,
        /// <summary>
        /// GUID.
        /// </summary>
        [EnumMember(Value = "Guid")]
        Guid,
        /// <summary>
        /// Boolean.
        /// </summary>
        [EnumMember(Value = "Boolean")]
        Boolean,
        /// <summary>
        /// Unknown
        /// </summary>
        [EnumMember(Value = "Unknown")]
        Unknown
    }
}