﻿namespace DatabaseWrapper.Core
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Runtime.Serialization;

    /// <summary>
    /// Enumeration containing the supported database types.
    /// </summary>
    public enum DbTypeEnum
    {
        /// <summary>
        /// Microsoft SQL Server
        /// </summary>
        [EnumMember(Value = "SqlServer")]
        SqlServer,
        /// <summary>
        /// MySQL
        /// </summary>
        [EnumMember(Value = "Mysql")]
        Mysql,
        /// <summary>
        /// PostgreSQL
        /// </summary>
        [EnumMember(Value = "Postgresql")]
        Postgresql,
        /// <summary>
        /// Sqlite
        /// </summary>
        [EnumMember(Value = "Sqlite")]
        Sqlite,
        /// <summary>
        /// Oracle
        /// </summary>
        [EnumMember(Value = "Oracle")]
        Oracle
    }
}