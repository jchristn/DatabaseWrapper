using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Database settings.
    /// </summary>
    public class DatabaseSettings
    {
        #region Public-Members

        /// <summary>
        /// Filename, if using Sqlite.
        /// </summary>
        public string Filename { get; set; } = null;

        /// <summary>
        /// The type of database.
        /// </summary>
        public DbTypes Type { get; set; } = DbTypes.Sqlite;

        /// <summary>
        /// The hostname of the database server.
        /// </summary>
        public string Hostname { get; set; } = null;

        /// <summary>
        /// The TCP port number on which the server is listening.
        /// </summary>
        public int Port { get; set; } = 0;

        /// <summary>
        /// The username to use when accessing the database.
        /// </summary>
        public string Username { get; set; } = null;

        /// <summary>
        /// The password to use when accessing the database.
        /// </summary>
        public string Password { get; set; } = null;

        /// <summary>
        /// For SQL Server Express, the instance name.
        /// </summary>
        public string Instance { get; set; } = null;

        /// <summary>
        /// The name of the database.
        /// </summary>
        public string DatabaseName { get; set; } = null;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DatabaseSettings()
        {

        }

        /// <summary>
        /// Instantiate the object using Sqlite.
        /// </summary>
        /// <param name="filename">The Sqlite database filename.</param>
        public DatabaseSettings(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            Type = DbTypes.Sqlite;
            Filename = filename;
        }

        /// <summary>
        /// Instantiate the object using SQL Server, MySQL, or PostgreSQL.
        /// </summary>
        /// <param name="dbType">The type of database.</param>
        /// <param name="hostname">The hostname of the database server.</param>
        /// <param name="port">The TCP port number on which the server is listening.</param>
        /// <param name="username">The username to use when accessing the database.</param>
        /// <param name="password">The password to use when accessing the database.</param> 
        /// <param name="dbName">The name of the database.</param>
        public DatabaseSettings(string dbType, string hostname, int port, string username, string password, string dbName)
        {
            if (String.IsNullOrEmpty(dbType)) throw new ArgumentNullException(nameof(dbType));
            if (String.IsNullOrEmpty(hostname)) throw new ArgumentNullException(nameof(hostname));
            if (String.IsNullOrEmpty(dbName)) throw new ArgumentNullException(nameof(dbName));

            Type = (DbTypes)(Enum.Parse(typeof(DbTypes), dbType));
            if (Type == DbTypes.Sqlite) throw new ArgumentException("For Sqlite, use the filename constructor.");

            Hostname = hostname;
            Port = port;
            Username = username;
            Password = password;
            Instance = null;
            DatabaseName = dbName;
        }

        /// <summary>
        /// Instantiate the object using SQL Server, MySQL, or PostgreSQL.
        /// </summary>
        /// <param name="dbType">The type of database.</param>
        /// <param name="hostname">The hostname of the database server.</param>
        /// <param name="port">The TCP port number on which the server is listening.</param>
        /// <param name="username">The username to use when accessing the database.</param>
        /// <param name="password">The password to use when accessing the database.</param> 
        /// <param name="dbName">The name of the database.</param>
        public DatabaseSettings(DbTypes dbType, string hostname, int port, string username, string password, string dbName)
        {
            if (String.IsNullOrEmpty(hostname)) throw new ArgumentNullException(nameof(hostname));
            if (String.IsNullOrEmpty(dbName)) throw new ArgumentNullException(nameof(dbName));

            Type = dbType;
            if (Type == DbTypes.Sqlite) throw new ArgumentException("For Sqlite, use the filename constructor for DatabaseSettings.");

            Hostname = hostname;
            Port = port;
            Username = username;
            Password = password;
            Instance = null;
            DatabaseName = dbName;
        }

        /// <summary>
        /// Instantiate the object for SQL Server Express.
        /// </summary> 
        /// <param name="hostname">The hostname of the database server.</param>
        /// <param name="port">The TCP port number on which the server is listening.</param>
        /// <param name="username">The username to use when accessing the database.</param>
        /// <param name="password">The password to use when accessing the database.</param>
        /// <param name="instance">For SQL Server Express, the instance name.</param>
        /// <param name="dbName">The name of the database.</param>
        public DatabaseSettings(string hostname, int port, string username, string password, string instance, string dbName)
        {
            if (String.IsNullOrEmpty(hostname)) throw new ArgumentNullException(nameof(hostname));
            if (String.IsNullOrEmpty(dbName)) throw new ArgumentNullException(nameof(dbName));

            Type = DbTypes.SqlServer;
            Hostname = hostname;
            Port = port;
            Username = username;
            Password = password;
            Instance = instance;
            DatabaseName = dbName;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}