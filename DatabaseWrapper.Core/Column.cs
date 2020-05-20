using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper.Core
{
    /// <summary>
    /// Database table column.
    /// </summary>
    public class Column
    {
        #region Public-Members

        /// <summary>
        /// The name of the column.
        /// </summary>
        public string Name;

        /// <summary>
        /// Whether or not the column is the table's primary key.
        /// </summary>
        public bool PrimaryKey;

        /// <summary>
        /// The data type of the column.
        /// </summary>
        public DataType Type;

        /// <summary>
        /// The maximum character length of the data contained within the column.
        /// </summary>
        public int? MaxLength;

        /// <summary>
        /// For precision, i.e. number of places after the decimal.
        /// </summary>
        public int? Precision;

        /// <summary>
        /// Whether or not the column can contain NULL.
        /// </summary>
        public bool Nullable;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public Column()
        {
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="primaryKey">Indicate if this column is the primary key.</param>
        /// <param name="dt">DataType for the column.</param>
        /// <param name="maxLen">Max length for the column.</param>
        /// <param name="precision">Precision for the column.</param>
        /// <param name="nullable">Indicate if this column is nullable.</param>
        public Column(string name, bool primaryKey, DataType dt, int? maxLen, int? precision, bool nullable)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));

            Name = name;
            PrimaryKey = primaryKey;
            Type = dt;
            MaxLength = maxLen;
            Precision = precision;
            Nullable = nullable;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Produce a human-readable string of the object.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            string ret =
                "  [" + Name + "] ";

            if (PrimaryKey) ret += "PK ";
            ret += "Type: " + Type + " ";
            if (MaxLength != null) ret += "MaxLen: " + MaxLength + " ";
            if (Precision != null) ret += "Precision: " + Precision + " ";
            ret += "Null: " + Nullable;

            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
