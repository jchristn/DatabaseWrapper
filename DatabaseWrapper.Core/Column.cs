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
        public string Name = null;

        /// <summary>
        /// Whether or not the column is the table's primary key.
        /// </summary>
        public bool PrimaryKey = false;

        /// <summary>
        /// The data type of the column.
        /// </summary>
        public DataType Type = DataType.Varchar;

        /// <summary>
        /// The maximum character length of the data contained within the column.
        /// </summary>
        public int? MaxLength = null;

        /// <summary>
        /// For precision, i.e. number of places after the decimal.
        /// </summary>
        public int? Precision = null;

        /// <summary>
        /// Whether or not the column can contain NULL.
        /// </summary>
        public bool Nullable = true;
         
        #endregion

        #region Private-Members

        private List<DataType> _RequiresLengthAndPrecision = new List<DataType>
        {
            DataType.Decimal,
            DataType.Double
        };

        private List<DataType> _RequiresLength = new List<DataType>
        {
            DataType.Nvarchar,
            DataType.Varchar
        };

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
        /// <param name="nullable">Indicate if this column is nullable.</param>
        public Column(string name, bool primaryKey, DataType dt, bool nullable)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (primaryKey && nullable) throw new ArgumentException("Primary key column '" + name + "' cannot be nullable.");

            Name = name;
            PrimaryKey = primaryKey;
            Type = dt; 
            Nullable = nullable;
             
            if (_RequiresLengthAndPrecision.Contains(dt))
            {
                throw new ArgumentException("Column '" + name + "' must include both maximum length and precision; use the constructor that allows these values to be specified.");
            }

            if (_RequiresLength.Contains(dt))
            {
                throw new ArgumentException("Column '" + name + "' must include a maximum length; use the constructor that allows these values to be specified.");
            }
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
            if (primaryKey && nullable) throw new ArgumentException("Primary key column '" + name + "' cannot be nullable.");
            if (maxLen != null && maxLen < 1) throw new ArgumentException("Column '" + name + "' maximum length must be greater than zero if not null.");
            if (precision != null && precision < 1) throw new ArgumentException("Column '" + name + "' preicision must be greater than zero if not null.");

            Name = name;
            PrimaryKey = primaryKey;
            Type = dt;
            MaxLength = maxLen;
            Precision = precision;
            Nullable = nullable;
             
            if (_RequiresLengthAndPrecision.Contains(dt))
            {
                if (maxLen == null || precision == null || maxLen < 1 || precision < 1)
                {
                    throw new ArgumentException("Column '" + name + "' must include both maximum length and precision, and both must be greater than zero.");
                }
            }

            if (_RequiresLength.Contains(dt))
            {
                if (maxLen == null || maxLen < 1)
                {
                    throw new ArgumentException("Column '" + name + "' must include a maximum length, and both must be greater than zero.");
                }
            }
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
                " [Column " + Name + "] ";

            if (PrimaryKey) ret += "PK ";
            ret += "Type: " + Type.ToString() + " ";
            if (MaxLength != null) ret += "MaxLen: " + MaxLength + " ";
            if (Precision != null) ret += "Precision: " + Precision + " ";
            ret += "Nullable: " + Nullable;

            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
