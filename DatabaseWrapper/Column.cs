using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseWrapper
{
    public class Column
    {
        #region Constructor

        public Column()
        {
        }

        #endregion

        #region Public-Members

        public string Name;
        public bool IsPrimaryKey;
        public string DataType;
        public int? MaxLength;
        public bool Nullable;

        #endregion

        #region Private-Members
        
        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
