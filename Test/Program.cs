using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper;

namespace Test
{
    class Program
    {
        static string _DbType;
        static string _Filename;
        static string _Username;
        static string _Password;
        static DatabaseClient _Database; 

        static void Main(string[] args)
        { 
            try
            {
                #region Select-Database-Type

                /*
                 * 
                 * 
                 * Create the database 'test' before proceeding if using mssql, mysql, or pgsql
                 * 
                 * 
                 */

                Console.Write("DB type [mssql|mysql|pgsql|sqlite]: ");
                _DbType = Console.ReadLine();
                if (String.IsNullOrEmpty(_DbType)) return;
                _DbType = _DbType.ToLower();

                if (_DbType.Equals("mssql") || _DbType.Equals("mysql") || _DbType.Equals("pgsql"))
                {
                    Console.Write("User: ");
                    _Username = Console.ReadLine();

                    Console.Write("Password: ");
                    _Password = Console.ReadLine();

                    switch (_DbType)
                    {
                        case "mssql":
                            _Database = new DatabaseClient(DbTypes.MsSql, "localhost", 1433, _Username, _Password, null, "test");
                            break;
                        case "mysql":
                            _Database = new DatabaseClient(DbTypes.MySql, "localhost", 3306, _Username, _Password, null, "test");
                            break;
                        case "pgsql":
                            _Database = new DatabaseClient(DbTypes.PgSql, "localhost", 5432, _Username, _Password, null, "test");
                            break;
                        default:
                            return;
                    }
                }
                else if (_DbType.Equals("sqlite"))
                {
                    Console.Write("Filename: ");
                    _Filename = Console.ReadLine();
                    if (String.IsNullOrEmpty(_Filename)) return;

                    _Database = new DatabaseClient(_Filename);
                }
                else
                {
                    Console.WriteLine("Invalid database type.");
                    return;
                }
                 
                _Database.Logger = Logger;
                _Database.LogQueries = true;
                _Database.LogResults = true;

                #endregion

                #region Drop-Table

                Console.WriteLine("Dropping table 'person'...");
                _Database.DropTable("person");
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Create-Table

                Console.WriteLine("Creating table 'person'...");
                List<Column> columns = new List<Column>();
                columns.Add(new Column("id", true, DataType.Int, 11, null, false));
                columns.Add(new Column("firstName", false, DataType.Nvarchar, 30, null, false));
                columns.Add(new Column("lastName", false, DataType.Nvarchar, 30, null, false));
                columns.Add(new Column("age", false, DataType.Int, 11, null, true));
                columns.Add(new Column("value", false, DataType.Long, 12, null, true));
                columns.Add(new Column("birthday", false, DataType.DateTime, null, null, true));
                columns.Add(new Column("hourly", false, DataType.Decimal, 18, 2, true));

                _Database.CreateTable("person", columns);
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Check-Existence-and-Describe

                Console.WriteLine("Table 'person' exists: " + _Database.TableExists("person"));
                Console.WriteLine("Table 'person' configuration:");
                columns = _Database.DescribeTable("person");
                foreach (Column col in columns) Console.WriteLine(col.ToString());
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Load-Update-Retrieve-Delete

                Console.WriteLine("Loading rows...");
                LoadRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Updating rows...");
                UpdateRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Retrieving rows...");
                RetrieveRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Retrieving rows by index...");
                RetrieveRowsByIndex();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Deleting rows...");
                DeleteRows();
                Console.WriteLine("Press ENTER to continue");
                Console.ReadLine();

                #endregion

                #region Drop-Table

                Console.WriteLine("Dropping table...");
                _Database.DropTable("person");
                Console.ReadLine();

                #endregion
            }
            catch (Exception e)
            {
                ExceptionConsole("Main", "Outer exception", e);
            } 
        }

        static void PrependAndTest()
        {
            Expression e1 = new Expression
            {
                LeftTerm = "key1",
                Operator = Operators.Or,
                RightTerm = new Expression("key2", Operators.And, "val2")
            };

            Expression e2 = new Expression("something", Operators.LessThan, "22");
            Expression e3 = Expression.PrependAndClause(e1, e2);
            Console.WriteLine(e3.ToWhereClause(DbTypes.MsSql));
        }

        static void PrependOrTest()
        {
            Expression e1 = new Expression
            {
                LeftTerm = "key1",
                Operator = Operators.Or,
                RightTerm = new Expression("key2", Operators.And, "val2")
            };

            Expression e2 = new Expression("something", Operators.LessThan, 22);
            Expression e3 = Expression.PrependOrClause(e1, e2);
            Console.WriteLine(e3.ToWhereClause(DbTypes.MsSql));
        }

        static void DisplayNestedAndExpression()
        {
            List<Expression> exprList = new List<Expression>();
            Expression e1 = new Expression { LeftTerm = "key1", Operator = Operators.Or, RightTerm = "val1" };
            Expression e2 = new Expression { LeftTerm = "key2", Operator = Operators.And, RightTerm = "val2" };
            Expression e3 = new Expression { LeftTerm = "key3", Operator = Operators.GreaterThan, RightTerm = "val3" };
            Expression e4 = new Expression { LeftTerm = "key4", Operator = Operators.LessThan, RightTerm = "val4" };
            exprList.Add(e1);
            exprList.Add(e2);
            exprList.Add(e3);
            exprList.Add(e4);
            Expression e = Expression.ListToNestedAndExpression(exprList);
            Console.WriteLine(e.ToWhereClause(DbTypes.MsSql));
        }

        static void DisplayNestedOrExpression()
        {
            List<Expression> exprList = new List<Expression>();
            Expression e1 = new Expression { LeftTerm = "key1", Operator = Operators.Or, RightTerm = "val1" };
            Expression e2 = new Expression { LeftTerm = "key2", Operator = Operators.And, RightTerm = "val2" };
            Expression e3 = new Expression { LeftTerm = "key3", Operator = Operators.GreaterThan, RightTerm = "val3" };
            Expression e4 = new Expression { LeftTerm = "key4", Operator = Operators.LessThan, RightTerm = "val4" };
            exprList.Add(e1);
            exprList.Add(e2);
            exprList.Add(e3);
            exprList.Add(e4);
            Expression e = Expression.ListToNestedOrExpression(exprList);
            Console.WriteLine(e.ToWhereClause(DbTypes.MsSql));
        }

        static void LoadRows()
        {
            for (int i = 0; i < 50; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstName", "first" + i);
                d.Add("lastName", "last" + i);
                d.Add("age", i);
                d.Add("value", i * 1000);
                d.Add("birthday", DateTime.Now);
                d.Add("hourly", 123.456);

                _Database.Insert("person", d);
            }
        }

        static void UpdateRows()
        {
            for (int i = 10; i < 20; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstName", "first" + i + "-updated");
                d.Add("lastName", "last" + i + "-updated");
                d.Add("age", i); 

                Expression e = new Expression("id", Operators.Equals, i);
                _Database.Update("person", d, e);
            }
        }

        static void RetrieveRows()
        {
            List<string> returnFields = new List<string> { "firstName", "lastName", "age" };

            for (int i = 30; i < 40; i++)
            {
                Expression e = new Expression
                {
                    LeftTerm = new Expression("id", Operators.LessThan, i),
                    Operator = Operators.And,
                    RightTerm = new Expression("age", Operators.LessThan, i)
                };

                // 
                // Yes, personId and age should be the same, however, the example
                // is here to show how to build a nested expression
                //

                _Database.Select("person", 0, 3, returnFields, e, "ORDER BY id ASC");
            }
        }

        static void RetrieveRowsByIndex()
        {
            List<string> returnFields = new List<string> { "firstName", "lastName", "age" };

            for (int i = 10; i < 20; i++)
            {
                Expression e = new Expression
                {
                    LeftTerm = new Expression("id", Operators.GreaterThan, 1),
                    Operator = Operators.And,
                    RightTerm = new Expression("age", Operators.LessThan, 50)
                };

                // 
                // Yes, personId and age should be the same, however, the example
                // is here to show how to build a nested expression
                //

                _Database.Select("person", (i - 10), 5, returnFields, e, "ORDER BY age DESC");
            }
        }

        private static void DeleteRows()
        {
            for (int i = 20; i < 30; i++)
            {
                Expression e = new Expression("id", Operators.Equals, i);
                _Database.Delete("person", e);
            }
        }
        
        private static string StackToString()
        {
            string ret = "";

            StackTrace t = new StackTrace();
            for (int i = 0; i < t.FrameCount; i++)
            {
                if (i == 0)
                {
                    ret += t.GetFrame(i).GetMethod().Name;
                }
                else
                {
                    ret += " <= " + t.GetFrame(i).GetMethod().Name;
                }
            }

            return ret;
        }

        private static void ExceptionConsole(string method, string text, Exception e)
        {
            var st = new StackTrace(e, true);
            var frame = st.GetFrame(0);
            int line = frame.GetFileLineNumber();
            string filename = frame.GetFileName();

            Console.WriteLine("---");
            Console.WriteLine("An exception was encountered which triggered this message.");
            Console.WriteLine("  Method: " + method);
            Console.WriteLine("  Text: " + text);
            Console.WriteLine("  Type: " + e.GetType().ToString());
            Console.WriteLine("  Data: " + e.Data);
            Console.WriteLine("  Inner: " + e.InnerException);
            Console.WriteLine("  Message: " + e.Message);
            Console.WriteLine("  Source: " + e.Source);
            Console.WriteLine("  StackTrace: " + e.StackTrace);
            Console.WriteLine("  Stack: " + StackToString());
            Console.WriteLine("  Line: " + line);
            Console.WriteLine("  File: " + filename);
            Console.WriteLine("  ToString: " + e.ToString());
            Console.WriteLine("---");

            return;
        }

        private static void Logger(string msg)
        {
            Console.WriteLine(msg);
        }
    }
}
