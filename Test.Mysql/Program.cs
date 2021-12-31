using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper.Mysql;
using DatabaseWrapper.Core;
using ExpressionTree;

namespace Test
{
    class Program
    {
        static Random _Random = new Random(DateTime.Now.Millisecond);
        static DatabaseSettings _Settings;
        static DatabaseClient _Database;
        static byte[] _FileBytes = File.ReadAllBytes("./headshot.png");
        static string _Table = "person";

        static string _Host = "localhost";
        static int _Port = 3306;

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

                Console.Write("User: ");
                string user = Console.ReadLine();

                Console.Write("Password: ");
                string pass = Console.ReadLine();

                _Settings = new DatabaseSettings(DbTypes.Mysql, _Host, _Port, user, pass, "test");
                _Database = new DatabaseClient(_Settings);

                _Database.Logger = Logger;
                _Database.LogQueries = true;
                _Database.LogResults = true;

                #endregion

                #region Drop-Table

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Dropping table '" + _Table + "'...");
                _Database.DropTable(_Table);
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Create-Table

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Creating table '" + _Table + "'...");
                List<Column> columns = new List<Column>();
                columns.Add(new Column("id", true, DataType.Int, 11, null, false));
                columns.Add(new Column("firstname", false, DataType.Nvarchar, 30, null, false));
                columns.Add(new Column("lastname", false, DataType.Nvarchar, 30, null, false));
                columns.Add(new Column("age", false, DataType.Int, 11, null, true));
                columns.Add(new Column("value", false, DataType.Long, 12, null, true));
                columns.Add(new Column("birthday", false, DataType.DateTime, null, null, true));
                columns.Add(new Column("hourly", false, DataType.Decimal, 18, 2, true));
                columns.Add(new Column("localtime", false, DataType.DateTimeOffset, null, null, true));
                columns.Add(new Column("picture", false, DataType.Blob, true));

                _Database.CreateTable(_Table, columns);
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Check-Existence-and-Describe

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Table '" + _Table + "' exists: " + _Database.TableExists(_Table));
                Console.WriteLine("Table '" + _Table + "' configuration:");
                columns = _Database.DescribeTable(_Table);
                foreach (Column col in columns) Console.WriteLine(col.ToString());
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Load-Update-Retrieve-Delete

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Loading rows...");
                LoadRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Loading multiple rows...");
                LoadMultipleRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Checking existence...");
                ExistsRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Counting age...");
                CountAge();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Summing age...");
                SumAge();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Updating rows...");
                UpdateRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows...");
                RetrieveRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows with special character...");
                RetrieveRowsWithSpecialCharacter();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows by index...");
                RetrieveRowsByIndex();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows by between...");
                RetrieveRowsByBetween();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving sorted rows...");
                RetrieveRowsSorted();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Deleting rows...");
                DeleteRows();
                Console.WriteLine("Press ENTER to continue");
                Console.ReadLine();

                #endregion

                #region Cause-Exception

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Testing exception...");

                try
                {
                    _Database.Query("SELECT * FROM " + _Table + "(((");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Caught exception: " + e.Message);
                    Console.WriteLine("Query: " + e.Data["Query"]);
                }

                #endregion

                #region Drop-Table

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Dropping table...");
                _Database.DropTable(_Table);
                Console.ReadLine();

                #endregion
            }
            catch (Exception e)
            {
                ExceptionConsole("Main", "Outer exception", e);
            }
        }

        static void LoadRows()
        {
            for (int i = 0; i < 50; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstname", "first" + i);
                d.Add("lastname", "last" + i);
                d.Add("age", i);
                d.Add("value", i * 1000);
                d.Add("birthday", DateTime.Now);
                d.Add("hourly", 123.456);
                d.Add("localtime", new DateTimeOffset(2021, 4, 14, 01, 02, 03, new TimeSpan(7, 0, 0)));
                d.Add("picture", _FileBytes);
                _Database.Insert(_Table, d);
            }

            for (int i = 0; i < 10; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstname", "firsté" + i);
                d.Add("lastname", "lasté" + i);
                d.Add("age", i);
                d.Add("value", i * 1000);
                d.Add("birthday", DateTime.Now);
                d.Add("hourly", 123.456);
                d.Add("localtime", new DateTimeOffset(2021, 4, 14, 01, 02, 03, new TimeSpan(7, 0, 0)));

                _Database.Insert("person", d);
            }
        }

        static void LoadMultipleRows()
        {
            List<Dictionary<string, object>> dicts = new List<Dictionary<string, object>>();

            for (int i = 0; i < 50; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstname", "firstmultiple" + i);
                d.Add("lastname", "lastmultiple" + i);
                d.Add("age", i);
                d.Add("value", i * 1000);
                d.Add("birthday", DateTime.Now);
                d.Add("hourly", 123.456);
                d.Add("localtime", new DateTimeOffset(2021, 4, 14, 01, 02, 03, new TimeSpan(7, 0, 0)));
                d.Add("picture", _FileBytes);
                dicts.Add(d);
            }

            /*
             * 
             * Uncomment this block if you wish to validate that inconsistent dictionary keys
             * will throw an argument exception.
             * 
            Dictionary<string, object> e = new Dictionary<string, object>();
            e.Add("firstnamefoo", "firstmultiple" + 1000);
            e.Add("lastname", "lastmultiple" + 1000);
            e.Add("age", 100);
            e.Add("value", 1000);
            e.Add("birthday", DateTime.Now);
            e.Add("hourly", 123.456);
            dicts.Add(e);
             *
             */

            _Database.InsertMultiple(_Table, dicts);
        }

        static void ExistsRows()
        {
            Expr e = new Expr("firstname", OperatorEnum.IsNotNull, null);
            Console.WriteLine("Exists: " + _Database.Exists(_Table, e));
        }

        static void CountAge()
        {
            Expr e = new Expr("age", OperatorEnum.GreaterThan, 25);
            Console.WriteLine("Age count: " + _Database.Count(_Table, e));
        }

        static void SumAge()
        {
            Expr e = new Expr("age", OperatorEnum.GreaterThan, 0);
            Console.WriteLine("Age sum: " + _Database.Sum(_Table, "age", e));
        }

        static void UpdateRows()
        {
            for (int i = 10; i < 20; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstname", "first" + i + "-updated");
                d.Add("lastname", "last" + i + "-updated");
                d.Add("age", i);
                d.Add("birthday", null);

                Expr e = new Expr("id", OperatorEnum.Equals, i);
                _Database.Update(_Table, d, e);
            }
        }

        static void RetrieveRows()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age", "picture" };

            for (int i = 30; i < 40; i++)
            {
                Expr e = new Expr
                {
                    Left = new Expr("id", OperatorEnum.LessThan, i),
                    Operator = OperatorEnum.And,
                    Right = new Expr("age", OperatorEnum.LessThan, i)
                };

                // 
                // Yes, personId and age should be the same, however, the example
                // is here to show how to build a nested expression
                //

                ResultOrder[] resultOrder = new ResultOrder[1];
                resultOrder[0] = new ResultOrder("id", OrderDirection.Ascending);

                DataTable result = _Database.Select(_Table, 0, 3, returnFields, e, resultOrder);
                if (result != null && result.Rows != null && result.Rows.Count > 0)
                {
                    foreach (DataRow row in result.Rows)
                    {
                        byte[] data = (byte[])(row["picture"]);
                        Console.WriteLine("Picture data length " + data.Length + " vs original length " + _FileBytes.Length);
                    }
                }
            }
        }

        static void RetrieveRowsWithSpecialCharacter()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };

            Expr e = new Expr("lastname", OperatorEnum.StartsWith, "lasté");

            DataTable result = _Database.Select(_Table, 0, 5, returnFields, e);
            if (result != null && result.Rows != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    Console.WriteLine("Person: " + row["firstname"] + " " + row["lastname"] + " age: " + row["age"]);
                }
            }
        }

        static void RetrieveRowsByIndex()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };

            for (int i = 10; i < 20; i++)
            {
                Expr e = new Expr
                {
                    Left = new Expr("id", OperatorEnum.GreaterThan, 1),
                    Operator = OperatorEnum.And,
                    Right = new Expr("age", OperatorEnum.LessThan, 50)
                };

                // 
                // Yes, personId and age should be the same, however, the example
                // is here to show how to build a nested expression
                //

                ResultOrder[] resultOrder = new ResultOrder[1];
                resultOrder[0] = new ResultOrder("id", OrderDirection.Ascending);

                _Database.Select(_Table, (i - 10), 5, returnFields, e, resultOrder);
            }
        }

        static void RetrieveRowsByBetween()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };
            Expr e = Expr.Between("id", new List<object> { 10, 20 });
            Console.WriteLine("Expression: " + e.ToString());
            _Database.Select(_Table, null, null, returnFields, e);
        }

        static void RetrieveRowsSorted()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };
            Expr e = Expr.Between("id", new List<object> { 10, 20 });
            Console.WriteLine("Expression: " + e.ToString());
            ResultOrder[] resultOrder = new ResultOrder[1];
            resultOrder[0] = new ResultOrder("firstname", OrderDirection.Ascending);
            _Database.Select(_Table, null, null, returnFields, e, resultOrder);
        }

        private static void DeleteRows()
        {
            for (int i = 20; i < 30; i++)
            {
                Expr e = new Expr("id", OperatorEnum.Equals, i);
                _Database.Delete(_Table, e);
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
