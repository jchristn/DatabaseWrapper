﻿namespace Test
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DatabaseWrapper.Postgresql;
    using DatabaseWrapper.Core;
    using ExpressionTree;

    class Program
    {
        static Random _Random = new Random(DateTime.Now.Millisecond);
        static DatabaseSettings _Settings;
        static DatabaseClient _Database;
        static byte[] _FileBytes = File.ReadAllBytes("./headshot.png");

        static string _Host = "localhost";
        static int _Port = 5432;

        // To use dbo.person, change _Table to either 'dbo.person' or just 'person'
        // To use with a specific schema, use 'schema.table', i.e. 'foo.person'
        static string _Table = "person";

        static async Task Main(string[] args)
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

                _Settings = new DatabaseSettings(DbTypeEnum.Postgresql, _Host, _Port, user, pass, "test");
                _Settings.Debug.Logger = Logger;
                _Settings.Debug.EnableForQueries = true;
                _Settings.Debug.EnableForResults = true;

                _Database = new DatabaseClient(_Settings);

                #endregion

                #region Sanitize-Data

                string[] attacks = {
                    "' OR '1'='1",
                    "'; DROP TABLE Users; --",
                    "' UNION SELECT username, password FROM Users--",
                    "' OR 1=1--",
                    "admin' --",
                    "'; EXEC xp_cmdshell 'net user';--",
                    "' OR 'x'='x",
                    "1 OR 1=1",
                    "1; SELECT * FROM Users",
                    "' OR id IS NOT NULL OR id = '",
                    "username' AND 1=0 UNION ALL SELECT 'admin', '81dc9bdb52d04dc20036dbd8313ed055'--",
                    "' OR '1'='1' /*",
                    "' UNION ALL SELECT NULL, NULL, NULL, CONCAT(username,':',password) FROM Users--",
                    "' AND (SELECT * FROM (SELECT(SLEEP(5)))bAKL) AND 'vRxe'='vRxe",
                    "'; WAITFOR DELAY '0:0:5'--",
                    "The quick brown fox jumped over the lazy dog"
                };

                for (int i = 0; i < 8; i++) Console.WriteLine("");
                Console.WriteLine("Sanitizing input strings");
                foreach (string attack in attacks)
                    Console.WriteLine("| " + attack + " | Sanitized: " + _Database.SanitizeString(attack));

                #endregion

                #region Drop-Table

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Dropping table '" + _Table + "'...");
                await _Database.DropTableAsync(_Table);
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Create-Table

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Creating table '" + _Table + "'...");
                List<Column> columns = new List<Column>();
                columns.Add(new Column("id", true, DataTypeEnum.Int, 11, null, false));
                columns.Add(new Column("firstname", false, DataTypeEnum.Nvarchar, 30, null, false));
                columns.Add(new Column("lastname", false, DataTypeEnum.Nvarchar, 30, null, false));
                columns.Add(new Column("age", false, DataTypeEnum.Int, 11, null, true));
                columns.Add(new Column("value", false, DataTypeEnum.Long, 12, null, true));
                columns.Add(new Column("birthday", false, DataTypeEnum.DateTime, null, null, true));
                columns.Add(new Column("hourly", false, DataTypeEnum.Decimal, 18, 2, true));
                columns.Add(new Column("localtime", false, DataTypeEnum.DateTimeOffset, null, null, true));
                columns.Add(new Column("picture", false, DataTypeEnum.Blob, true));
                columns.Add(new Column("guid", false, DataTypeEnum.Guid, true));
                columns.Add(new Column("active", false, DataTypeEnum.Boolean, true));

                await _Database.CreateTableAsync(_Table, columns);
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Check-Existence-and-Describe

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Table '" + _Table + "' exists: " + await _Database.TableExistsAsync(_Table));
                Console.WriteLine("Table '" + _Table + "' configuration:");
                columns = await _Database.DescribeTableAsync(_Table);
                foreach (Column col in columns) Console.WriteLine(col.ToString());
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                #endregion

                #region Load-Update-Retrieve-Delete

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Loading rows...");
                await LoadRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Loading multiple rows...");
                await LoadMultipleRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Checking existence...");
                await ExistsRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Counting age...");
                await CountAge();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Summing age...");
                await SumAge();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Updating rows...");
                await UpdateRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows...");
                await RetrieveRows();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows with special character...");
                await RetrieveRowsWithSpecialCharacter();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows by index...");
                await RetrieveRowsByIndex();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving rows by between...");
                await RetrieveRowsByBetween();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Retrieving sorted rows...");
                await RetrieveRowsSorted();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                for (int i = 0; i < 24; i++) Console.WriteLine("");
                Console.WriteLine("Deleting rows...");
                await DeleteRows();
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
                Console.WriteLine(e.ToString());
            }
        }

        static async Task LoadRows()
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
                d.Add("guid", Guid.NewGuid());
                d.Add("active", (i % 2 > 0));

                await _Database.InsertAsync(_Table, d);
            }

            for (int i = 0; i < 10; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstname", "firsté" + i);
                d.Add("lastname", "lastШЋЖŠĆŽ" + i);
                d.Add("age", i);
                d.Add("value", i * 1000);
                d.Add("birthday", DateTime.Now);
                d.Add("hourly", 123.456);
                d.Add("localtime", new DateTimeOffset(2021, 4, 14, 01, 02, 03, new TimeSpan(7, 0, 0)));
                d.Add("guid", Guid.NewGuid());
                d.Add("active", (i % 2 > 0));

                await _Database.InsertAsync(_Table, d);
            }
        }

        static async Task LoadMultipleRows()
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
                d.Add("guid", Guid.NewGuid());
                d.Add("active", (i % 2 > 0));

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

            await _Database.InsertMultipleAsync(_Table, dicts);
        }

        static async Task ExistsRows()
        {
            Expr e = new Expr("firstname", OperatorEnum.IsNotNull, null);
            Console.WriteLine("Exists: " + await _Database.ExistsAsync(_Table, e));
        }

        static async Task CountAge()
        {
            Expr e = new Expr("age", OperatorEnum.GreaterThan, 25);
            Console.WriteLine("Age count: " + await _Database.CountAsync(_Table, e));
        }

        static async Task SumAge()
        {
            Expr e = new Expr("age", OperatorEnum.GreaterThan, 0);
            Console.WriteLine("Age sum: " + await _Database.SumAsync(_Table, "age", e));
        }

        static async Task UpdateRows()
        {
            for (int i = 10; i < 20; i++)
            {
                Dictionary<string, object> d = new Dictionary<string, object>();
                d.Add("firstname", "first" + i + "-updated");
                d.Add("lastname", "last" + i + "-updated");
                d.Add("age", i);
                d.Add("birthday", null);

                Expr e = new Expr("id", OperatorEnum.Equals, i);
                await _Database.UpdateAsync(_Table, d, e);
            }
        }

        static async Task RetrieveRows()
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
                resultOrder[0] = new ResultOrder("id", OrderDirectionEnum.Ascending);

                DataTable result = await _Database.SelectAsync(_Table, 0, 3, returnFields, e, resultOrder);
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

        static async Task RetrieveRowsWithSpecialCharacter()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };

            Expr e = new Expr("firstname", OperatorEnum.StartsWith, "firsté");

            ResultOrder[] resultOrder = new ResultOrder[1];
            resultOrder[0] = new ResultOrder("id", OrderDirectionEnum.Ascending);

            DataTable result = await _Database.SelectAsync(_Table, 0, 5, returnFields, e, resultOrder);
            if (result != null && result.Rows != null && result.Rows.Count > 0)
            {
                foreach (DataRow row in result.Rows)
                {
                    Console.WriteLine("Person: " + row["firstname"] + " " + row["lastname"] + " age: " + row["age"]);
                }
            }
        }

        static async Task RetrieveRowsByIndex()
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
                resultOrder[0] = new ResultOrder("id", OrderDirectionEnum.Ascending);

                await _Database.SelectAsync(_Table, (i - 10), 5, returnFields, e, resultOrder);
            }
        }

        static async Task RetrieveRowsByBetween()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };
            Expr e = Expr.Between("id", new List<object> { 10, 20 });
            Console.WriteLine("Expression: " + e.ToString());
            await _Database.SelectAsync(_Table, null, null, returnFields, e);
        }

        static async Task RetrieveRowsSorted()
        {
            List<string> returnFields = new List<string> { "firstname", "lastname", "age" };
            Expr e = Expr.Between("id", new List<object> { 10, 20 });
            Console.WriteLine("Expression: " + e.ToString());
            ResultOrder[] resultOrder = new ResultOrder[1];
            resultOrder[0] = new ResultOrder("firstname", OrderDirectionEnum.Ascending);
            await _Database.SelectAsync(_Table, null, null, returnFields, e, resultOrder);
        }

        private static async Task DeleteRows()
        {
            for (int i = 20; i < 30; i++)
            {
                Expr e = new Expr("id", OperatorEnum.Equals, i);
                await _Database.DeleteAsync(_Table, e);
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
