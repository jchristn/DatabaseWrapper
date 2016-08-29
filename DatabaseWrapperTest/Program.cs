using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper;

namespace DatabaseWrapperTest
{
    class Program
    { 
        //
        //
        // Before attempting to run this program, be sure to create the person table in a database
        // named 'test' per the scripts found in the samples folder, and set the constructor with
        // the appropriate parameters!
        //
        //

        static DatabaseClient client;

        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("Displaying prepended AND expression...");
                PrependAndTest();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Displaying nested AND expression...");
                DisplayNestedAndExpression();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                Console.WriteLine("Displaying nested OR expression...");
                DisplayNestedOrExpression();
                Console.WriteLine("Press ENTER to continue...");
                Console.ReadLine();

                // MsSql
                // client = new DatabaseClient(DbTypes.MsSql, "localhost", 0, null, null, "SQLEXPRESS", "test");

                // MySql
                client = new DatabaseClient(DbTypes.MySql, "127.0.0.1", 3306, "root", "password", null, "test");

                client.DebugRawQuery = true;
                client.DebugResultRowCount = true;

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

                Console.WriteLine("Deleting rows...");
                DeleteRows();
                Console.WriteLine("Press ENTER to continue");
                Console.ReadLine();
            }
            catch (Exception e)
            {
                ExceptionConsole("Main", "Outer exception", e);
            }
            finally
            {
                Console.WriteLine("Press ENTER to exit");
                Console.ReadLine();
            }
        }

        static void PrependAndTest()
        {
            Expression e1 = new Expression
            {
                LeftTerm = "key1",
                Operator = Operators.Or, 
                RightTerm = new Expression
                {
                    LeftTerm = "key2",
                    Operator = Operators.And,
                    RightTerm = "val2"
                }
            };

            Expression e2 = new Expression
            {
                LeftTerm = "something",
                Operator = Operators.LessThan,
                RightTerm = "22"
            };

            Expression e3 = Expression.PrependAndClause(e1, e2);
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
                d.Add("notes", "This is person number " + i);

                client.Insert("person", d);
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
                d.Add("notes", "This is updated person number " + i);

                Expression e = new Expression
                {
                    LeftTerm = "personId",
                    Operator = Operators.Equals,
                    RightTerm = i
                };

                client.Update("person", d, e);
            }
        }

        static void RetrieveRows()
        {
            List<string> returnFields = new List<string> { "firstName", "lastName", "age" };

            for (int i = 30; i < 40; i++)
            {
                Expression e = new Expression
                {
                    LeftTerm = new Expression
                    {
                        LeftTerm = "personId",
                        Operator = Operators.LessThan,
                        RightTerm = i
                    },
                    Operator = Operators.And,
                    RightTerm = new Expression
                    {
                        LeftTerm = "age",
                        Operator = Operators.LessThan,
                        RightTerm = i
                    }
                };

                // 
                // Yes, personId and age should be the same, however, the example
                // is here to show how to build a nested expression
                //

                client.Select("person", 3, returnFields, e, null);
            }
        }

        static void DeleteRows()
        {
            for (int i = 20; i < 30; i++)
            {
                Expression e = new Expression
                {
                    LeftTerm = "personId",
                    Operator = Operators.Equals,
                    RightTerm = i
                };

                client.Delete("person", e);
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
    }
}
