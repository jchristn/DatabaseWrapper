﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatabaseWrapper;
using DatabaseWrapper.Core;
using ExpressionTree;
using GetSomeInput;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace Test.DatabaseConsole
{
    class Program
    {
        static Random _Random = new Random(DateTime.Now.Millisecond);
        static DatabaseSettings _Settings;
        static DatabaseClient _Database;

        // To use dbo.person, change _Table to either 'dbo.person' or just '" + _Table + "'
        // To use with a specific schema, use 'schema.table', i.e. 'foo.person'
        static string _DbType = "mysql";
        static string _User = "root";
        static string _Password = "password";
        static string _Hostname = "localhost";
        static int _Port = 3306;
        static string _DbName = "test";
        static bool _Debug = true;
        static bool _RunForever = true;

        static void Main(string[] args)
        {
            #region Instantiate

            /*
                * 
                * 
                * Create the database 'test' before proceeding if using mssql, mysql, or pgsql
                * 
                * 
                */

            _DbType = Inputty.GetString("DB type [sqlserver|mysql|postgresql|sqlite]:", _DbType, false).ToLower();

            if (_DbType.Equals("sqlserver") || _DbType.Equals("mysql") || _DbType.Equals("postgresql"))
            {
                Console.WriteLine("Ensure the database has been created beforehand.");
                Console.WriteLine("");

                _User = Inputty.GetString("User:", "root", false);
                _Password = Inputty.GetString("Pass:", null, true);
                _Hostname = Inputty.GetString("Host:", _Hostname, false);
                _Port = Inputty.GetInteger("Port:", _Port, true, false);
                _DbName = Inputty.GetString("Database:", _DbName, true);

                switch (_DbType)
                {
                    case "sqlserver":
                        _Settings = new DatabaseSettings(_Hostname, _Port, _User, _Password, null, _DbName);
                        _Database = new DatabaseClient(_Settings);
                        break;
                    case "mysql":
                        _Settings = new DatabaseSettings(DbTypes.Mysql, _Hostname, _Port, _User, _Password, _DbName);
                        _Database = new DatabaseClient(_Settings);
                        break;
                    case "postgresql":
                        _Settings = new DatabaseSettings(DbTypes.Postgresql, _Hostname, _Port, _User, _Password, _DbName);
                        _Database = new DatabaseClient(_Settings);
                        break;
                    default:
                        return;
                }
            }
            else if (_DbType.Equals("sqlite"))
            {
                string filename = Inputty.GetString("Filename:", "sqlite.db", false);
                _Settings = new DatabaseSettings(filename);
                _Database = new DatabaseClient(_Settings);
            }
            else
            {
                Console.WriteLine("Invalid database type.");
                return;
            }

            #endregion

            #region Menu

            while (_RunForever)
            {
                try
                {
                    string userInput = Inputty.GetString("Command [? for help]:", null, false);
                    if (userInput.Equals("q"))
                    {
                        _RunForever = false;
                    }
                    else if (userInput.Equals("cls"))
                    {
                        Console.Clear();
                    }
                    else if (userInput.Equals("debug"))
                    {
                        if (_Debug)
                        {
                            _Debug = false;
                            _Settings.Debug.Logger = null;
                            _Settings.Debug.EnableForQueries = false;
                            _Settings.Debug.EnableForResults = false;
                        }
                        else
                        {
                            _Debug = true;
                            _Settings.Debug.Logger = Logger;
                            _Settings.Debug.EnableForQueries = true;
                            _Settings.Debug.EnableForResults = true;
                        }
                    }
                    else if (userInput.Equals("?"))
                    {
                        Console.WriteLine("");
                        Console.WriteLine("Available commands:");
                        Console.WriteLine("  q         quit");
                        Console.WriteLine("  ?         help, this menu");
                        Console.WriteLine("  cls       clear the screen");
                        Console.WriteLine("  debug     enable/disable debug, currently " + _Debug);
                        Console.WriteLine("  [query]   execute a query");
                        Console.WriteLine("");
                    }
                    else
                    {
                        DataTable result = _Database.Query(userInput);
                        if (result == null || result.Rows.Count < 1)
                        {
                            Console.WriteLine("");
                            Console.WriteLine("(none)");
                            Console.WriteLine("");
                        }
                        else
                        {
                            Console.WriteLine("");
                            dynamic data = Helper.DataTableToListDynamic(result);
                            string json = SerializeJson(data, true);
                            Console.WriteLine(json);
                            Console.WriteLine("");
                            Console.WriteLine(result.Rows.Count + " row(s)");
                            Console.WriteLine("");
                        }
                    }
                }
                catch (Exception e)
                {
                    ExceptionConsole("Main", "Exception", e);
                }
            }

            #endregion
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

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        public static string SerializeJson(object obj, bool pretty)
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
        {
            if (obj == null) return null;
            string json;

            if (pretty)
            {
                json = JsonConvert.SerializeObject(
                  obj,
                  Newtonsoft.Json.Formatting.Indented,
                  new JsonSerializerSettings
                  {
                      NullValueHandling = NullValueHandling.Ignore,
                      DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                      Converters = new List<JsonConverter> { new StringEnumConverter() }
                  });
            }
            else
            {
                json = JsonConvert.SerializeObject(obj,
                  new JsonSerializerSettings
                  {
                      NullValueHandling = NullValueHandling.Ignore,
                      DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                      Converters = new List<JsonConverter> { new StringEnumConverter() }
                  });
            }

            return json;
        }
    }
}