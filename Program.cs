using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using System.IO;
using System.Text.RegularExpressions;
using System.Data;

namespace Mysv
{
    public class Program
    {
        private const string MYSQL_CONNECTION_STR = "server={0};user={1};port={3};password={2};allow user variables=true;default command timeout=1200;charset=utf8;";

        private static string lastExecutedStatement;

        public static void Main(string[] args)
        {
            if (args.Length != 6)
            {
                PrintUsage();
                return;
            }

            Console.Out.Write("Password: ");
            string password = Console.ReadLine();
            Console.Out.WriteLine();

            string action = args[0];

            DateTime startTime = DateTime.Now;

            string path = (args[5].EndsWith("\\")) ? args[5] : args[5] + "\\";

            string[] address = args[1].Split(':');

            if (address.Length != 2) 
            {
                Console.Out.WriteLine("Invalid address!\n\n");
                PrintUsage();
                return;
            }

            if (action == "--dry-run")
                DryRun(address[0], address[1], args[3], password, path);
            else if (action.StartsWith("--create"))
                Create(path, address[0], address[1], args[2], args[3], password, args[4]);
            else if (action == "--update")
                Update(null, address[0], address[1], args[2], args[3], password, path, args[4]);
            else if (action.StartsWith("--update="))
                Update(action.Substring(12), address[0], address[1], args[2], args[3], password, path, args[4]);
            else
            {
                PrintUsage();
                return;
            }
            TimeSpan elapsedTime = DateTime.Now - startTime;
            Console.Out.WriteLine("\nDone in " + elapsedTime.Hours + ":" + elapsedTime.Minutes + ":" + elapsedTime.Seconds + ".");
        }

        private static void PrintUsage() {
            Console.Out.WriteLine("Usage: mysv ACTION ADDRESS DATABASE USERNAME DEFINER CHANGESET_PATH\n\n"+
                                    "ACTION:\n"+
                                    "  --dry-run                           - same as --update but don't actually perform any changes, just show what would be done.\n"+
									"  --create                            - creates initial database (00.00.00.sql) without applying any changesets.\n" +
                                    "  --update                            - update to latest changeset.\n"+
                                    "  --update=CHANGESET_VERSION          - update to specific changeset (current db version should be lower than the CHANGESET_VERSION).\n\n"+
                                    "ADDRESS:\n"+ 
                                    "  MySQL database network address in hostname:port notation. E.g. localhost:3306\n\n"+
                                    "DATABASE:\n"+ 
                                    "  Database name.\n\n"+
                                    "USERNAME:\n"+ 
                                    "  Username for connection. User should have global privileges.\n\n"+
                                    "DEFINER:\n"+    
                                    "  Sql security definer. E.g. `admin`@`%`, `root`@`localhost`, etc.\n\n" +
                                    "CHANGESET_PATH:\n"+
									"  Path to folder with changeset and dataset scripts.\n\n\n" +
									"[v1.0.0. Report mysv bugs to romovs@gmail.com]");
        }

        private static void Update(string maxChangesetVersion, string host, string port, string database, string username, string password, string changesetsPath, string definer)
        {
            MySqlConnection conn = null;
            MySqlCommand cmdSel = new MySqlCommand();
            MySqlCommand cmdCheck = new MySqlCommand();
            MySqlCommand cmdVersion = new MySqlCommand();

            try
            {
                conn = new MySqlConnection(string.Format(MYSQL_CONNECTION_STR, host, username, password, port));
                conn.Open();

                // get current database version
                cmdSel.Connection = conn;
				cmdSel.CommandText = "USE " + database + ";SELECT MAX(version) as version FROM sys_schema_changelog;";

                var reader = cmdSel.ExecuteReader();
                reader.Read();
                object val = reader.GetValue(0);

                if (!(val is string))
                {
					Console.Error.WriteLine("Oooops! sys_schema_changelog should have at least one initial record (00.00.00)");
                    return;
                }
                string currentDbVer = val as string;
                reader.Close();

                // load pending changesets
                string[] changeSetsAvailable = Directory.GetFiles(changesetsPath, "*.sql");
                string[] dataSetsAvailable = Directory.GetFiles(changesetsPath, "*.data*");
                List<string> changeSetsPending = new List<string>();

                Regex r = new Regex(@"\d{2}.\d{2}.\d{2}");

                foreach (string changeSet in changeSetsAvailable)
                {
                    string changeSetName = changeSet.Substring(changeSet.LastIndexOf("\\") + 1);
                    string changeSetVersion = changeSetName.Substring(0, 8);

                    if (!r.IsMatch(changeSetVersion))                  // validate just in case
                    {
                        Console.Error.WriteLine("A changeset with invalid name was found: " + changeSetName);
                        Console.Error.WriteLine("Changeset names should follow xx.xx.xx.sql convention. Where 'x' is an decimal digit.");
                        return;
                    }

                    if (changeSetVersion.CompareTo(currentDbVer) > 0)
                    {
                        if (maxChangesetVersion == null || changeSetVersion.CompareTo(maxChangesetVersion) <= 0)
                            changeSetsPending.Add(changeSetVersion);
                    }
                }

                if (changeSetsPending.Count == 0)
                {
                    Console.Out.WriteLine("Database is up-to-date. No changesets need to be applied.");
                    return;
                }

                changeSetsPending.Sort();

                MySqlParameter p_ver = new MySqlParameter("@version", MySqlDbType.VarChar);
                cmdVersion.Connection = conn;
                cmdVersion.Parameters.Add(p_ver);
				cmdVersion.CommandText = "INSERT INTO sys_schema_changelog (version) VALUES (@version)";
                cmdVersion.Prepare();

                foreach (var changeSet in changeSetsPending)
                {
                    string changeSetName = changeSet + ".sql";

                    Console.Out.Write("Applying " + changeSetName + " ...");
                    // execute the changeset
                    StreamReader streamReader = new StreamReader(changesetsPath + changeSet + ".sql", Encoding.UTF8, false);
                    string sql = streamReader.ReadToEnd();
                    streamReader.Close();

                    sql = Regex.Replace(sql, "DEFINER=`[^`]+`@`[^`]+`", "DEFINER=" + definer);     // replace definer

                    // locate all "LOAD DATA" commands and insert dataset filenames.
                    MatchCollection matches = Regex.Matches(sql, "LOAD DATA LOCAL INFILE '' INTO TABLE [A-Za-z0-9_]*");

                    bool datasetMissing = false;

                    if (matches.Count > 0) 
                    {
                        foreach (Match match in matches)
                        {
                            var tblName = match.Value.Substring(match.Value.LastIndexOf(' ')+1);

                            var datasetPath = (changesetsPath + changeSet + "." + tblName).Replace(@"\", @"\\");

                            sql = sql.Insert(match.Index + "LOAD DATA LOCAL INFILE '".Length, datasetPath);

                            if (!File.Exists(datasetPath))
                            {
                                Console.Error.WriteLine("\n\n Error occured! ");
                                Console.Error.WriteLine(" Dataset " +  changeSet + "." + tblName + " is missing.");
                                datasetMissing = true;
                            }
                        }
                    }

                    if (datasetMissing)
                        throw new SQLExecuteException();

                    MySqlScript script = new MySqlScript(conn, sql);
                    script.Error += new MySqlScriptErrorEventHandler(ScriptErrorHandler);
                    script.StatementExecuted += new MySqlStatementExecutedEventHandler(ScriptStatementExecutedHandler);
                    script.Execute();

                    // update database version
                    p_ver.Value = changeSet;

                    cmdVersion.ExecuteNonQuery();
                    Console.Out.WriteLine(" Done.");

                    // validate views (in case colum was removed from table but still referenced in the view)
                    Console.Out.Write("Validating views ...");
                    cmdSel.CommandText = "SHOW TABLES LIKE '%view_%';";

                    reader = cmdSel.ExecuteReader();
                    bool viewsValid = true;
                    IList<string> views = new List<string>();
                    while (reader.Read())
                        views.Add(reader.GetString(0));

                    reader.Close();

                    cmdCheck.Connection = conn;

                    foreach (var view in views) 
                    {
                        cmdCheck.CommandText = "CHECK TABLE " + view + " QUICK;";
                        var checkReader = cmdCheck.ExecuteReader();

                        checkReader.Read();

                        if (checkReader.GetString("Msg_text") != "OK")
                        {
                            viewsValid = false;
                            Console.Error.WriteLine("\n  View " + view + " is invalid. Make sure all referenced columns exist in table");
                        }

                        checkReader.Close();
                    }

                    if (viewsValid)
                        Console.Out.WriteLine(" Done.");
                    else
                        throw new SQLExecuteException();
                }
            }
            catch (SQLExecuteException)
            {
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Oops!!!!\n" + e.Message);
            }
            finally
            {
                if (conn != null && conn.State != ConnectionState.Closed)
                    conn.Close();
                if (cmdSel != null)
                    cmdSel.Dispose();
                if (cmdVersion != null)
                    cmdVersion.Dispose();
                if (cmdCheck != null)
                    cmdCheck.Dispose();
            }
        }

        static void ScriptStatementExecutedHandler(object sender, MySqlScriptEventArgs args)
        {
            lastExecutedStatement = args.StatementText;
        }

        static void ScriptErrorHandler(object sender, MySqlScriptErrorEventArgs args)
        {
            Console.Error.WriteLine("\n\n Error occured! ");
            Console.Error.WriteLine(" Last successfully executed statement: " + lastExecutedStatement);
            Console.Error.WriteLine(" Exception: " + args.Exception.Message);
            throw new SQLExecuteException();
        }


        private static void DryRun(string host, string port, string username, string password, string changesetsPath)
        {
            Console.Error.WriteLine("--dry-run is not implemented yet.");
        }

        private static void Create(string changesetPath, string host, string port, string database, string username, string password, string definer)
        {
            MySqlConnection conn = null;

            string createDb = "SET NET_READ_TIMEOUT = 1200;DROP DATABASE IF EXISTS " + database + ";" +
                                "CREATE DATABASE " + database + " DEFAULT CHARACTER SET utf8;" +
                                "USE " + database + ";";
            try
            {
                Console.Out.Write("Creating initial database from 00.00.00.sql ...");
				StreamReader streamReader = new StreamReader(changesetPath + "00.00.00.sql", Encoding.UTF8, false);
                string sql = createDb + streamReader.ReadToEnd();
                streamReader.Close();

                sql = Regex.Replace(sql, "DEFINER=`[^`]+`@`[^`]+`", "DEFINER=" + definer);     // replace definer

                conn = new MySqlConnection(string.Format(MYSQL_CONNECTION_STR, host, username, password, port));
                conn.Open();
                MySqlScript script = new MySqlScript(conn, sql);
                script.Error += new MySqlScriptErrorEventHandler(ScriptErrorHandler);
                script.StatementExecuted += new MySqlStatementExecutedEventHandler(ScriptStatementExecutedHandler);
                script.Execute();
            }
            catch (SQLExecuteException)
            {
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Oops!!!!\n" + e.Message);
            }
            finally
            {
                if (conn != null && conn.State != ConnectionState.Closed)
                    conn.Close();
            }
        }

    }

    public class SQLExecuteException : Exception
    {
        public SQLExecuteException()
            : base() { }

        public SQLExecuteException(string message)
            : base(message) { }

        public SQLExecuteException(string format, params object[] args)
            : base(string.Format(format, args)) { }

        public SQLExecuteException(string message, Exception innerException)
            : base(message, innerException) { }

        public SQLExecuteException(string format, Exception innerException, params object[] args)
            : base(string.Format(format, args), innerException) { }
    }
}
