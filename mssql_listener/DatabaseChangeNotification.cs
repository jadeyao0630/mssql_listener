using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.IdentityModel.Tokens;
using System.Reflection.PortableExecutable;
using System.Drawing;
using Microsoft.VisualBasic;
using System.ComponentModel;
using System.Diagnostics.Metrics;


namespace mssql_listener
{
    public class MsSqlServerData
    {
        public string server = "";
        public string database = "";
        public int port = 1433;
        public string user = "";
        public string password = "";

    }
    internal class DatabaseChangeNotification
    {
        private string connectionString = "server=192.168.10.122,1433;Database=glory;User Id=sa;Password=qijiashe6;Trusted_Connection=True";
        private long _lastSyncVersion = 0; // 初始同步版本号
        private Boolean isFrist = false;
        MySqlServerData mySqlServer;
        MsSqlServerData server;
        public DatabaseChangeNotification(MsSqlServerData server, MySqlServerData mySqlServer)
        {
            if (server != null &&
                !server.server.IsNullOrEmpty() &&
                !server.database.IsNullOrEmpty() &&
                !server.user.IsNullOrEmpty() &&
                !server.password.IsNullOrEmpty())
            {
                this.connectionString = $"server={server.server},{server.port};Database={server.database};User Id={server.user};Password={server.password};";
            }
            this.mySqlServer = mySqlServer;
            this.server = server;
        }
        public void StartListening(string[] tables)
        {
            // 确保每次启动监听前都调用Stop方法来清除任何现有的依赖项。
            Console.WriteLine(connectionString);
            SqlDependency.Start(connectionString);
            foreach(var table in tables)
            {
                enabledChangeTracking(table);
                Console.WriteLine();
                ListenForChanges(table);
            }
            Console.Read();
            SqlDependency.Stop(connectionString);
        }
        private void enabledChangeTracking(string table)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                
                try
                {
                    using (SqlCommand command = new SqlCommand("ALTER DATABASE "+ server.database + " SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);", connection))
                    {
                        Console.WriteLine(command.ExecuteNonQuery());
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(server.database + " is already enabled change tracking");
                }
                try
                {
                    using (SqlCommand command = new SqlCommand("ALTER TABLE dbo." + table + " ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);", connection))
                    {
                        Console.WriteLine(command.ExecuteNonQuery());
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(table + " is already enabled change tracking");
                }

                connection.Close();
            }
        }
        private void ListenForChanges(string table)
        {
            List<string> pk = GetPrimartyKey(connectionString, table);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                
                MySql mysql = new MySql(mySqlServer);
                using (SqlCommand command = new SqlCommand("SELECT "+ String.Join(',', pk) +" FROM dbo." + table, connection))
                {
                    // 创建SqlDependency并订阅OnChange事件。
                   // SqlDependency dependency = new SqlDependency(command);
                    //dependency.OnChange += new OnChangeEventHandler(OnDatabaseChange);

                    // 必须执行命令，SqlDependency才能开始监听变更。

                    command.CommandType = CommandType.Text;
                    SqlDependency dependency = new SqlDependency(command);
                    dependency.OnChange += new OnChangeEventHandler((s,e)=>OnDatabaseChange(s,e, table));
                    
                    using (SqlDataReader sdr = command.ExecuteReader())
                    {
                        //Console.WriteLine();
                        //while (sdr.Read())
                        //{
                            //Console.WriteLine()
                        //}
                        sdr.Close();
                    }
                    var _command = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();", connection);
                    var currentVersion = (long)(_command.ExecuteScalar() is DBNull ? Convert.ToInt64(0) : _command.ExecuteScalar());
                    if (!isFrist)
                    {
                        if (!mysql.IsTableExist("sync_version"))
                        {

                            string createTableSql = "CREATE TABLE sync_version (table_name VARCHAR(255),last_sync_version BIGINT,PRIMARY KEY (`table_name`)); ";
                            Console.WriteLine(createTableSql);

                            mysql.Execute(createTableSql);
                        }
                        Console.WriteLine($"{table} Change detected: {_lastSyncVersion} - {currentVersion}");
                        string updateTableSql = "SELECT * FROM sync_version WHERE table_name = '" + table + "';";
                        foreach (var item in mysql.Get(updateTableSql))
                        {
                            Console.WriteLine(item);
                        }


                        if (_lastSyncVersion < currentVersion)
                        {
                            // 查询变更的数据
                            string query = $@"SELECT CT.* FROM CHANGETABLE(CHANGES dbo.{table}, {_lastSyncVersion}) AS CT";

                            _command = new SqlCommand(query, connection);

                            using (var reader = _command.ExecuteReader())
                            {
                                var primaryKey="";
                                var pkName = "";
                                while (reader.Read())
                                {
                                    if(pk.Count > 0)
                                    {
                                        var changeType = reader["SYS_CHANGE_OPERATION"].ToString();
                                        primaryKey = reader[pk[0]].ToString();
                                        pkName = pk[0];
                                        Console.WriteLine($"{table} Change detected: {changeType} on PrimaryKey: {primaryKey} - {pk[0]}");

                                    }
                                    //
                                    
                                    // 根据变化类型和主键处理变化
                                }
                                reader.Close();
                                if(!primaryKey.IsNullOrEmpty() && !pkName.IsNullOrEmpty())
                                {
                                    var __command = new SqlCommand("SELECT * FROM dbo." + table + " WHERE "+ pkName +"='" + primaryKey + "';", connection);
                                    using (SqlDataReader sdr = __command.ExecuteReader())
                                    {
                                        while (sdr.Read())
                                        {
                                            //Console.WriteLine();
                                            Console.WriteLine("ID:{0}\t数据:{1}\t", sdr[pkName].ToString(), sdr[sdr.GetName(1)].ToString());

                                            if (!mysql.IsTableExist(table))
                                            {
                                                string createTableSql = GetCreateTableStatementFromMsSql(connectionString, table, pk)["query"].ToString();
                                                Console.WriteLine(createTableSql);

                                                mysql.Execute(createTableSql);
                                            }
                                            else
                                            {
                                                Console.WriteLine($"{table} 存在");
                                            }
                                            
                                            //Console.WriteLine(mysql.Get("select * from cases"));
                                        }
                                        sdr.Close();
                                    }
                                }
                                
                            }

                            _lastSyncVersion = currentVersion; // 更新同步版本号
                            string updateSql = "REPLACE INTO sync_version (table_name ,last_sync_version) VALUES ('"+table+"','"+ _lastSyncVersion + "'); ";
                            //Console.WriteLine(createTableSql);

                            mysql.Execute(updateSql);
                        }
                    }
                    else
                    {
                        isFrist = false;
                        Dictionary<string, object> result = GetCreateTableStatementFromMsSql(connectionString, table, pk);
                        Dictionary<string, string> columnTypes= result["columnData"] as Dictionary<string, string>;
                        if (!mysql.IsTableExist("sync_version"))
                        {

                            string createTableSql = "CREATE TABLE sync_version (table_name VARCHAR(255),last_sync_version BIGINT,PRIMARY KEY (`table_name`)); ";
                            Console.WriteLine(createTableSql);

                            mysql.Execute(createTableSql);
                        }
                        if (!mysql.IsTableExist(table))
                        {
                            
                            string createTableSql = result["query"].ToString();
                            Console.WriteLine(createTableSql);

                            mysql.Execute(createTableSql);
                        }
                        if (_lastSyncVersion < currentVersion)
                            _lastSyncVersion = currentVersion; // 更新同步版本号
                        var __command = new SqlCommand("SELECT * FROM dbo." + table, connection);
                        using (SqlDataReader sdr = __command.ExecuteReader())
                        {
                            
                            var count = 0;
                            while (sdr.Read())
                            {
                                List<string> columns = [];
                                List<object> values = [];
                                for (int i = 0; i < sdr.FieldCount; i++)
                                {
                                    var columnName = sdr.GetName(i);
                                    columns.Add(columnName);
                                    //Console.WriteLine(columnName + ": " + sdr.GetValue(i)+"["+ sdr.IsDBNull(i)+"]"+"--"+ DBNull.Value);
                                    if (sdr.IsDBNull(i)) values.Add("NULL");
                                    else
                                    {
                                        if (columnTypes != null && columnTypes.Count > 0 && columnTypes.ContainsKey(columnName))
                                        {
                                            switch (columnTypes[columnName].ToLower())
                                            {
                                                case "int":
                                                    values.Add(sdr.GetValue(i));
                                                    break;
                                                case "tinyint":
                                                    values.Add(sdr.GetValue(i));
                                                    break;
                                                case "text":
                                                    values.Add("'" + sdr.GetValue(i) + "'");
                                                    break;
                                                case "money":
                                                    values.Add(sdr.GetValue(i));
                                                    break;
                                                case "datetime":
                                                    values.Add("'" + sdr.GetValue(i)+ "'" );
                                                    break;
                                                case "timestamp":
                                                    string versionString = BitConverter.ToString((byte[])sdr.GetValue(i)).Replace("-", "");
                                                    values.Add("'" + versionString + "'");
                                                    break;
                                                case "uniqueidentifier":
                                                    values.Add("'" + sdr.GetValue(i) + "'");
                                                    break;
                                                case "nvarchar":
                                                    values.Add("'" + sdr.GetValue(i) + "'");
                                                    break;
                                                case "varchar":
                                                    values.Add("'" + sdr.GetValue(i) + "'");
                                                    break;
                                                default:
                                                    values.Add("'" + sdr.GetValue(i) + "'");
                                                    break;
                                            }
                                        }
                                        else
                                            values.Add(sdr.GetValue(i));
                                    }
                                    
                                }
                                count++;
                                //if (count > 2) break;
                                string query = $"REPLACE INTO {table} ({String.Join(',', columns)}) VALUES ({String.Join(',', values)})";
                                //Console.WriteLine(query);
                                mysql.Execute(query);

                            }
                            sdr.Close();
                        }
                    }
                    

                }
            }
        }

        private void OnDatabaseChange(object sender, SqlNotificationEventArgs e,string table)
        {
            // 当监听到变更时，这个方法会被调用。
            //Console.WriteLine(e);
            Console.WriteLine("数据库{3}变更通知：类型={0}, 信息={1}, 源={2}", e.Type, e.Info, e.Source, table);

            // 重新开始监听下一次的变更。
            ListenForChanges(table);
        }

        public void StopListening()
        {
            SqlDependency.Stop(connectionString);
        }
        static Dictionary<string, object> GetCreateTableStatementFromMsSql(string connectionString, string tableName, List<string> pks)
        {
            StringBuilder createTableSql = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName} (");
            Dictionary<string, string> columnTypes = new Dictionary<string, string>();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                string query = $@"
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tableName}'";
                using (var command = new SqlCommand(query, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader["COLUMN_NAME"].ToString();

                        bool isPk = pks.Contains(columnName);
                        string dataType = reader["DATA_TYPE"].ToString();
                        columnTypes[columnName] = dataType;
                        object charMaxLength = reader["CHARACTER_MAXIMUM_LENGTH"];
                        string maxLengthStr = charMaxLength != DBNull.Value ? charMaxLength.ToString() : "255";
                        // Convert MS SQL data type to MySQL data type
                        string mysqlDataType = ConvertMsSqlDataTypeToMySql(dataType, maxLengthStr)+(isPk? " PRIMARY KEY" : "");

                        createTableSql.Append($"{columnName} {mysqlDataType}, ");
                    }
                }
            }

            createTableSql.Length -= 2; // Remove last comma and space
            createTableSql.Append(") ROW_FORMAT=COMPRESSED;");
            return new Dictionary<string, object> { { "query", createTableSql }, { "columnData", columnTypes } };
        }
        static string ConvertMsSqlDataTypeToMySql(string mssqlDataType,string length)
        {
            // Simple data type conversion for demonstration
            // You may need to expand this based on your specific schema
            switch (mssqlDataType.ToLower())
            {
                case "int":
                    return "INT";
                case "tinyint":
                    return "TINYINT";
                case "text":
                    return "TEXT";
                case "money":
                    return "DECIMAL(19, 4)";
                case "datetime":
                    return "DATETIME";
                case "timestamp":
                    return "VARCHAR(36)";
                case "uniqueidentifier":
                    return "VARCHAR(36)";
                case "nvarchar":
                    return "VARCHAR(" + length + ") CHARACTER SET utf8mb4";
                case "varchar":
                    return "VARCHAR("+ length+")";
                // Add more conversions as needed
                default:
                    return "VARCHAR("+ length+")"; // Default conversion
            }
        }
        static List<string> GetPrimartyKey(string connectionString, string tableName)
        {
            List<string> pk=[];
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = @"
                SELECT 
                    kcu.TABLE_NAME,
                    kcu.COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE 
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = @TableName;";

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableName", tableName);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string columnName = reader["COLUMN_NAME"].ToString();
                            Console.WriteLine($"Primary key column: {columnName}");
                            pk.Add(columnName);
                        }
                    }
                }
            }
            return pk;
        }
        static void createVersionTable()
        {

        }
    }
}
