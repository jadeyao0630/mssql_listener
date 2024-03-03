using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.IdentityModel.Tokens;


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
        private Boolean isFrist = true;
        MySqlServerData mySqlServer;
        public DatabaseChangeNotification(MsSqlServerData server, MySqlServerData mySqlServer)
        {
            if (server != null &&
                !server.server.IsNullOrEmpty() &&
                !server.database.IsNullOrEmpty() &&
                !server.user.IsNullOrEmpty() &&
                !server.password.IsNullOrEmpty())
            {
                this.connectionString = $"server={server.server},{server.port};Database={server.database};User Id={server.user};Password={server.password};Trusted_Connection=True";
            }
            this.mySqlServer = mySqlServer;
        }
        public void StartListening(string[] tables)
        {
            // 确保每次启动监听前都调用Stop方法来清除任何现有的依赖项。
            Console.WriteLine(connectionString);
            SqlDependency.Start(connectionString);
            foreach(var table in tables)
            {
                enabledChangeTracking(table);
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
                    using (SqlCommand command = new SqlCommand("ALTER TABLE dbo." + table + " ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);", connection))
                    {
                        Console.WriteLine(command.ExecuteNonQuery());
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(table + " is enabled change tracking");
                }

                connection.Close();
            }
        }
        private void ListenForChanges(string table)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                MySql mysql = new MySql(mySqlServer);
                using (SqlCommand command = new SqlCommand("SELECT id FROM dbo."+ table, connection))
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
                            
                        sdr.Close();
                    }
                    var _command = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();", connection);
                    var currentVersion = (long)_command.ExecuteScalar();
                    if (!isFrist)
                    {
                        

                        if (_lastSyncVersion < currentVersion)
                        {
                            // 查询变更的数据
                            string query = $@"SELECT CT.* FROM CHANGETABLE(CHANGES dbo.{table}, {_lastSyncVersion}) AS CT";

                            _command = new SqlCommand(query, connection);

                            using (var reader = _command.ExecuteReader())
                            {
                                var primaryKey="";
                                while (reader.Read())
                                {
                                    var changeType = reader["SYS_CHANGE_OPERATION"].ToString();
                                    primaryKey = reader["id"].ToString();

                                    Console.WriteLine($"{table} Change detected: {changeType} on PrimaryKey: {primaryKey}");
                                    //
                                    
                                    // 根据变化类型和主键处理变化
                                }
                                reader.Close();
                                if(!primaryKey.IsNullOrEmpty())
                                {
                                    var __command = new SqlCommand("SELECT * FROM dbo." + table + " WHERE id='" + primaryKey + "';", connection);
                                    using (SqlDataReader sdr = __command.ExecuteReader())
                                    {
                                        while (sdr.Read())
                                        {
                                            //Console.WriteLine();
                                            Console.WriteLine("ID:{0}\t数据:{1}\t", sdr["id"].ToString(), sdr[sdr.GetName(1)].ToString());

                                            if (!mysql.IsTableExist(table))
                                            {
                                                string createTableSql = GetCreateTableStatementFromMsSql(connectionString, table);
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
                        }
                    }
                    else
                    {
                        isFrist = false;
                        if (!mysql.IsTableExist(table))
                        {
                            string createTableSql = GetCreateTableStatementFromMsSql(connectionString, table);
                            Console.WriteLine(createTableSql);

                            mysql.Execute(createTableSql);
                        }
                        if (_lastSyncVersion < currentVersion)
                            _lastSyncVersion = currentVersion; // 更新同步版本号
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
        static string GetCreateTableStatementFromMsSql(string connectionString, string tableName)
        {
            StringBuilder createTableSql = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName} (");

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                string query = $@"
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tableName}'";

                using (var command = new SqlCommand(query, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader["COLUMN_NAME"].ToString();
                        string dataType = reader["DATA_TYPE"].ToString();

                        // Convert MS SQL data type to MySQL data type
                        string mysqlDataType = ConvertMsSqlDataTypeToMySql(dataType);

                        createTableSql.Append($"{columnName} {mysqlDataType}, ");
                    }
                }
            }

            createTableSql.Length -= 2; // Remove last comma and space
            createTableSql.Append(");");

            return createTableSql.ToString();
        }
        static string ConvertMsSqlDataTypeToMySql(string mssqlDataType)
        {
            // Simple data type conversion for demonstration
            // You may need to expand this based on your specific schema
            switch (mssqlDataType.ToLower())
            {
                case "int":
                    return "INT";
                case "varchar":
                    return "VARCHAR(255)";
                // Add more conversions as needed
                default:
                    return "VARCHAR(255)"; // Default conversion
            }
        }
    }
}
