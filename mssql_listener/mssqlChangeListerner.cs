﻿
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace mssql_listener
{
    public enum databaseType{
        mssql,mysql
    }
    public class database
    {
        public string server;
        public int port=1433;
        public string databaseName;
        public string user;
        public string password;
        public databaseType type= databaseType.mssql;
        public bool isTrusted = true;
        public string getConnectionString(bool withDB=true)
        {
            var dbName = !withDB ? "" : "Database=" + this.databaseName + ";";
            return $"server={this.server},{this.port};{dbName}User Id={this.user};Password={this.password};Trusted_Connection={this.isTrusted}";
        }
    }
    
    internal class mssqlChangeListerner
    {
        private Dictionary<string, string[]> pkMatcher = new Dictionary<string, string[]>{
            {"p_Project", ["ProjGUID"] },
            {"cb_Product", ["ProductGUID"] },
            {"myBusinessUnit", ["BUGUID"] },
            {"p_Building", ["BldGUID"] },
            {"p_Room", ["RoomGUID"] },
            {"s_Contract", ["ContractGUID"] },
            {"cb_Contract", ["ContractGUID"] },
            {"s_Fee", ["FeeGUID"] },
            {"s_Getin", ["GetinGUID"] },
            {"s_Order", ["OrderGUID"] },
            {"cb_Cost", ["CostGUID"] },
            {"cb_ContractProj", ["ContractGUID", "ProjGUID"] },
            {"cb_HTFKApply", ["HTFKApplyGUID"] },
            {"cb_Pay", ["PayGUID"] },
            {"cb_HTAlter", ["HTAlterGUID"] },
        };
        private SqlConnection connection;
        private SqlConnection connection_sync;
        private SqlConnection target_connection;
        private string connectionStr;
        private database targetDatabase;
        private database database;
        private Dictionary<string, Dictionary<string, Dictionary<string, object>>> tableConstructors = new Dictionary<string, Dictionary<string, Dictionary<string, object>>>();
        public mssqlChangeListerner(database database,database targetDatabase)
        {
            connectionStr = database.getConnectionString();
            this.database = database;
            this.targetDatabase = targetDatabase;
            Console.WriteLine(connectionStr);
            Console.WriteLine(targetDatabase.getConnectionString());
            target_connection = new SqlConnection(targetDatabase.getConnectionString());
            connection = new SqlConnection(connectionStr);
            connection_sync = new SqlConnection(connectionStr);
        }
        public void Start(string[] tableNames,bool isInit=false)
        {
            Console.WriteLine("isInit: " + isInit);
            connection.Open();

            connection_sync.Open();
            /*
            try
            {
                target_connection.Open();
                if(isInit) {  deleteDatabase(database.databaseName); }
                target_connection.ChangeDatabase(database.databaseName);
            }
            catch (Exception e)
            {
                //Console.WriteLine(e.ToString());
                target_connection.Close();
                Console.WriteLine(targetDatabase.getConnectionString(false));
                target_connection = new SqlConnection(targetDatabase.getConnectionString(false));
                target_connection.Open();
                try
                {
                    //Console.WriteLine("CREATE DATABASE " + database.databaseName + " COLLATE Chinese_PRC_CI_AS;");
                    using (SqlCommand command = new SqlCommand("CREATE DATABASE " + database.databaseName + " COLLATE Chinese_PRC_CI_AS;", target_connection))
                    {
                        Console.WriteLine(command.ExecuteNonQuery());
                        Console.WriteLine(database.databaseName + " is created");
                        
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(database.databaseName + " is already exsited");
                    //Console.WriteLine(ex.ToString());
                }
                target_connection.ChangeDatabase(database.databaseName);
            }
            
            // 确保SqlDependency的启动
            enableDatabaseBrokker(database.databaseName);
            SqlDependency.Start(connectionStr);
            var count = 0;
            foreach (var table in tableNames) { 
                copyTableStructure(table );

                enabledChangeTracking(table);
                //ListenForChanges(table);
                syncChanged(table, getCurrentChangedId(), getLastChangedId(table), ListenForChanges(table));
                count++;
                Console.WriteLine(table+" is on tracking..."+ count+"/"+ tableNames.Length);
            }
            */
            createChangeLogTable();
            foreach (var table in tableNames)
            {
                addTrigger(table);
            }
            Console.Read();
            connection.Close();

            connection_sync.Close();
            target_connection.Close();
            SqlDependency.Stop(connectionStr);
        }
        private void addTrigger(string tableName)
        {
            var addTriggerQuery = $@"
CREATE TRIGGER trigger_{tableName}
ON {tableName}
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'';

    -- 构建动态 SQL 语句
    SET @sql = 'INSERT INTO ChangeLog (Action, ChangeTime) ' +
               'SELECT CASE WHEN EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted) THEN ''INSERT'' ' +
               'WHEN EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted) THEN ''DELETE'' ' +
               'ELSE ''UPDATE'' END, GETDATE();'

    -- 执行动态 SQL
    EXEC sp_executesql @sql;
END;
";
            try
            {
                using (SqlCommand command = new SqlCommand(addTriggerQuery, connection))
                {
                    Console.WriteLine("ChangeLog created " + command.ExecuteNonQuery());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ChangeLog is existed" + e.Message);
            }
        }
        private void createChangeLogTable()
        {
            var query = "IF NOT ExISTS( SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ChangeLog]') AND type in (N'U')) "+
                "BEGIN " +
                    "CREATE TABLE ChangeLog (Action VARCHAR(255),DataSnapshot  NVARCHAR(MAX), ChangeTime datetime);" +
                "END;";
            try
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    Console.WriteLine("ChangeLog created " + command.ExecuteNonQuery());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ChangeLog is existed" + e.Message);
            }
        }
        private List<string> ListenForChanges(string tableName)
        {
            
            var pks = tableConstructors[tableName]["data"]["pks"] as List<string>;
            //Console.WriteLine(String.Join(",", pks));
            /*
            if (pks.Count == 0 && pkMatcher.ContainsKey(tableName))
            {
                pks.Add(pkMatcher[tableName]);
                setPrimaryKey(tableName, pkMatcher[tableName]);
            }
            */
            if (pks.Count == 0)
            {
                Console.WriteLine(tableName+" does not have a primaryKey - "+ string.Join(",", pks));
                //return [];
            }
            using (SqlCommand command = new SqlCommand($"SELECT {String.Join(",", pks)} FROM dbo." + tableName, connection))
            {
                Console.WriteLine($"SELECT {String.Join(",", pks)} FROM dbo." + tableName);
                command.CommandType = CommandType.Text;
                // 创建一个SqlDependency并绑定OnChange事件
                SqlDependency dependency = new SqlDependency(command);
                dependency.OnChange += (s,e)=>OnDatabaseChange(s,e,tableName);

                // 必须执行命令
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    // 在这里处理数据
                    /*
                    while (reader.Read())
                    {
                        Console.WriteLine(reader[0]);
                    }
                    */
                    reader.Close();
                }

            }
            return pks;
        }
        private void deleteDatabase(string dbName)
        {
            var query = $"ALTER DATABASE {dbName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;DROP DATABASE {dbName};";
            try
            {
                using (SqlCommand command = new SqlCommand(query, target_connection))
                {
                    Console.WriteLine(dbName+" deleteDatabase " + command.ExecuteNonQuery());
                }
            }catch (Exception e)
            {
                Console.WriteLine(dbName+" deleteDatabase " + e.Message);
            }
            
        }
        private long getCurrentChangedId()
        {
            var _command = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();", connection);
            return (long)(_command.ExecuteScalar() is DBNull ? Convert.ToInt64(0) : _command.ExecuteScalar());

        }
        private long getLastChangedId(string tableName)
        {
            var isExists = checkIfTableExists("sync_version");
            Console.WriteLine(isExists);
            if (!isExists)
            {
                var query = $"CREATE TABLE sync_version (table_name VARCHAR(255),last_sync_version BIGINT, PRIMARY KEY (table_name));";
                using (SqlCommand command = new SqlCommand(query, target_connection))
                {
                    Console.WriteLine(command.ExecuteNonQuery());
                }
                return -1;
            }
            else
            {
                string query = $"SELECT last_sync_version FROM sync_version WHERE table_name = '{tableName}';";
                using (SqlCommand select_command = new SqlCommand(query, target_connection))
                {
                    using (SqlDataReader sdr = select_command.ExecuteReader())
                    {
                        if (sdr.HasRows)
                        {
                            sdr.Read();
                            return sdr.GetInt64(0);
                        }
                        else
                        {
                            return -1;
                        }
                        
                    }
                }
            }
        }
        private bool checkIfTableExists(string tableName)
        {
            var query = $"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{tableName}'";
            using (SqlCommand select_command = new SqlCommand(query, target_connection))
            {
                using (SqlDataReader sdr = select_command.ExecuteReader())
                {
                    return sdr.HasRows;
                }
            }
        }
        private void syncChanged(string tableName, long currentVersion, long lastSyncVersion,List<string> pks)
        {
            Console.WriteLine("tableName: " + tableName);
            Console.WriteLine("currentVersion: " + currentVersion);
            Console.WriteLine("lastSyncVersion: " + lastSyncVersion);
            Console.WriteLine("primary keys: " + String.Join(",",pks));
            bool isInit = lastSyncVersion == -1;
            lastSyncVersion = lastSyncVersion == -1 ? 0 : lastSyncVersion;
            if (lastSyncVersion < currentVersion)
            {
                try
                {
                    string query = $@"SELECT CT.* FROM CHANGETABLE(CHANGES dbo.{tableName}, {lastSyncVersion}) AS CT ORDER BY SYS_CHANGE_VERSION;";

                    var command = new SqlCommand(query, connection);

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            var count = 0;
                            List<Dictionary<string, string>> changedData = new List<Dictionary<string, string>>();
                            while (reader.Read())
                            {
                                if (pks.Count > 0)
                                {
                                    var changeType = reader["SYS_CHANGE_OPERATION"].ToString();
                                    var primaryKey = reader[pks[0]].ToString();
                                    var pkName = pks[0];
                                    Console.WriteLine($"{tableName} Change detected: {changeType} on PrimaryKey: {primaryKey} - {pkName}");
                                    changedData.Add(new Dictionary<string, string>() {
                                {"changeType",changeType },
                                {"primaryKey",primaryKey },
                                {"pkName",pkName },
                                {"version",reader["SYS_CHANGE_VERSION"].ToString() },
                            });

                                }
                                //Console.WriteLine(count);
                                count++;
                            }
                            reader.Close();
                            var _count = 0;
                            foreach (var data in changedData)
                            {
                                _count++;
                                if (!String.IsNullOrEmpty(data["changeType"]) && !String.IsNullOrEmpty(data["primaryKey"]))
                                {
                                    applyChanges(data["changeType"], tableName, data["pkName"], data["primaryKey"], data["version"], _count == changedData.Count);
                                }
                            }
                        }
                        else
                        {
                            if (isInit)
                            {
                                Console.WriteLine("This is no changes in " + tableName + ", set the last change version to " + currentVersion);
                                updateLastVersion(tableName, currentVersion.ToString());
                            }
                        }


                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine( "syncChanged " + e.Message);
                }
                
            }
        }
        private void applyChanges(string changeType, string tableName, string pkName, string primaryKey,string version,bool isLast)
        {
            var query="";
            if (changeType == "D")
            {
                query = $"DELETE FROM dbo.{tableName} WHERE {pkName} = '{primaryKey}'";
            }
            else
            {
                using (SqlCommand _command = new SqlCommand($@"SELECT * FROM dbo.{tableName} WHERE {pkName} = '{primaryKey}'", connection))
                {
                    using (var reader = _command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();
                            StringBuilder changeQuery = new StringBuilder();
                            List<string> updates = new List<string>();
                            List<string> columns = new List<string>();
                            List<object> values = new List<object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var dataType = tableConstructors[tableName][columnName]["dataType"].ToString().ToLower();
                                var val= reader.GetValue(i);
                                if (reader.IsDBNull(i))
                                {
                                    val= DBNull.Value;
                                }else if(dataType == "datetime")
                                {
                                    //Console.WriteLine(columnName+": "+val);
                                    val = "CONVERT(datetime, '"+ val + "')";
                                   // val = reader.GetDateTime(i);
                                }else if (dataType == "int" || dataType == "tinyint" || dataType == "money")
                                {
                                    val = reader.GetValue(i);
                                }
                                else
                                {
                                    val ="'" + val + "'";
                                }
                                if (dataType != "timestamp" && val != DBNull.Value)
                                {
                                        updates.Add(columnName + " = " + val);
                                        columns.Add(columnName);
                                        values.Add(val);
                                }
                                
                            }
                            query = $"UPDATE dbo.{tableName} SET {String.Join(",", updates)} WHERE {pkName} = '{primaryKey}' " +
                                    "IF @@ROWCOUNT = 0 " +
                                    "BEGIN " +
                                        $"INSERT INTO dbo.{tableName} ({String.Join(',', columns)}) VALUES ({String.Join(',', values)})" +
                                    "END;";
                        }
                        reader.Close();
                    }

                }
                
            }
            if (!String.IsNullOrEmpty(query))
            {
                //Console.WriteLine(query);
                try
                {
                    using (SqlCommand command = new SqlCommand(query, target_connection))
                    {
                        Console.WriteLine(command.ExecuteNonQuery());
                        if(isLast) updateLastVersion(tableName, version);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("applyChanges: "+ex.Message+", "+ query);
                    //Console.WriteLine(query);
                }
            }
            
        }
        private void updateLastVersion(string tableName, string lastSyncVersion)
        {
            //string updateSql = "INSERT INTO sync_version (table_name ,last_sync_version) VALUES ('" + tableName + "','" + lastSyncVersion + "'); ";
            string updateSql = $"UPDATE dbo.sync_version SET last_sync_version = { lastSyncVersion } WHERE table_name = '{tableName}' " +
            "IF @@ROWCOUNT = 0 " +
            "BEGIN " +
                $"INSERT INTO dbo.sync_version(table_name, last_sync_version) VALUES('{tableName}', {lastSyncVersion}) " +
            "END;";
            Console.WriteLine(updateSql);
            using (SqlCommand command = new SqlCommand(updateSql, target_connection))
            {
                Console.WriteLine(command.ExecuteNonQuery());
            }
        }
        private void syncAll(string tableName)
        {
            Console.WriteLine("syncing data of " + tableName);
            try
            {
                using (SqlCommand command = new SqlCommand($"TRUNCATE TABLE dbo.{tableName}", target_connection))
                {
                    Console.WriteLine(command.ExecuteNonQuery());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            using (SqlCommand command = new SqlCommand($"SELECT * FROM dbo.{tableName}", connection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    // 将数据复制到目标数据库

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(target_connection))
                    {
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.DestinationTableName = tableName;

                        try
                        {
                            bulkCopy.WriteToServer(reader);
                            Console.WriteLine(tableName+" copied...");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"复制数据时出现错误：{ex.Message}");
                        }
                    }
                    
                }
            }
        }
        private void copyTableStructure(string tableName )
        {
            List<string> pks = GetPrimartyKey(tableName);
            StringBuilder createTableSql = new StringBuilder($"CREATE TABLE dbo.{tableName} (");
            Dictionary<string, string> columnTypes = new Dictionary<string, string>();
            string query = $@"
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tableName}'";
            using (var command = new SqlCommand(query, connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    string columnName = reader.GetString("COLUMN_NAME");
                    string dataType = reader.GetString("DATA_TYPE");
                    columnTypes[columnName] = dataType;
                    object charMaxLength = reader["CHARACTER_MAXIMUM_LENGTH"];

                    //dataType = dataType == "text" && charMaxLength.ToString() == "2147483647" ? "VARCHAR" : dataType;
                    charMaxLength = charMaxLength.ToString() == "2147483647" || dataType == "ntext" ? DBNull.Value : charMaxLength;
                    charMaxLength = charMaxLength.ToString() == "-1" ? "max" : charMaxLength;

                    string maxLengthStr = charMaxLength != DBNull.Value ? "(" + charMaxLength + ")" : "";
                    createTableSql.Append($"{columnName} {dataType}{maxLengthStr}, ");
                    if (!tableConstructors.ContainsKey(tableName))
                    {
                        tableConstructors[tableName] = new Dictionary<string, Dictionary<string, object>>();
                    }
                    tableConstructors[tableName][columnName] = new Dictionary<string, object> {
                        { "dataType", dataType } ,
                        { "charMaxLength", charMaxLength } ,
                        { "isPk", pks.Contains(columnName) } ,
                    };
                    
                }
                if (pks.Count == 0 && pkMatcher.ContainsKey(tableName))
                {
                    pks.AddRange(pkMatcher[tableName]);
                    setPrimaryKey(tableName, pkMatcher[tableName][0]);
                }
                tableConstructors[tableName]["data"] = new Dictionary<string, object> { { "pks", pks } };
            }
            if(pks.Count>0) createTableSql.Append($"PRIMARY KEY ({String.Join(",", pks)})");
            createTableSql.Append(")");
            try
            {
                using (SqlCommand command = new SqlCommand(createTableSql.ToString(), target_connection))
                {
                    Console.WriteLine(command.ExecuteNonQuery());
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(targetDatabase.server+" ["+ tableName + "] is exists "+ex.Message);
            }
            if (getLastChangedId(tableName) == -1) { 
                syncAll(tableName);
                //updateLastVersion(tableName, getCurrentChangedId().ToString());

            }
        }
        private void enableDatabaseBrokker(string database)
        {

            try
            {
                using (SqlCommand command = new SqlCommand("ALTER DATABASE "+ database+" SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);", connection))
                {
                    Console.WriteLine(command.ExecuteNonQuery());
                }
            }
            catch (Exception)
            {
                Console.WriteLine(database + " is already enabled change tracking");
            }
            using (SqlCommand select_command = new SqlCommand("SELECT is_broker_enabled FROM sys.databases WHERE name = '" + database+"'", connection))
            {
                using (SqlDataReader sdr = select_command.ExecuteReader())
                {
                    if(sdr.HasRows && (sdr.Read() && sdr.GetBoolean("is_broker_enabled")))
                    {
                        Console.WriteLine(database + " is already enabled BROKER");
                    }
                    else
                    {
                        try
                        {
                            using (SqlCommand command = new SqlCommand("ALTER DATABASE " + database + " SET ENABLE_BROKER;", connection))
                            {
                                Console.WriteLine(command.ExecuteNonQuery());
                            }
                        }
                        catch (Exception)
                        {
                            Console.WriteLine(database + " is already enabled BROKER");
                        }
                    }
                   
                }
            }
            


        }
        private void enabledChangeTracking(string table)
        {
            try
            {
                using (SqlCommand command = new SqlCommand("ALTER TABLE "+ table+" ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);", connection))
                {
                    Console.WriteLine(command.ExecuteNonQuery());
                }
            }
            catch (Exception)
            {
                Console.WriteLine(table+" is already enabled change tracking");
            }
        }

        List<string> GetPrimartyKey(string tableName)
        {
            List<string> pk = [];
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
                        string columnName = reader.GetString("COLUMN_NAME");
                        Console.WriteLine($"Primary key column: {columnName}");
                        pk.Add(columnName);
                    }
                }
            }
            return pk;
        }
        private void setPrimaryKey(string tableName, string key)
        {
            try
            {
                using (SqlCommand command = new SqlCommand("ALTER TABLE " + tableName + " ADD PRIMARY KEY ("+ key+ ");", connection))
                {
                    Console.WriteLine(command.ExecuteNonQuery());
                }
            }
            catch (Exception)
            {
                Console.WriteLine(tableName + " has an error to set primary key of "+key);
            }
        }
        // 当数据库表变化时调用的事件
        private void OnDatabaseChange(object sender, SqlNotificationEventArgs e, string tableName)
        {
            Console.WriteLine(DateTime.Now.ToString("MM-dd HH:mm:ss") +" 表格{3}变更通知：类型={0}, 信息={1}, 源={2}", e.Type, e.Info, e.Source, tableName);
            syncChanged(tableName, getCurrentChangedId(), getLastChangedId(tableName), ListenForChanges(tableName));

        }

        public void Stop()
        {
            SqlDependency.Stop(connectionStr);
        }
    }
}