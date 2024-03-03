using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mssql_listener
{
    internal class TrackingCT
    {

        static string connectionString = "server=192.168.10.122,1433;Database=glory;User Id=sa;Password=qijiashe6;Trusted_Connection=True";
        public void run() {
            SqlDependency.Start(connectionString);
            

            ListenForChanges();
            Console.Read();
            SqlDependency.Stop(connectionString);
        }
        private void ListenForChanges()
        {
            long lastSyncVersion = GetLastSyncVersion(); // 假设这个方法返回你上次同步的版本号
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // 获取当前变更跟踪版本
                SqlCommand getVersionCommand = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();", connection);
                long currentVersion = (long)getVersionCommand.ExecuteScalar();

                // 查询自上次同步以来发生变更的行
                string query = $@"SELECT CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION, T.*
        FROM CHANGETABLE(CHANGES dbo.sales, @LastSyncVersion) AS CT
        JOIN dbo.sales AS T ON T.id = CT.id";

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@lastSyncVersion", lastSyncVersion);
                    SqlDependency dependency = new SqlDependency(command);
                    dependency.OnChange += new OnChangeEventHandler(OnDatabaseChange);
                    using (SqlDataReader sdr = command.ExecuteReader())
                    {
                        Console.WriteLine();
                        while (sdr.Read())
                        {

                            Console.WriteLine("ID:{0}\t数据:{1}\t", sdr["id"].ToString(), sdr["sales"].ToString());
                        }
                        sdr.Close();
                    }
                }

                // 更新你的同步版本号，以便下次使用
                UpdateLastSyncVersion(currentVersion);
            }
        }
        private void OnDatabaseChange(object sender, SqlNotificationEventArgs e)
        {
            // 当监听到变更时，这个方法会被调用。
            //Console.WriteLine(e);
            Console.WriteLine("数据库变更通知：类型={0}, 信息={1}, 源={2}", e.Type, e.Info, e.Source);

            // 重新开始监听下一次的变更。
            ListenForChanges();
        }
        static long GetLastSyncVersion()
        {
            // 实现获取上次同步的版本号
            return 0;
        }

        static void UpdateLastSyncVersion(long newVersion)
        {
            // 实现更新同步版本号的逻辑
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand("INSERT INTO SyncVersion (Version) VALUES (@Version);", connection))
                {
                    command.Parameters.AddWithValue("@Version", newVersion);
                    command.ExecuteNonQuery();
                }
            }
        }
        static async Task CheckForChangesAsync(SqlConnection connection, long lastSyncVersion)
        {
            // 获取当前的变更跟踪版本号
            var currentVersionCommand = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();", connection);
            long currentVersion = (long)await currentVersionCommand.ExecuteScalarAsync();

            if (currentVersion > lastSyncVersion)
            {
                var query = $@"
            SELECT CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION, T.*
            FROM CHANGETABLE(CHANGES dbo.sales, @LastSyncVersion) AS CT
            JOIN dbo.sales AS T ON T.id = CT.id";

                var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@LastSyncVersion", lastSyncVersion);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        // 处理变更
                        Console.WriteLine(reader);
                        Console.WriteLine(reader["id"]);
                    }
                }

                // 更新最后同步版本号
                lastSyncVersion = currentVersion;
                UpdateLastSyncVersion(lastSyncVersion);
            }
        }
        private static long lastSyncVersion = 0; // 初始版本号，生产环境中应从持久化存储中读取


        static public async Task OnTimedEvent()
        {
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await CheckForChangesAsync(connection, lastSyncVersion);
            }
        }
    }
}
