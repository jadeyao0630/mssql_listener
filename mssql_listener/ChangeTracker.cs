using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace mssql_listener
{
    internal class ChangeTracker
    {
        private System.Timers.Timer _timer;
        private string _connectionString = "server=192.168.10.122,1433;Database=glory;User Id=sa;Password=qijiashe6;Trusted_Connection=True";
        private long _lastSyncVersion = 0; // 初始同步版本号

        public ChangeTracker()
        {
            _timer = new System.Timers.Timer(1000); // 设置轮询间隔，这里是5秒
            _timer.Elapsed += CheckForChanges;
            _timer.AutoReset = true;
            _timer.Enabled = true;
            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        private void CheckForChanges(object sender, ElapsedEventArgs e)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                // 获取当前变更跟踪版本号
                var command = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();", connection);
                var currentVersion = (long)command.ExecuteScalar();

                if (_lastSyncVersion < currentVersion)
                {
                    // 查询变更的数据
                    string query = $@"SELECT CT.* FROM CHANGETABLE(CHANGES dbo.sales, {_lastSyncVersion}) AS CT";

                    command = new SqlCommand(query, connection);

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var changeType = reader["SYS_CHANGE_OPERATION"].ToString();
                            var primaryKey = reader["id"].ToString();
                            Console.WriteLine($"Change detected: {changeType} on PrimaryKey: {primaryKey}");
                            // 根据变化类型和主键处理变化
                        }
                    }

                    _lastSyncVersion = currentVersion; // 更新同步版本号
                }
            }
        }
    }
}
