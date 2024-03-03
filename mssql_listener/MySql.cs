using Microsoft.IdentityModel.Tokens;

using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mssql_listener
{
    public class MySqlServerData
    {
        public string server="";
        public string database = "";
        public int port=3306;
        public string user = "";
        public string password = "";

    }
    internal class MySql
    {
        private string connectionString = "";
        public MySql(MySqlServerData serverData) {
            if (serverData != null &&
                !serverData.server.IsNullOrEmpty() &&
                !serverData.database.IsNullOrEmpty() &&
                !serverData.user.IsNullOrEmpty() &&
                !serverData.password.IsNullOrEmpty())
            {
                connectionString= $"Server={serverData.server};User={serverData.user};Database={serverData.database};Port={serverData.port};Password={serverData.password};ConvertZeroDateTime=True";
            }
        }
        public void Execute(string query)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {

                    Console.WriteLine("connection is ..." + connectionString);
                    // 打开连接
                    connection.Open();
                    Console.WriteLine("connection is openned..."+ connectionString);

                    using (var command = new MySqlCommand(query, connection))
                    {
                        // 执行命令
                        command.ExecuteNonQuery();
                        Console.WriteLine($"命令 '{query}' 已成功执行。");
                    }
                }
                catch (Exception ex)
                {
                    // 异常处理
                    Console.WriteLine($"执行命令 '{query}' 时出错：" + ex.Message);
                }
            }
        }
        public List<Dictionary<string, object>> Get(string query)
        {
            List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
            using (var connection = new MySqlConnection(connectionString))
            {
                // 打开连接
                connection.Open();

                // 创建并执行查询命令
                using (var command = new MySqlCommand(query, connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        // 遍历查询结果
                        while (reader.Read())
                        {
                            // 读取每行的列数据

                            Dictionary<string, object> row = new Dictionary<string, object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                row.Add(reader.GetName(i), reader.IsDBNull(i) ? "" : reader.GetValue(i));
                                //Console.WriteLine(string.Join(Environment.NewLine, row.Select(kvp => "{"+$"{kvp.Key}: {kvp.Value}"+"}")));
                            }
                            rows.Add(row);
                            //Console.WriteLine($"{reader.GetName(0)}, {reader.GetName(1)}");
                        }
                    }
                }

            }
            return rows;
        }
        public Boolean IsTableExist(string table)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string query = $@"
                    SELECT *
                    FROM information_schema.tables 
                    WHERE table_schema = '{connection.Database}' 
                          AND table_name = '{table}'
                    LIMIT 1;";

                    using (var command = new MySqlCommand(query, connection))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            return reader.HasRows;
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine($"MySQL错误: {ex.Message}");
                    return false;
                }
            }
        }
    }
}
